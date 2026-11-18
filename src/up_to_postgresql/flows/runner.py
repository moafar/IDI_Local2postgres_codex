# src/up_to_postgresql/flows/runner.py
"""Run configured file-processing flows."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import file_digest
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.loading import (
    PostgresqlLoadResult,
    load_to_postgresql,
    prepare_postgresql_load,
)
from up_to_postgresql.readers import read_source
from up_to_postgresql.source import resolve_source_path
from up_to_postgresql.transforms import prestacions_pendents as pending


LOGGER = logging.getLogger(__name__)
VALID_POLICIES = ("error", "warning", "report")


def _status(config: FlowConfig, message: str) -> None:
    print(f"[{config.name} | {config.env}] {message}", flush=True)


class FlowRunError(ValueError):
    """Raised when a configured flow cannot be executed."""


@dataclass(frozen=True)
class FlowRunResult:
    flow: str
    env: str
    rows_read: int
    columns_read: int
    empty_rows_removed: int
    empty_columns_removed: tuple[str, ...]
    rows_written: int
    columns_written: int
    duplicate_rows: int
    required_columns: tuple[str, ...]
    missing_required_columns: tuple[str, ...]
    duplicate_key: tuple[str, ...]
    missing_duplicate_key_columns: tuple[str, ...]
    transformations: tuple[str, ...]
    warnings: tuple[str, ...]
    source_path: Path
    processed_path: Path
    report_path: Path
    postgresql: PostgresqlLoadResult | None = None


def run_flow(
    config: FlowConfig,
    *,
    load: bool = False,
    connection_factory: Any | None = None,
    password_provider: Any | None = None,
    confirm_callback: Any | None = None,
) -> FlowRunResult:
    snapshot = None
    if config.name == pending.FLOW_NAME:
        settings = _transformations(config)[0]
        snapshot = {
            "periode_informe": settings["periode_informe"],
            "data_tall": settings["data_tall"],
            "stage": "read",
            "rows_published": 0,
        }
    try:
        return _run_flow(
            config, load, connection_factory, password_provider, confirm_callback, snapshot
        )
    except Exception as error:
        if snapshot is None:
            raise
        stage = snapshot["stage"]
        reason = (
            str(error)
            if isinstance(error, pending.PendingSnapshotError)
            else f"Snapshot failed during {stage} ({type(error).__name__})."
        )
        report_path = _pending_report_path(config, "error")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        _write_report(
            report_path,
            {
                **snapshot,
                "flow": config.name,
                "env": config.env,
                "status": "error",
                "error_message": reason,
            },
        )
        raise FlowRunError(f"{reason} Report: {report_path}") from None


def _run_flow(
    config: FlowConfig,
    load: bool,
    connection_factory: Any,
    password_provider: Any,
    confirm_callback: Any,
    snapshot: dict[str, Any] | None,
) -> FlowRunResult:
    _status(config, "Iniciando pipeline")
    prepared_password: str | None = None
    if load and snapshot is None:
        _status(config, "Preparando carga a PostgreSQL")
        prepared_password = prepare_postgresql_load(
            config,
            password_provider=password_provider,
            confirm_callback=confirm_callback,
        )
        _status(config, "Carga PostgreSQL confirmada")
    _status(config, "Resolviendo archivo de origen")
    if snapshot is not None and config.data["source"].get("path") != pending.SOURCE_FILENAME:
        raise pending.PendingSnapshotError(f"Expected source {pending.SOURCE_FILENAME}.")
    source_path = resolve_source_path(config)
    _status(config, f"Leyendo archivo: {source_path}")
    if snapshot is not None:
        with source_path.open("rb") as source:
            snapshot["source_hash"] = file_digest(source, "sha256").hexdigest()
        expected_header = [
            item["source"]
            for item in config.data["load"]["column_mapping"]
            if item["source"] not in pending.DERIVED_COLUMNS
        ]
        records = pending.check_csv(source_path, expected_header, snapshot)
    frame = read_source(config)
    if snapshot is not None:
        if (
            list(frame.columns) != expected_header
            or len(frame) != records
            or not isinstance(frame.index, pd.RangeIndex)
            or frame.isna().any().any()
        ):
            raise pending.PendingSnapshotError("CSV reader did not preserve the expected detail.")
        snapshot["stage"] = "validation"
        _validate_pending_key(config, frame, snapshot)
    _status(config, f"{len(frame)} filas x {len(frame.columns)} columnas leídas")
    processing = _processing(config)
    cleaned = frame
    _status(config, "Limpiando filas y columnas vacías")
    if processing.get("drop_empty_rows", False) or processing.get(
        "drop_empty_columns", False
    ):
        cleaned = _drop_empty(
            cleaned,
            drop_rows=bool(processing.get("drop_empty_rows", False)),
            drop_columns=bool(processing.get("drop_empty_columns", False)),
        )
    empty_rows_removed = len(frame) - len(cleaned)
    empty_columns_removed = tuple(
        str(column) for column in frame.columns if column not in cleaned.columns
    )
    _status(
        config,
        "Limpieza completada: "
        f"{empty_rows_removed} filas eliminadas, "
        f"{len(empty_columns_removed)} columnas eliminadas",
    )
    _status(config, "Aplicando transformaciones")
    if snapshot is not None:
        snapshot["stage"] = "transformation"
        transformed = pending.transform(frame, _transformations(config)[0], snapshot)
    else:
        transformed = _apply_transformations(cleaned, config)
    transformed = _project_mapped_columns(transformed, config)
    transformation_names = _transformation_names(config)
    _status(
        config,
        "Transformaciones aplicadas: "
        + (", ".join(transformation_names) if transformation_names else "ninguna"),
    )
    validation = _validation(config)
    _status(config, "Validando columnas requeridas")
    warnings: list[str] = []
    required_columns, missing_required_columns = _check_required_columns(
        validation, transformed, warnings
    )
    _status(
        config,
        "Validación de columnas: "
        + ("OK" if not missing_required_columns else f"faltan {list(missing_required_columns)}"),
    )
    _status(config, "Comprobando duplicados")
    duplicate_key, missing_duplicate_key_columns = _duplicate_key(validation, transformed)
    duplicate_rows = 0
    if missing_duplicate_key_columns:
        duplicate_rows = 0
    elif duplicate_key:
        duplicate_rows = int(transformed.duplicated(subset=duplicate_key, keep=False).sum())
    else:
        duplicate_rows = int(transformed.duplicated(keep=False).sum())
    _apply_duplicate_policy(validation, duplicate_rows, missing_duplicate_key_columns, warnings)
    _status(config, f"Duplicados detectados: {duplicate_rows}")

    output_dir = _output_base_dir(config)
    output_dir.mkdir(parents=True, exist_ok=True)
    processed_path = output_dir / _output_name(
        config, "processed_filename", f"{config.name}_processed.csv"
    )
    report_path = output_dir / _output_name(
        config, "report_filename", f"{config.name}_report.json"
    )
    if snapshot is not None:
        if load:
            snapshot["stage"] = "load_confirmation"
            prepared_password = prepare_postgresql_load(
                config,
                password_provider=password_provider,
                confirm_callback=confirm_callback,
            )
        else:
            report_path = _pending_report_path(config, "preview")
        snapshot["stage"] = "processed_output"

    _status(config, f"Escribiendo archivo procesado: {processed_path}")
    transformed.to_csv(processed_path, index=False, encoding="utf-8")
    postgresql_result = None
    if load:
        _status(config, "Iniciando carga a PostgreSQL")
        if snapshot is not None:
            snapshot["stage"] = "load"
        postgresql_result = load_to_postgresql(
            config,
            transformed,
            connection_factory=connection_factory,
            password_provider=password_provider,
            confirm_callback=confirm_callback,
            password=prepared_password,
            confirmed=True,
        )
        if snapshot is not None:
            snapshot["rows_published"] = postgresql_result.rows_loaded
        _status(config, f"{postgresql_result.rows_loaded} filas cargadas en PostgreSQL")
    result = FlowRunResult(
        flow=config.name,
        env=config.env,
        rows_read=len(frame),
        columns_read=len(frame.columns),
        empty_rows_removed=empty_rows_removed,
        empty_columns_removed=empty_columns_removed,
        rows_written=len(transformed),
        columns_written=len(transformed.columns),
        duplicate_rows=duplicate_rows,
        required_columns=required_columns,
        missing_required_columns=tuple(missing_required_columns),
        duplicate_key=tuple(duplicate_key),
        missing_duplicate_key_columns=tuple(missing_duplicate_key_columns),
        transformations=transformation_names,
        warnings=tuple(warnings),
        source_path=source_path,
        processed_path=processed_path,
        report_path=report_path,
        postgresql=postgresql_result,
    )
    _status(config, f"Escribiendo reporte: {report_path}")
    report = _report(config, result)
    if snapshot is not None:
        snapshot["stage"] = "report"
        report.update(snapshot, stage="completed", status="success")
        report["steps"] = [
            "read_source",
            "validate_full_detail",
            "filter_and_derive_pending_prestations",
            "project_and_validate_output",
            "write_processed_output",
            *(["load_postgresql"] if load else []),
            "write_execution_report",
        ]
    _write_report(report_path, report)
    _status(config, "Pipeline completado")
    return result


def _validate_pending_key(
    config: FlowConfig, frame: pd.DataFrame, snapshot: dict[str, Any]
) -> None:
    validation = _validation(config)
    key, missing = _duplicate_key(validation, frame)
    snapshot["duplicate_key"] = key
    if key != pending.KEY or missing:
        raise pending.PendingSnapshotError("Expected the complete source key.")
    _check_required_columns(validation, frame, [])
    key_values = frame.loc[:, list(key)]
    duplicates = int(key_values.duplicated(keep=False).sum())
    incomplete = int(key_values.apply(lambda col: col.str.strip().eq("")).any(axis=1).sum())
    snapshot.update(
        distinct_keys=len(key_values.drop_duplicates()),
        incomplete_key_rows=incomplete,
        duplicate_rows=duplicates,
    )
    if frame.empty:
        raise pending.PendingSnapshotError("CSV contains no detail rows.")
    if incomplete or duplicates:
        raise pending.PendingSnapshotError(
            f"Invalid source key: {incomplete} incomplete rows, {duplicates} duplicate rows."
        )


def _pending_report_path(config: FlowConfig, suffix: str) -> Path:
    path = _output_base_dir(config) / _output_name(
        config, "report_filename", f"{config.name}_report.json"
    )
    return path.with_stem(f"{path.stem}_{suffix}")


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _drop_empty(
    frame: pd.DataFrame, *, drop_rows: bool, drop_columns: bool
) -> pd.DataFrame:
    empty_marked = frame.replace(r"^\s*$", pd.NA, regex=True)
    non_empty_rows = (
        ~empty_marked.isna().all(axis=1)
        if drop_rows
        else pd.Series(True, index=frame.index)
    )
    non_empty_columns = (
        ~empty_marked.isna().all(axis=0)
        if drop_columns
        else pd.Series(True, index=frame.columns)
    )
    return frame.loc[non_empty_rows, non_empty_columns].copy()


def _apply_transformations(configured_frame: pd.DataFrame, config: FlowConfig) -> pd.DataFrame:
    frame = configured_frame.copy()
    processing = _processing(config)
    trim_strings = processing.get("trim_strings")
    if trim_strings not in (None, False):
        frame = _trim_strings(frame, "all" if trim_strings is True else trim_strings)
    for transformation in _transformations(config):
        name = transformation["name"]

        if name == "trim_strings":
            frame = _trim_strings(frame, transformation.get("columns"))
            continue

        if name == "derive_year":
            source_column = transformation.get("source_column")
            target_column = transformation.get("target_column")

            if not isinstance(source_column, str) or not source_column:
                raise FlowRunError(
                    "Transformation 'derive_year' requires 'source_column'."
                )

            if not isinstance(target_column, str) or not target_column:
                raise FlowRunError(
                    "Transformation 'derive_year' requires 'target_column'."
                )

            if source_column not in frame.columns:
                raise FlowRunError(
                    f"Source column {source_column!r} not found for 'derive_year'."
                )

            parsed_dates = pd.to_datetime(frame[source_column], errors="coerce")
            frame[target_column] = parsed_dates.dt.year.astype("Int64").astype("string")
            continue

        raise FlowRunError(f"Unsupported transformation {name!r}.")
    return frame


def _project_mapped_columns(frame: pd.DataFrame, config: FlowConfig) -> pd.DataFrame:
    load = config.data.get("load")
    if not isinstance(load, dict):
        return frame
    raw_mapping = load.get("column_mapping")
    if not isinstance(raw_mapping, list):
        return frame
    columns: list[str] = []
    for item in raw_mapping:
        if not isinstance(item, dict):
            return frame
        source = item.get("source")
        if not isinstance(source, str) or not source:
            return frame
        if source not in columns:
            columns.append(source)
    if not columns or any(column not in frame.columns for column in columns):
        return frame
    return frame.loc[:, columns].copy()


def _transformations(config: FlowConfig) -> list[dict[str, Any]]:
    raw_transformations = config.data.get("transformations", [])
    if raw_transformations is None:
        return []
    if not isinstance(raw_transformations, list):
        raise FlowRunError("Flow transformations must be a list when present.")
    transformations: list[dict[str, Any]] = []
    for transformation in raw_transformations:
        if not isinstance(transformation, dict):
            raise FlowRunError("Each flow transformation must be a mapping.")
        name = transformation.get("name")
        if not isinstance(name, str) or not name:
            raise FlowRunError("Each flow transformation requires a name.")
        transformations.append(transformation)
    return transformations


def _transformation_names(config: FlowConfig) -> tuple[str, ...]:
    names = [transformation["name"] for transformation in _transformations(config)]
    if _processing(config).get("trim_strings") not in (None, False):
        names.insert(0, "trim_strings")
    return tuple(names)


def _trim_strings(frame: pd.DataFrame, raw_columns: Any) -> pd.DataFrame:
    columns = _selected_columns(frame, raw_columns)
    trimmed = frame.copy()
    for column in columns:
        trimmed[column] = trimmed[column].fillna("").astype(str).str.strip()
    return trimmed


def _selected_columns(frame: pd.DataFrame, raw_columns: Any) -> list[str]:
    if raw_columns in (None, "all"):
        return [str(column) for column in frame.columns]
    if not isinstance(raw_columns, list) or not all(
        isinstance(column, str) and column for column in raw_columns
    ):
        raise FlowRunError("trim_strings.columns must be a list of columns or 'all'.")
    missing = [column for column in raw_columns if column not in frame.columns]
    if missing:
        raise FlowRunError(f"trim_strings columns not found: {missing}")
    return raw_columns


def _processing(config: FlowConfig) -> dict[str, Any]:
    processing = config.data.get("processing", {})
    if processing is None:
        return {}
    if not isinstance(processing, dict):
        raise FlowRunError("Flow processing must be a mapping when present.")
    return processing


def _validation(config: FlowConfig) -> dict[str, Any]:
    validation = config.data.get("validation", {})
    if validation is None:
        return {}
    if not isinstance(validation, dict):
        raise FlowRunError("Flow validation must be a mapping when present.")
    return validation


def _check_required_columns(
    validation: dict[str, Any], frame: pd.DataFrame, warnings: list[str]
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    raw_columns = validation.get("required_columns", [])
    if raw_columns is None:
        return (), ()
    if not isinstance(raw_columns, list) or not all(
        isinstance(column, str) and column for column in raw_columns
    ):
        raise FlowRunError("Flow validation.required_columns must be a list of columns.")
    missing = [column for column in raw_columns if column not in frame.columns]
    if missing:
        message = f"Required columns not found: {missing}"
        _apply_policy(
            validation.get("missing_columns_policy", "error"),
            message,
            warnings,
        )
    return tuple(raw_columns), tuple(missing)


def _duplicate_key(
    validation: dict[str, Any], frame: pd.DataFrame
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    raw_key = validation.get("duplicate_key")
    if raw_key is None:
        return (), ()
    if not isinstance(raw_key, list) or not all(
        isinstance(column, str) and column for column in raw_key
    ):
        raise FlowRunError("Flow validation.duplicate_key must be a list of columns.")
    missing = [column for column in raw_key if column not in frame.columns]
    return tuple(raw_key), tuple(missing)


def _apply_duplicate_policy(
    validation: dict[str, Any],
    duplicate_rows: int,
    missing_key_columns: tuple[str, ...],
    warnings: list[str],
) -> None:
    policy = validation.get("duplicate_policy", "report")
    if missing_key_columns:
        _apply_policy(
            validation.get("missing_columns_policy", "error"),
            f"Duplicate key columns not found: {list(missing_key_columns)}",
            warnings,
        )
    if duplicate_rows:
        _apply_policy(policy, f"Duplicate rows found: {duplicate_rows}", warnings)


def _apply_policy(policy: str, message: str, warnings: list[str]) -> None:
    if policy not in VALID_POLICIES:
        raise FlowRunError(f"Validation policy must be one of {VALID_POLICIES}.")
    if policy == "error":
        raise FlowRunError(message)
    if policy == "warning":
        LOGGER.warning(message)
        warnings.append(message)


def _output_base_dir(config: FlowConfig) -> Path:
    paths = config.data.get("paths", {})
    if not isinstance(paths, dict):
        raise FlowRunError("Flow paths must be a mapping.")
    raw_output_dir = paths.get("output_base_dir", paths.get("output_dir"))
    if not isinstance(raw_output_dir, str) or not raw_output_dir:
        raise FlowRunError("Flow requires a non-empty paths.output_base_dir value.")
    return Path(raw_output_dir)


def _output_name(config: FlowConfig, key: str, default: str) -> Path:
    output = config.data.get("output", {})
    if output is None:
        output = {}
    if not isinstance(output, dict):
        raise FlowRunError("Flow output must be a mapping when present.")
    legacy_key = key.replace("_filename", "_path")
    raw_path = output.get(key, output.get(legacy_key, default))
    if not isinstance(raw_path, str) or not raw_path:
        raise FlowRunError(f"Flow output.{key} must be a non-empty string.")
    path = Path(raw_path)
    if path.is_absolute():
        raise FlowRunError(f"Flow output.{key} must be relative to output_base_dir.")
    return path


def _report(config: FlowConfig, result: FlowRunResult) -> dict[str, Any]:
    source = config.data.get("source", {})
    validation = _validation(config)
    return {
        "flow": result.flow,
        "env": result.env,
        "source": {
            "type": source.get("type") if isinstance(source, dict) else None,
            "path": str(result.source_path),
            "sheet": source.get("sheet") if isinstance(source, dict) else None,
            "header_row": source.get("header_row", 1) if isinstance(source, dict) else 1,
        },
        "rows_read": result.rows_read,
        "columns_read": result.columns_read,
        "empty_rows_removed": result.empty_rows_removed,
        "empty_columns_removed": list(result.empty_columns_removed),
        "rows_written": result.rows_written,
        "columns_written": result.columns_written,
        "required_columns": list(result.required_columns),
        "missing_required_columns": list(result.missing_required_columns),
        "transformations": list(result.transformations),
        "duplicate_key": list(result.duplicate_key),
        "missing_duplicate_key_columns": list(result.missing_duplicate_key_columns),
        "duplicate_rows": result.duplicate_rows,
        "policies": {
            "missing_columns": validation.get("missing_columns_policy", "error"),
            "duplicates": validation.get("duplicate_policy", "report"),
        },
        "warnings": list(result.warnings),
        "processed_path": str(result.processed_path),
        "report_path": str(result.report_path),
        "steps": [
            "read_source",
            "drop_empty_rows",
            "drop_empty_columns",
            "detect_internal_duplicates",
            "write_processed_output",
            "write_execution_report",
        ],
        "postgresql": _postgresql_report(result),
    }


def _postgresql_report(result: FlowRunResult) -> dict[str, Any]:
    if result.postgresql is None:
        return {"status": "skipped"}
    return {
        "status": result.postgresql.status,
        "target_schema": result.postgresql.target_schema,
        "target_table": result.postgresql.target_table,
        "load_mode": result.postgresql.load_mode,
        "source_filename": result.postgresql.source_filename,
        "source_hash": result.postgresql.source_hash,
        "rows_loaded": result.postgresql.rows_loaded,
    }
