# src/up_to_postgresql/flows/runner.py
"""Run configured file flows and coordinate guarded demanda analysis.

Depends on readers, the PostgreSQL loader and the substitutions executor.
Only demanda uses year validation, session locking and analytical reports;
other flows retain their existing processing and loading behavior.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import logging
from pathlib import Path
import re
import sys
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from up_to_postgresql.config.schema import FlowConfig, validate_flow_config
from up_to_postgresql.flows.demanda_sustituciones import (
    DemandaSustitucionesResult,
    generar_sustituciones,
)
from up_to_postgresql.loading import (
    PostgresqlLoadResult,
    load_to_postgresql,
    prepare_postgresql_load,
)
from up_to_postgresql.loading.postgresql import (
    _default_confirm_callback,
    _default_connection_factory,
    _default_password_provider,
    _postgresql_config,
)
from up_to_postgresql.readers import read_source
from up_to_postgresql.source import resolve_source_path


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
    demanda_sustituciones: DemandaSustitucionesResult | None = None


@dataclass(frozen=True)
class DemandaRecalculationResult:
    sustituciones: DemandaSustitucionesResult
    report_path: Path


@dataclass
class _DemandaReportState:
    stage: str = "lectura"
    anio: int | None = None
    load_started: bool = False
    postgresql: PostgresqlLoadResult | None = None
    sustituciones: DemandaSustitucionesResult | None = None


def run_flow(
    config: FlowConfig,
    *,
    load: bool = False,
    connection_factory: Any | None = None,
    password_provider: Any | None = None,
    confirm_callback: Any | None = None,
) -> FlowRunResult:
    state = _DemandaReportState() if load and config.name == "demanda" else None
    try:
        return _run_flow(
            config,
            load=load,
            connection_factory=connection_factory,
            password_provider=password_provider,
            confirm_callback=confirm_callback,
            demanda_state=state,
        )
    except Exception as error:
        if state is not None:
            _record_demanda_failure(config, state, error, manual=False)
            error.add_note(f"Etapa de demanda: {state.stage}")
        raise


def _run_flow(
    config: FlowConfig,
    *,
    load: bool,
    connection_factory: Any | None,
    password_provider: Any | None,
    confirm_callback: Any | None,
    demanda_state: _DemandaReportState | None,
) -> FlowRunResult:
    _status(config, "Iniciando pipeline")
    prepared_password: str | None = None
    demanda_load = load and config.name == "demanda"
    if load and not demanda_load:
        _status(config, "Preparando carga a PostgreSQL")
        prepared_password = prepare_postgresql_load(
            config,
            password_provider=password_provider,
            confirm_callback=confirm_callback,
        )
        _status(config, "Carga PostgreSQL confirmada")
    _status(config, "Resolviendo archivo de origen")
    if demanda_state is not None:
        demanda_state.stage = "lectura"
    source_path = resolve_source_path(config)
    _status(config, f"Leyendo archivo: {source_path}")
    frame = read_source(config)
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
    transformed = _project_mapped_columns(_apply_transformations(cleaned, config), config)
    transformation_names = _transformation_names(config)
    _status(
        config,
        "Transformaciones aplicadas: "
        + (", ".join(transformation_names) if transformation_names else "ninguna"),
    )
    if demanda_state is not None:
        demanda_state.stage = "validacion"
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

    if demanda_load:
        validate_flow_config(config.data, expected_name="demanda", env=config.env)
    anio_demanda = _demanda_year(config, transformed) if demanda_load else None
    if demanda_load:
        demanda_state.anio = anio_demanda
        demanda_state.stage = "confirmacion"
        _status(config, "Preparando carga a PostgreSQL")
        prepared_password = prepare_postgresql_load(
            config,
            password_provider=password_provider,
            confirm_callback=confirm_callback,
        )
        if prepared_password is None:
            raise FlowRunError("La contraseña de PostgreSQL no está disponible")
        _status(config, "Carga PostgreSQL confirmada")

    output_dir = _output_base_dir(config)
    output_dir.mkdir(parents=True, exist_ok=True)
    processed_path = output_dir / _output_name(
        config, "processed_filename", f"{config.name}_processed.csv"
    )
    report_path = output_dir / _output_name(
        config, "report_filename", f"{config.name}_report.json"
    )

    _status(config, f"Escribiendo archivo procesado: {processed_path}")
    if demanda_state is not None:
        demanda_state.stage = "salida_procesada"
    transformed.to_csv(processed_path, index=False, encoding="utf-8")
    postgresql_result = None
    sustituciones_result = None
    if load:
        lock = (
            _demanda_advisory_lock(config, connection_factory, prepared_password)
            if demanda_load
            else nullcontext()
        )
        if demanda_state is not None:
            demanda_state.stage = "bloqueo"
        with lock:
            if demanda_state is not None:
                demanda_state.stage = "carga"
                demanda_state.load_started = True
            _status(config, "Iniciando carga a PostgreSQL")
            postgresql_result = load_to_postgresql(
                config,
                transformed,
                connection_factory=connection_factory,
                password_provider=password_provider,
                confirm_callback=confirm_callback,
                password=prepared_password,
                confirmed=True,
            )
            if demanda_state is not None:
                demanda_state.postgresql = postgresql_result
            if demanda_load:
                if postgresql_result.status != "success":
                    raise FlowRunError("La carga de demanda no finalizó satisfactoriamente")
            _status(config, f"{postgresql_result.rows_loaded} filas cargadas en PostgreSQL")
            if demanda_load:
                demanda_state.stage = "analisis"
                _status(config, f"Generando sustituciones de {anio_demanda}")
                sustituciones_result = generar_sustituciones(
                    config,
                    anio=anio_demanda,
                    connection_factory=connection_factory,
                    password=prepared_password,
                )
                if (
                    not isinstance(sustituciones_result, DemandaSustitucionesResult)
                    or sustituciones_result.estado != "success"
                ):
                    raise FlowRunError("El análisis de demanda no devolvió un resultado válido")
                demanda_state.sustituciones = sustituciones_result
                _status(config, "Sustituciones generadas")
                demanda_state.stage = "bloqueo"
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
        demanda_sustituciones=sustituciones_result,
    )
    _status(config, f"Escribiendo reporte: {report_path}")
    if demanda_state is not None:
        demanda_state.stage = "reporte"
        _write_json_report(report_path, _report(config, result))
    else:
        report_path.write_text(
            json.dumps(_report(config, result), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    _status(config, "Pipeline completado")
    return result


def recalcular_demanda_sustituciones(
    config: FlowConfig,
    *,
    anio: int,
    connection_factory: Any | None = None,
    password_provider: Any | None = None,
    confirm_callback: Any | None = None,
) -> DemandaRecalculationResult:
    """Recalculate one year without reading or loading a source file."""
    state = _DemandaReportState(stage="validacion")
    try:
        if config.name != "demanda":
            raise FlowRunError("El recálculo solo está disponible para demanda")
        if not isinstance(anio, int) or isinstance(anio, bool) or not 1 <= anio <= 9999:
            raise FlowRunError("--anio debe ser un entero entre 1 y 9999")
        state.anio = anio
        validate_flow_config(config.data, expected_name="demanda", env=config.env)
        schema = _postgresql_config(config)["schema"]
        report_path = _manual_report_path(config, anio)
        state.stage = "confirmacion"
        confirm = confirm_callback or _default_confirm_callback
        if not confirm(
            f"Recalcular sustituciones en {schema}.demanda_sustituciones "
            f"para el año {anio}; se reemplazarán los resultados de ese año."
        ):
            raise FlowRunError("Recálculo de sustituciones cancelado")
        password = (password_provider or _default_password_provider)()
        if password is None:
            raise FlowRunError("La contraseña de PostgreSQL no está disponible")
        state.stage = "bloqueo"
        with _demanda_advisory_lock(config, connection_factory, password):
            state.stage = "analisis"
            result = generar_sustituciones(
                config, anio=anio, connection_factory=connection_factory,
                password=password,
            )
            if not isinstance(result, DemandaSustitucionesResult) or result.estado != "success":
                raise FlowRunError("El análisis de demanda no devolvió un resultado válido")
            state.sustituciones = result
            state.stage = "bloqueo"
        state.stage = "reporte"
        _write_json_report(report_path, {
            "flow": "demanda",
            "env": config.env,
            "modo": "recalculo_manual",
            "estado_ejecucion": "success",
            "carga_archivo_ejecutada": False,
            "postgresql": {"status": "skipped"},
            "demanda_sustituciones": asdict(result),
        })
        return DemandaRecalculationResult(result, report_path)
    except Exception as error:
        _record_demanda_failure(config, state, error, manual=True)
        error.add_note(f"Etapa de recálculo: {state.stage}")
        raise


def _manual_report_path(config: FlowConfig, anio: int) -> Path:
    return _output_base_dir(config) / f"demanda_sustituciones_recalculo_{anio}.json"


def _empty_sustituciones(anio: int | None, estado: str) -> dict[str, Any]:
    return {
        "anio_procesado": anio,
        "version_algoritmo": None,
        "registros_al": None,
        "candidatas_unicas": None,
        "candidatas_multiples": None,
        "sin_candidata": None,
        "registros_agregados": None,
        "relaciones_generadas": None,
        "duracion_segundos": None,
        "estado": estado,
    }


def _write_json_report(path: Path, report: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
    except OSError as error:
        raise FlowRunError(f"No se pudo escribir el reporte JSON: {path}") from error


def _record_demanda_failure(
    config: FlowConfig,
    state: _DemandaReportState,
    error: Exception,
    *,
    manual: bool,
) -> None:
    if manual:
        postgresql = {"status": "skipped"}
    elif state.postgresql is not None:
        postgresql = _postgresql_result_report(state.postgresql)
    else:
        postgresql = {
            "status": "error" if state.stage == "carga" else "skipped",
            "rows_loaded": None,
        }
    substitutions = (
        asdict(state.sustituciones) if state.sustituciones is not None
        else _empty_sustituciones(
            state.anio, "error" if state.stage == "analisis" else "skipped"
        )
    )
    substitutions_committed = (
        True if state.sustituciones is not None or
        getattr(error, "demanda_sustituciones_commit_confirmed", False)
        else None
    )
    report = {
        "flow": "demanda",
        "env": config.env,
        "modo": "recalculo_manual" if manual else "automatico",
        "estado_ejecucion": "error",
        "carga_archivo_ejecutada": False if manual else state.load_started,
        "postgresql": postgresql,
        "demanda_sustituciones": substitutions,
        "error": {
            "stage": state.stage,
            "type": type(error).__name__,
            "commit_demanda_confirmado": (
                True if state.postgresql and state.postgresql.status == "success"
                else None
            ),
            "commit_sustituciones_confirmado": substitutions_committed,
        },
    }
    try:
        if manual:
            path = (
                _manual_report_path(config, state.anio) if state.anio else
                _output_base_dir(config) / "demanda_sustituciones_recalculo_error.json"
            )
        else:
            path = _output_base_dir(config) / _output_name(
                config, "report_filename", "demanda_report.json"
            )
        _write_json_report(path, report)
    except Exception as report_error:
        error.add_note(
            f"No se pudo escribir el reporte de error: {type(report_error).__name__}"
        )
        LOGGER.error("No se pudo escribir el reporte de error de demanda")


def _current_demanda_year() -> int:
    return datetime.now(ZoneInfo("Europe/Madrid")).year


def _demanda_year(config: FlowConfig, frame: pd.DataFrame) -> int:
    load_config = config.data.get("load")
    if not isinstance(load_config, dict):
        raise FlowRunError("demanda requiere configuración de carga")
    mapping = load_config.get("column_mapping")
    if not isinstance(mapping, list):
        raise FlowRunError("demanda requiere load.column_mapping")
    source_columns = [
        item.get("source") for item in mapping
        if isinstance(item, dict) and item.get("target") == "any_prestacio"
    ]
    if len(source_columns) != 1 or source_columns[0] not in frame.columns:
        raise FlowRunError("demanda requiere un mapeo válido de any_prestacio")
    years = set()
    for value in frame[source_columns[0]]:
        if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}", value):
            raise FlowRunError("demanda requiere un año de cuatro dígitos en cada fila")
        year = int(value)
        if year == 0:
            raise FlowRunError("demanda contiene un año inválido")
        years.add(year)
    if len(years) != 1:
        raise FlowRunError("demanda requiere un único año en el archivo")
    year = years.pop()
    if year != _current_demanda_year():
        raise FlowRunError("La carga automática de demanda requiere el año calendario actual")
    return year


def _demanda_lock_key(schema: str) -> int:
    # First eight SHA-256 bytes, signed big-endian: stable across processes.
    digest = hashlib.sha256(f"up_to_postgresql:demanda:{schema}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


@contextmanager
def _demanda_advisory_lock(config: FlowConfig, connection_factory: Any, password: str):
    postgresql = _postgresql_config(config)
    schema = postgresql["schema"]
    key = _demanda_lock_key(schema)
    connection = (connection_factory or _default_connection_factory)(
        host=postgresql["host"],
        port=postgresql["port"],
        dbname=postgresql["database"],
        user=postgresql["user"],
        password=password,
    )
    acquired = False
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (key,))
            row = cursor.fetchone()
        if row is None or row[0] is not True:
            raise FlowRunError(f"Ya hay una carga de demanda en curso en {schema}")
        acquired = True
        yield
    finally:
        active_error = sys.exc_info()[1]
        release_error = None
        if acquired:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(%s)", (key,))
                    row = cursor.fetchone()
                if row is None or row[0] is not True:
                    raise FlowRunError("No se pudo liberar el bloqueo de demanda")
            except Exception as error:
                release_error = error
        try:
            connection.close()
        except Exception as error:
            if release_error is None:
                release_error = error
            else:
                release_error.add_note(f"Close failed: {type(error).__name__}")
        if release_error is not None:
            if active_error is not None:
                active_error.add_note(
                    f"Advisory lock cleanup failed: {type(release_error).__name__}"
                )
            else:
                raise release_error


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
    report = {
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
    if config.name == "demanda":
        report["modo"] = "automatico" if result.postgresql else "procesamiento"
        report["estado_ejecucion"] = "success"
        report["demanda_sustituciones"] = (
            asdict(result.demanda_sustituciones) if result.demanda_sustituciones
            else _empty_sustituciones(None, "skipped")
        )
    return report


def _postgresql_report(result: FlowRunResult) -> dict[str, Any]:
    if result.postgresql is None:
        return {"status": "skipped"}
    return _postgresql_result_report(result.postgresql)


def _postgresql_result_report(result: PostgresqlLoadResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "target_schema": result.target_schema,
        "target_table": result.target_table,
        "load_mode": result.load_mode,
        "source_filename": result.source_filename,
        "source_hash": result.source_hash,
        "rows_loaded": result.rows_loaded,
    }
