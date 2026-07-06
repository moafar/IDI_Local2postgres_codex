"""PostgreSQL loading for processed flow data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import getpass
import hashlib
import os
from pathlib import Path
import re
from typing import Any, Callable

import pandas as pd

from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.source import SourcePathError, resolve_source_path

IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
LOAD_CONTROL_COLUMNS = (
    "flow_name",
    "environment",
    "source_filename",
    "source_hash",
    "target_schema",
    "target_table",
    "load_mode",
    "rows_read",
    "rows_loaded",
    "status",
    "finished_at",
    "error_message",
)
STAGING_TABLE = "_uptopg_load_staging"


class PostgresqlLoadError(RuntimeError):
    """Raised when PostgreSQL loading cannot be completed."""


class PostgresqlLoadCancelled(PostgresqlLoadError):
    """Raised when the user declines the PostgreSQL load confirmation."""


@dataclass(frozen=True)
class PostgresqlLoadResult:
    target_schema: str
    target_table: str
    load_mode: str
    source_filename: str
    source_hash: str
    rows_loaded: int
    status: str


ConnectionFactory = Callable[..., Any]
PasswordProvider = Callable[[], str]
ConfirmCallback = Callable[[str], bool]


def load_to_postgresql(
    config: FlowConfig,
    frame: pd.DataFrame,
    *,
    connection_factory: ConnectionFactory | None = None,
    password_provider: PasswordProvider | None = None,
    confirm_callback: ConfirmCallback | None = None,
) -> PostgresqlLoadResult:
    loader = PostgresqlLoader(
        connection_factory=connection_factory,
        password_provider=password_provider,
        confirm_callback=confirm_callback,
    )
    return loader.load(config, frame)


class PostgresqlLoader:
    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory | None = None,
        password_provider: PasswordProvider | None = None,
        confirm_callback: ConfirmCallback | None = None,
    ) -> None:
        self.connection_factory = connection_factory or _default_connection_factory
        self.password_provider = password_provider or _default_password_provider
        self.confirm_callback = confirm_callback or _default_confirm_callback

    def load(self, config: FlowConfig, frame: pd.DataFrame) -> PostgresqlLoadResult:
        postgresql = _postgresql_config(config)
        load = _load_config(config)
        schema = _identifier(postgresql["schema"], "postgresql.schema")
        target_table = _identifier(load["target_table"], "load.target_table")
        load_mode = load["load_mode"]
        partition_column = _partition_column(load)
        mapping = _column_mapping(load)
        try:
            source_path = resolve_source_path(config)
        except (FileNotFoundError, SourcePathError) as error:
            raise PostgresqlLoadError(str(error)) from error
        source_hash = _sha256(source_path)
        source_filename = str(source_path)
        rows_loaded = 0
        status = "success"
        error_message: str | None = None

        message = (
            f"Load file {source_filename} with {len(frame)} rows into "
            f"{schema}.{target_table} using mode {load_mode}?"
        )
        if not self.confirm_callback(message):
            raise PostgresqlLoadCancelled("PostgreSQL load cancelled by user.")

        password = self.password_provider()
        connection = self.connection_factory(
            host=postgresql["host"],
            port=postgresql["port"],
            dbname=postgresql["database"],
            user=postgresql["user"],
            password=password,
        )
        try:
            try:
                with connection.cursor() as cursor:
                    prepared = _prepare_frame(frame, mapping)
                    _validate_database_contract(cursor, schema, target_table, mapping)
                    _validate_partition_column(
                        cursor,
                        schema,
                        target_table,
                        prepared,
                        load_mode,
                        partition_column,
                    )
                    _validate_source_hash(
                        cursor,
                        schema,
                        config.name,
                        target_table,
                        source_hash,
                        bool(load.get("reload_existing_hash", False)),
                    )
                    _validate_load_mode_preconditions(
                        cursor, schema, target_table, load_mode
                    )
                    if load_mode == "replace_partition":
                        rows_loaded = _replace_partition(
                            cursor,
                            schema,
                            target_table,
                            prepared,
                            partition_column,
                        )
                    else:
                        _apply_load_mode(cursor, schema, target_table, load_mode)
                        rows_loaded = _insert_rows(
                            cursor, schema, target_table, prepared
                        )
                    _insert_load_control(
                        cursor,
                        schema,
                        config=config,
                        source_filename=source_filename,
                        source_hash=source_hash,
                        target_table=target_table,
                        load_mode=load_mode,
                        rows_read=len(frame),
                        rows_loaded=rows_loaded,
                        status=status,
                        error_message=None,
                    )
                _commit(connection)
            except Exception as error:
                if isinstance(error, PostgresqlLoadCancelled):
                    raise
                status = "error"
                error_message = str(error)
                _rollback(connection)
                if not _connection_closed(connection):
                    try:
                        with connection.cursor() as cursor:
                            _insert_load_control(
                                cursor,
                                schema,
                                config=config,
                                source_filename=source_filename,
                                source_hash=source_hash,
                                target_table=target_table,
                                load_mode=load_mode,
                                rows_read=len(frame),
                                rows_loaded=rows_loaded,
                                status=status,
                                error_message=error_message,
                            )
                        _commit(connection)
                    except Exception:
                        _rollback(connection)
                raise
        finally:
            close = getattr(connection, "close", None)
            if callable(close):
                close()

        return PostgresqlLoadResult(
            target_schema=schema,
            target_table=target_table,
            load_mode=load_mode,
            source_filename=source_filename,
            source_hash=source_hash,
            rows_loaded=rows_loaded,
            status=status,
        )


def _postgresql_config(config: FlowConfig) -> dict[str, Any]:
    raw = config.data.get("postgresql")
    if not isinstance(raw, dict):
        raise PostgresqlLoadError("Flow configuration requires a postgresql mapping.")
    required = ("host", "port", "database", "user", "schema")
    missing = [key for key in required if key not in raw]
    if missing:
        raise PostgresqlLoadError(f"Missing postgresql configuration: {missing}")
    if "password" in raw:
        raise PostgresqlLoadError("PostgreSQL password must not be stored in config.")
    return raw


def _load_config(config: FlowConfig) -> dict[str, Any]:
    raw = config.data.get("load")
    if not isinstance(raw, dict):
        raise PostgresqlLoadError("Flow configuration requires a load mapping.")
    return raw


def _column_mapping(load: dict[str, Any]) -> list[tuple[str, str]]:
    raw_mapping = load.get("column_mapping")
    if not isinstance(raw_mapping, list) or not raw_mapping:
        raise PostgresqlLoadError("load.column_mapping must be a non-empty list.")
    mapping: list[tuple[str, str]] = []
    targets: list[str] = []
    for item in raw_mapping:
        if not isinstance(item, dict):
            raise PostgresqlLoadError("Each load.column_mapping item must be a mapping.")
        source = item.get("source")
        target = item.get("target")
        if not isinstance(source, str) or not source:
            raise PostgresqlLoadError("Each load.column_mapping item requires a source.")
        targets.append(_identifier(target, "load.column_mapping.target"))
        mapping.append((source, targets[-1]))
    if len(targets) != len(set(targets)):
        raise PostgresqlLoadError("load.column_mapping contains duplicate targets.")
    return mapping


def _partition_column(load: dict[str, Any]) -> str | None:
    value = load.get("partition_column")
    if value is None:
        return None
    return _identifier(value, "load.partition_column")


def _prepare_frame(frame: pd.DataFrame, mapping: list[tuple[str, str]]) -> pd.DataFrame:
    missing = [source for source, _ in mapping if source not in frame.columns]
    if missing:
        raise PostgresqlLoadError(f"Source columns not found in DataFrame: {missing}")
    prepared = frame[[source for source, _ in mapping]].copy()
    prepared.columns = [target for _, target in mapping]
    return prepared.fillna("").astype(str)


def _validate_database_contract(
    cursor: Any, schema: str, target_table: str, mapping: list[tuple[str, str]]
) -> None:
    if not _table_exists(cursor, schema, target_table):
        raise PostgresqlLoadError(f"Target table does not exist: {schema}.{target_table}")
    if not _table_exists(cursor, schema, "load_control"):
        raise PostgresqlLoadError(f"load_control table does not exist: {schema}.load_control")
    expected_columns = [target for _, target in mapping]
    actual_columns = _table_columns(cursor, schema, target_table)
    if actual_columns != expected_columns:
        raise PostgresqlLoadError(
            "Target columns do not match mapping: "
            f"expected {expected_columns}, got {actual_columns}"
        )
    load_control_columns = _table_columns(cursor, schema, "load_control")
    missing_control = [
        column for column in LOAD_CONTROL_COLUMNS if column not in load_control_columns
    ]
    if missing_control:
        raise PostgresqlLoadError(f"load_control missing columns: {missing_control}")


def _validate_partition_column(
    cursor: Any,
    schema: str,
    target_table: str,
    frame: pd.DataFrame,
    load_mode: str,
    partition_column: str | None,
) -> None:
    if load_mode != "replace_partition":
        return
    if partition_column is None:
        raise PostgresqlLoadError(
            "load.partition_column is required for replace_partition."
        )
    if partition_column not in frame.columns:
        raise PostgresqlLoadError(
            "load.partition_column is not present in processed DataFrame: "
            f"{partition_column}"
        )
    if partition_column not in _table_columns(cursor, schema, target_table):
        raise PostgresqlLoadError(
            "load.partition_column is not present in target table "
            f"{schema}.{target_table}: {partition_column}"
        )


def _validate_source_hash(
    cursor: Any,
    schema: str,
    flow_name: str,
    target_table: str,
    source_hash: str,
    reload_existing_hash: bool,
) -> None:
    if reload_existing_hash:
        return
    cursor.execute(
        f"""
        select count(*)
        from {_quote(schema)}.load_control
        where flow_name = %s
          and target_table = %s
          and source_hash = %s
          and status = 'success'
        """,
        (flow_name, target_table, source_hash),
    )
    count = _scalar(cursor)
    if int(count or 0) > 0:
        raise PostgresqlLoadError(
            "Source hash already loaded for flow/table; set "
            "reload_existing_hash to true to override."
        )


def _validate_load_mode_preconditions(
    cursor: Any, schema: str, target_table: str, load_mode: str
) -> None:
    if load_mode == "fail":
        cursor.execute(f"select count(*) from {_quote(schema)}.{_quote(target_table)}")
        count = _scalar(cursor)
        if int(count or 0) > 0:
            raise PostgresqlLoadError(
                f"Target table {schema}.{target_table} already has rows."
            )
        return
    if load_mode in ("append", "replace", "replace_partition"):
        return
    raise PostgresqlLoadError(f"Unsupported load mode: {load_mode}")


def _apply_load_mode(cursor: Any, schema: str, target_table: str, load_mode: str) -> None:
    if load_mode in ("append", "fail"):
        return
    if load_mode == "replace":
        cursor.execute(f"delete from {_quote(schema)}.{_quote(target_table)}")
        return
    raise PostgresqlLoadError(f"Unsupported load mode: {load_mode}")


def _replace_partition(
    cursor: Any,
    schema: str,
    target_table: str,
    frame: pd.DataFrame,
    partition_column: str | None,
) -> int:
    if partition_column is None:
        raise PostgresqlLoadError(
            "load.partition_column is required for replace_partition."
        )
    cursor.execute(
        f"""
        create temporary table {_quote(STAGING_TABLE)}
        (like {_qualified_table(schema, target_table)} including defaults)
        on commit drop
        """
    )
    rows_loaded = _insert_rows(cursor, None, STAGING_TABLE, frame)
    partition_values = _staging_partition_values(cursor, STAGING_TABLE, partition_column)
    if not partition_values:
        raise PostgresqlLoadError(
            "replace_partition found no partition values in "
            f"{partition_column}."
        )
    cursor.execute(
        f"""
        delete from {_qualified_table(schema, target_table)}
        where {_quote(partition_column)} in (
            select distinct {_quote(partition_column)}
            from {_quote(STAGING_TABLE)}
            where {_quote(partition_column)} is not null
              and {_quote(partition_column)} <> ''
        )
        """
    )
    columns = [_identifier(str(column), "column") for column in frame.columns]
    cursor.execute(
        f"""
        insert into {_qualified_table(schema, target_table)}
        ({', '.join(_quote(column) for column in columns)})
        select {', '.join(_quote(column) for column in columns)}
        from {_quote(STAGING_TABLE)}
        """
    )
    return rows_loaded


def _staging_partition_values(
    cursor: Any, staging_table: str, partition_column: str
) -> list[Any]:
    cursor.execute(
        f"""
        select distinct {_quote(partition_column)}
        from {_quote(staging_table)}
        where {_quote(partition_column)} is not null
          and {_quote(partition_column)} <> ''
        """
    )
    return [row[0] for row in cursor.fetchall()]


def _insert_rows(
    cursor: Any, schema: str | None, target_table: str, frame: pd.DataFrame
) -> int:
    columns = [_identifier(str(column), "column") for column in frame.columns]
    if not columns:
        return 0
    sql = (
        f"insert into {_qualified_table(schema, target_table)} "
        f"({', '.join(_quote(column) for column in columns)}) "
        f"values ({', '.join(['%s'] * len(columns))})"
    )
    rows = [tuple(row) for row in frame.itertuples(index=False, name=None)]
    if rows:
        executemany = getattr(cursor, "executemany", None)
        if callable(executemany):
            executemany(sql, rows)
        else:
            for row in rows:
                cursor.execute(sql, row)
    return len(rows)


def _insert_load_control(
    cursor: Any,
    schema: str,
    *,
    config: FlowConfig,
    source_filename: str,
    source_hash: str,
    target_table: str,
    load_mode: str,
    rows_read: int,
    rows_loaded: int,
    status: str,
    error_message: str | None,
) -> None:
    values = (
        config.name,
        config.env,
        source_filename,
        source_hash,
        schema,
        target_table,
        load_mode,
        rows_read,
        rows_loaded,
        status,
        datetime.now(timezone.utc),
        error_message,
    )
    cursor.execute(
        f"""
        insert into {_quote(schema)}.load_control
        ({', '.join(_quote(column) for column in LOAD_CONTROL_COLUMNS)})
        values ({', '.join(['%s'] * len(LOAD_CONTROL_COLUMNS))})
        """,
        values,
    )


def _table_exists(cursor: Any, schema: str, table: str) -> bool:
    cursor.execute(
        """
        select exists (
            select 1
            from information_schema.tables
            where table_schema = %s
              and table_name = %s
        )
        """,
        (schema, table),
    )
    return bool(_scalar(cursor))


def _table_columns(cursor: Any, schema: str, table: str) -> list[str]:
    cursor.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s
          and table_name = %s
        order by ordinal_position
        """,
        (schema, table),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _scalar(cursor: Any) -> Any:
    row = cursor.fetchone()
    return row[0] if row else None


def _commit(connection: Any) -> None:
    commit = getattr(connection, "commit", None)
    if callable(commit):
        commit()


def _rollback(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback) and not _connection_closed(connection):
        rollback()


def _connection_closed(connection: Any) -> bool:
    closed = getattr(connection, "closed", False)
    if isinstance(closed, bool):
        return closed
    return bool(closed)


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_PATTERN.fullmatch(value):
        raise PostgresqlLoadError(f"{label} must be a PostgreSQL identifier.")
    return value


def _quote(identifier: str) -> str:
    return f'"{_identifier(identifier, "identifier")}"'


def _qualified_table(schema: str | None, table: str) -> str:
    if schema is None:
        return _quote(table)
    return f"{_quote(schema)}.{_quote(table)}"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _default_password_provider() -> str:
    password = os.environ.get("POSTGRES_PASSWORD")
    if password:
        return password
    return getpass.getpass("PostgreSQL password: ")


def _default_confirm_callback(message: str) -> bool:
    response = input(f"{message} Type 'yes' to continue: ")
    return response == "yes"


def _default_connection_factory(**kwargs: Any) -> Any:
    import psycopg

    return psycopg.connect(**kwargs)
