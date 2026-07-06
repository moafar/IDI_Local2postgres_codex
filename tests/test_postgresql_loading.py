"""Tests for PostgreSQL loading."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.flows.runner import run_flow
from up_to_postgresql.loading import PostgresqlLoadError, load_to_postgresql
from up_to_postgresql.source import with_source_path


LOAD_CONTROL_COLUMNS = [
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
]


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.result: list[tuple[Any, ...]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.connection.begin()
        self.connection.statements.append((sql, params))
        normalized = " ".join(sql.lower().split())
        if "from information_schema.tables" in normalized:
            schema, table = params or ("", "")
            self.result = [((schema, table) in self.connection.tables,)]
        elif "from information_schema.columns" in normalized:
            schema, table = params or ("", "")
            self.result = [
                (column,)
                for column in self.connection.table_columns_for_query(schema, table)
            ]
        elif "from \"test\".load_control" in normalized and "count(*)" in normalized:
            flow_name, target_table, source_hash = params or ("", "", "")
            count = sum(
                1
                for row in self.connection.load_control
                if row["flow_name"] == flow_name
                and row["target_table"] == target_table
                and row["source_hash"] == source_hash
                and row["status"] == "success"
            )
            self.result = [(count,)]
        elif "select count(*) from \"test\".\"items\"" in normalized:
            self.result = [(len(self.connection.rows),)]
        elif normalized.startswith("create temporary table"):
            self.connection.staging_rows = []
            self.result = []
        elif (
            normalized.startswith("select distinct")
            and "from \"_uptopg_load_staging\"" in normalized
        ):
            partition_index = self.connection.tables[("test", "items")].index(
                self.connection.partition_column
            )
            values = sorted(
                {
                    row[partition_index]
                    for row in self.connection.staging_rows
                    if row[partition_index] not in ("", None)
                }
            )
            self.result = [(value,) for value in values]
        elif normalized.startswith("delete from \"test\".\"items\" where"):
            partition_index = self.connection.tables[("test", "items")].index(
                self.connection.partition_column
            )
            partition_values = {
                row[partition_index]
                for row in self.connection.staging_rows
                if row[partition_index] not in ("", None)
            }
            self.connection.deleted_partition_values = partition_values
            self.connection.rows = [
                row
                for row in self.connection.rows
                if row[partition_index] not in partition_values
            ]
            self.result = []
        elif normalized.startswith("delete from"):
            self.connection.deleted = True
            self.connection.rows.clear()
            self.result = []
        elif normalized.startswith("insert into \"test\".load_control"):
            row = dict(zip(LOAD_CONTROL_COLUMNS, params or (), strict=False))
            self.connection.load_control.append(row)
            self.result = []
        elif normalized.startswith("insert into \"_uptopg_load_staging\""):
            self.connection.staging_rows.append(tuple(params or ()))
            self.result = []
        elif (
            normalized.startswith("insert into \"test\".\"items\"")
            and "select" in normalized
            and "from \"_uptopg_load_staging\"" in normalized
        ):
            if self.connection.fail_insert:
                raise RuntimeError("insert failed")
            self.connection.rows.extend(self.connection.staging_rows)
            self.result = []
        elif normalized.startswith("insert into \"test\".\"items\""):
            if self.connection.fail_insert:
                raise RuntimeError("insert failed")
            self.connection.rows.append(tuple(params or ()))
            self.result = []

    def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        for row in rows:
            self.execute(sql, row)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.result[0] if self.result else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.result


class FakeConnection:
    def __init__(self) -> None:
        self.tables = {
            ("test", "items"): ["id", "name"],
            ("test", "load_control"): LOAD_CONTROL_COLUMNS,
        }
        self.rows: list[tuple[Any, ...]] = []
        self.load_control: list[dict[str, Any]] = []
        self.statements: list[tuple[str, tuple[Any, ...] | None]] = []
        self.closed = False
        self.deleted = False
        self.deleted_partition_values: set[Any] = set()
        self.staging_rows: list[tuple[Any, ...]] = []
        self.partition_column = "id"
        self.fail_insert = False
        self.commits = 0
        self.rollbacks = 0
        self._snapshot: dict[str, Any] | None = None

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def table_columns_for_query(self, schema: str, table: str) -> list[str]:
        return self.tables[(schema, table)]

    def begin(self) -> None:
        if self._snapshot is not None:
            return
        self._snapshot = {
            "rows": list(self.rows),
            "load_control": [dict(row) for row in self.load_control],
            "deleted": self.deleted,
            "deleted_partition_values": set(self.deleted_partition_values),
            "staging_rows": list(self.staging_rows),
        }

    def commit(self) -> None:
        self.commits += 1
        self._snapshot = None
        self.staging_rows = []

    def rollback(self) -> None:
        self.rollbacks += 1
        if self._snapshot is None:
            return
        self.rows = self._snapshot["rows"]
        self.load_control = self._snapshot["load_control"]
        self.deleted = self._snapshot["deleted"]
        self.deleted_partition_values = self._snapshot["deleted_partition_values"]
        self.staging_rows = self._snapshot["staging_rows"]
        self._snapshot = None

    def close(self) -> None:
        self.closed = True


class MissingPartitionTargetConnection(FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.tables[("test", "items")] = ["id", "name", "any_prestacio"]
        self.item_column_queries = 0

    def table_columns_for_query(self, schema: str, table: str) -> list[str]:
        if (schema, table) == ("test", "items"):
            self.item_column_queries += 1
            if self.item_column_queries > 1:
                return ["id", "name"]
        return super().table_columns_for_query(schema, table)


class CloseOnErrorConnection(FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.closed_cursor_attempts = 0

    def __exit__(self, exc_type: type[BaseException] | None, *_args: Any) -> None:
        if exc_type is not None:
            self.close()
        return None

    def cursor(self) -> FakeCursor:
        if self.closed:
            self.closed_cursor_attempts += 1
            raise RuntimeError("the connection is closed")
        return super().cursor()


def base_config(
    tmp_path: Path, *, load_mode: str = "append", partition_column: str | None = None
) -> FlowConfig:
    source = tmp_path / "input.csv"
    source.write_text("id,name\n1,Ana\n", encoding="utf-8")
    load = {
        "target_table": "items",
        "load_mode": load_mode,
        "reload_existing_hash": False,
        "column_mapping": [
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
        ],
    }
    if partition_column is not None:
        load["partition_column"] = partition_column
    return FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_base_dir": str(tmp_path), "output_base_dir": str(tmp_path)},
            "source": {"type": "csv", "path": "input.csv"},
            "postgresql": {
                "host": "localhost",
                "port": 5433,
                "database": "data_dbo_idi",
                "user": "rom",
                "schema": "test",
            },
            "load": load,
        },
    )


def test_load_builds_connection_without_config_password(tmp_path: Path) -> None:
    connection = FakeConnection()
    captured: dict[str, Any] = {}

    def connect(**kwargs: Any) -> FakeConnection:
        captured.update(kwargs)
        return connection

    result = load_to_postgresql(
        base_config(tmp_path),
        pd.DataFrame({"id": [1], "name": ["Ana"]}),
        connection_factory=connect,
        password_provider=lambda: "secret",
        confirm_callback=lambda _message: True,
    )

    assert captured == {
        "host": "localhost",
        "port": 5433,
        "dbname": "data_dbo_idi",
        "user": "rom",
        "password": "secret",
    }
    assert result.rows_loaded == 1
    assert connection.rows == [("1", "Ana")]
    assert connection.load_control[-1]["status"] == "success"


def test_load_control_and_confirmation_use_real_source_file(tmp_path: Path) -> None:
    (tmp_path / "configured.csv").write_text("id,name\n0,Wrong\n", encoding="utf-8")
    real_source = tmp_path / "real.csv"
    real_source.write_text("id,name\n1,Ana\n", encoding="utf-8")
    connection = FakeConnection()
    messages: list[str] = []
    config = with_source_path(base_config(tmp_path), "real.csv")

    result = load_to_postgresql(
        config,
        pd.DataFrame({"id": ["1"], "name": ["Ana"]}),
        connection_factory=lambda **_kwargs: connection,
        password_provider=lambda: "secret",
        confirm_callback=lambda message: messages.append(message) or True,
    )

    expected_hash = hashlib.sha256(real_source.read_bytes()).hexdigest()
    assert result.source_filename == str(real_source.resolve())
    assert result.source_hash == expected_hash
    assert connection.load_control[-1]["source_filename"] == str(real_source.resolve())
    assert connection.load_control[-1]["source_hash"] == expected_hash
    assert str(real_source.resolve()) in messages[0]
    assert "1 rows" in messages[0]
    assert "test.items" in messages[0]
    assert "append" in messages[0]


def test_load_reads_password_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = FakeConnection()
    captured: dict[str, Any] = {}
    monkeypatch.setenv("POSTGRES_PASSWORD", "env-secret")

    def connect(**kwargs: Any) -> FakeConnection:
        captured.update(kwargs)
        return connection

    result = load_to_postgresql(
        base_config(tmp_path),
        pd.DataFrame({"id": [1], "name": ["Ana"]}),
        connection_factory=connect,
        confirm_callback=lambda _message: True,
    )

    assert captured["password"] == "env-secret"
    assert "env-secret" not in repr(result)
    assert "env-secret" not in repr(connection.load_control)


def test_load_rejects_missing_source_column(tmp_path: Path) -> None:
    connection = FakeConnection()

    with pytest.raises(PostgresqlLoadError, match="Source columns"):
        load_to_postgresql(
            base_config(tmp_path),
            pd.DataFrame({"id": ["1"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )


def test_load_rejects_missing_target_table(tmp_path: Path) -> None:
    connection = FakeConnection()
    del connection.tables[("test", "items")]

    with pytest.raises(PostgresqlLoadError, match="Target table does not exist"):
        load_to_postgresql(
            base_config(tmp_path),
            pd.DataFrame({"id": ["1"], "name": ["Ana"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )

    assert connection.load_control[-1]["status"] == "error"


def test_load_rejects_target_columns_that_do_not_match_mapping(tmp_path: Path) -> None:
    connection = FakeConnection()
    connection.tables[("test", "items")] = ["id", "other"]

    with pytest.raises(PostgresqlLoadError, match="Target columns"):
        load_to_postgresql(
            base_config(tmp_path),
            pd.DataFrame({"id": ["1"], "name": ["Ana"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )


def test_load_rejects_duplicate_hash(tmp_path: Path) -> None:
    connection = FakeConnection()
    config = base_config(tmp_path)
    first = load_to_postgresql(
        config,
        pd.DataFrame({"id": ["1"], "name": ["Ana"]}),
        connection_factory=lambda **_kwargs: connection,
        password_provider=lambda: "secret",
        confirm_callback=lambda _message: True,
    )

    with pytest.raises(PostgresqlLoadError, match="Source hash already loaded"):
        load_to_postgresql(
            config,
            pd.DataFrame({"id": ["2"], "name": ["Bob"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )

    assert first.source_hash == connection.load_control[0]["source_hash"]


def test_duplicate_hash_does_not_mask_error_with_closed_connection(
    tmp_path: Path,
) -> None:
    connection = CloseOnErrorConnection()
    config = base_config(tmp_path)
    source_hash = hashlib.sha256((tmp_path / "input.csv").read_bytes()).hexdigest()
    connection.load_control.append(
        {
            "flow_name": config.name,
            "target_table": "items",
            "source_hash": source_hash,
            "status": "success",
        }
    )

    with pytest.raises(PostgresqlLoadError, match="Source hash already loaded"):
        load_to_postgresql(
            config,
            pd.DataFrame({"id": ["2"], "name": ["Bob"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )

    assert connection.rows == []
    assert connection.closed_cursor_attempts == 0
    assert connection.load_control[-1]["status"] == "error"
    assert "Source hash already loaded" in connection.load_control[-1]["error_message"]


def test_same_hash_is_allowed_for_different_flow_or_table(tmp_path: Path) -> None:
    connection = FakeConnection()
    config = base_config(tmp_path)
    first = load_to_postgresql(
        config,
        pd.DataFrame({"id": ["1"], "name": ["Ana"]}),
        connection_factory=lambda **_kwargs: connection,
        password_provider=lambda: "secret",
        confirm_callback=lambda _message: True,
    )
    connection.load_control[0]["flow_name"] = "other_flow"

    second = load_to_postgresql(
        config,
        pd.DataFrame({"id": ["2"], "name": ["Bob"]}),
        connection_factory=lambda **_kwargs: connection,
        password_provider=lambda: "secret",
        confirm_callback=lambda _message: True,
    )

    assert second.source_hash == first.source_hash
    assert connection.rows[-1] == ("2", "Bob")


def test_load_modes_fail_append_and_replace(tmp_path: Path) -> None:
    append_connection = FakeConnection()
    load_to_postgresql(
        base_config(tmp_path, load_mode="append"),
        pd.DataFrame({"id": ["1"], "name": ["Ana"]}),
        connection_factory=lambda **_kwargs: append_connection,
        password_provider=lambda: "secret",
        confirm_callback=lambda _message: True,
    )
    assert append_connection.rows == [("1", "Ana")]
    assert not append_connection.deleted

    replace_connection = FakeConnection()
    replace_connection.rows = [("old", "row")]
    load_to_postgresql(
        base_config(tmp_path, load_mode="replace"),
        pd.DataFrame({"id": ["2"], "name": ["Bob"]}),
        connection_factory=lambda **_kwargs: replace_connection,
        password_provider=lambda: "secret",
        confirm_callback=lambda _message: True,
    )
    assert replace_connection.deleted
    assert replace_connection.rows == [("2", "Bob")]

    fail_connection = FakeConnection()
    fail_connection.rows = [("old", "row")]
    with pytest.raises(PostgresqlLoadError, match="already has rows"):
        load_to_postgresql(
            base_config(tmp_path, load_mode="fail"),
            pd.DataFrame({"id": ["3"], "name": ["Carla"]}),
            connection_factory=lambda **_kwargs: fail_connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )


def test_replace_partition_requires_partition_column(tmp_path: Path) -> None:
    connection = FakeConnection()

    with pytest.raises(PostgresqlLoadError, match="partition_column is required"):
        load_to_postgresql(
            base_config(tmp_path, load_mode="replace_partition"),
            pd.DataFrame({"id": ["1"], "name": ["Ana"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )


def test_replace_partition_rejects_missing_dataframe_column(tmp_path: Path) -> None:
    connection = FakeConnection()

    with pytest.raises(PostgresqlLoadError, match="processed DataFrame"):
        load_to_postgresql(
            base_config(
                tmp_path,
                load_mode="replace_partition",
                partition_column="any_prestacio",
            ),
            pd.DataFrame({"id": ["1"], "name": ["Ana"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )


def test_replace_partition_rejects_missing_target_column(tmp_path: Path) -> None:
    connection = MissingPartitionTargetConnection()
    config = base_config(
        tmp_path, load_mode="replace_partition", partition_column="any_prestacio"
    )
    config.data["load"]["column_mapping"].append(
        {"source": "any", "target": "any_prestacio"}
    )

    with pytest.raises(PostgresqlLoadError, match="target table"):
        load_to_postgresql(
            config,
            pd.DataFrame({"id": ["1"], "name": ["Ana"], "any": ["2026"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )


def test_replace_partition_requires_partition_values(tmp_path: Path) -> None:
    connection = FakeConnection()

    with pytest.raises(PostgresqlLoadError, match="no partition values"):
        load_to_postgresql(
            base_config(tmp_path, load_mode="replace_partition", partition_column="id"),
            pd.DataFrame({"id": [""], "name": ["Ana"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )

    assert connection.rows == []
    assert connection.load_control[-1]["status"] == "error"


def test_replace_partition_deletes_only_staged_partition_values(
    tmp_path: Path,
) -> None:
    connection = FakeConnection()
    connection.rows = [("2025", "Old"), ("2026", "Previous"), ("2027", "Future")]

    result = load_to_postgresql(
        base_config(tmp_path, load_mode="replace_partition", partition_column="id"),
        pd.DataFrame({"id": ["2026"], "name": ["Current"]}),
        connection_factory=lambda **_kwargs: connection,
        password_provider=lambda: "secret",
        confirm_callback=lambda _message: True,
    )

    assert result.load_mode == "replace_partition"
    assert connection.deleted_partition_values == {"2026"}
    assert connection.rows == [
        ("2025", "Old"),
        ("2027", "Future"),
        ("2026", "Current"),
    ]
    assert connection.load_control[-1]["load_mode"] == "replace_partition"
    assert connection.load_control[-1]["status"] == "success"


def test_replace_partition_rolls_back_when_insert_fails(tmp_path: Path) -> None:
    connection = FakeConnection()
    connection.rows = [("2025", "Old"), ("2026", "Previous")]
    connection.fail_insert = True

    with pytest.raises(RuntimeError, match="insert failed"):
        load_to_postgresql(
            base_config(tmp_path, load_mode="replace_partition", partition_column="id"),
            pd.DataFrame({"id": ["2026"], "name": ["Current"]}),
            connection_factory=lambda **_kwargs: connection,
            password_provider=lambda: "secret",
            confirm_callback=lambda _message: True,
        )

    assert connection.rows == [("2025", "Old"), ("2026", "Previous")]
    assert connection.rollbacks == 1
    assert connection.load_control[-1]["load_mode"] == "replace_partition"
    assert connection.load_control[-1]["status"] == "error"


def test_run_flow_without_load_does_not_touch_postgresql(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text("id,name\n1,Ana\n", encoding="utf-8")
    config = base_config(tmp_path)

    result = run_flow(
        config,
        connection_factory=lambda **_kwargs: pytest.fail("unexpected PostgreSQL connect"),
    )

    assert result.postgresql is None
    assert result.rows_written == 1
