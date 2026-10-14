"""Verify the demanda substitutions executor and its transaction boundaries.

Depends on pytest and psycopg; injected connections test transaction behavior.
Optional integration uses only local uptopg_test over /var/run/postgresql as
rom, creates disposable test tables, and removes them afterward. No Aurora
access or stored passwords.
"""

from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path

import psycopg
from psycopg import sql
import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.flows.demanda_sustituciones import (
    DemandaSustitucionesError,
    generar_sustituciones,
)
from up_to_postgresql.flows.demanda_sustituciones_sql import (
    ALGORITHM_VERSION,
    RESULT_COLUMNS,
    SOURCE_COLUMNS,
    build_queries,
)


_DDL_PATH = (
    Path(__file__).resolve().parents[1] / "docs/sql/create_demanda_sustituciones.sql"
)
_REQUIRED_CONSTRAINTS = (
    "demanda_sustituciones_pkey",
    "ds_identificadores",
    "ds_dominio",
    "ds_clasificacion",
    "ds_candidata",
    "ds_nivel_9",
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=None):
        self.last_query = query
        self.connection.executions.append((query, params))
        if query == self.connection.fail_query:
            raise RuntimeError("SQL execution failed")

    def fetchall(self):
        if "information_schema.columns" in self.last_query:
            return self.connection.columns
        if "information_schema.table_constraints" in self.last_query:
            return self.connection.constraints
        if "information_schema.key_column_usage" in self.last_query:
            return self.connection.primary_key_columns
        raise AssertionError("unexpected fetchall")

    def fetchone(self):
        if "FROM pg_temp._ds_result" in self.last_query:
            return self.connection.counts
        raise AssertionError("unexpected fetchone")


class FakeConnection:
    autocommit = False

    def __init__(self):
        self.executions = []
        self.calls = []
        self.fail_query = None
        self.counts = (3, 1, 1, 0, 1, 3)
        self.columns = [
            (
                name,
                {"any_prestacio": "int2", "numero_prestacions_al": "int8",
                 "mismo_nivel_9": "bool", "numero_candidatas": "int8",
                 "fecha_carga": "timestamptz"}.get(name, "text"),
                "NO" if name in {"id_relacion", "id_al", "any_prestacio",
                                 "numero_prestacions_al", "numero_candidatas",
                                 "clasificacion", "version_algoritmo", "fecha_carga"}
                else "YES",
                None if name in {"any_prestacio", "numero_prestacions_al",
                                 "mismo_nivel_9", "numero_candidatas", "fecha_carga"}
                else "C",
            )
            for name in RESULT_COLUMNS
        ]
        self.constraints = [
            (name, "PRIMARY KEY" if name.endswith("_pkey") else "CHECK")
            for name in _REQUIRED_CONSTRAINTS
        ]
        self.primary_key_columns = [("id_relacion",)]

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.calls.append("commit")
        if self.fail_query == "commit":
            raise RuntimeError("commit failed")

    def rollback(self):
        self.calls.append("rollback")

    def close(self):
        self.calls.append("close")


@pytest.fixture
def config():
    return resolve_flow_config("demanda", "test")


@pytest.fixture
def fake():
    connection = FakeConnection()
    captured = []

    def factory(**kwargs):
        captured.append(kwargs)
        return connection

    return connection, factory, captured


@pytest.mark.parametrize("anio", [None, "2026", 0, -1, 10000, True, 2026.0])
def test_reject_invalid_year_before_connection(config, fake, anio):
    _, factory, captured = fake
    with pytest.raises(DemandaSustitucionesError, match="anio"):
        generar_sustituciones(config, anio=anio, connection_factory=factory,
                              password="unused")
    assert not captured


def test_reject_wrong_flow_or_environment(config, fake):
    _, factory, captured = fake
    for changed in (
        FlowConfig("other", "test", config.data),
        FlowConfig("demanda", "invalid", config.data),
        FlowConfig(
            "demanda", "test",
            {**config.data, "postgresql": {**config.data["postgresql"], "schema": "prd"}},
        ),
    ):
        with pytest.raises(ValueError):
            generar_sustituciones(changed, anio=2026,
                                  connection_factory=factory, password="unused")
    assert not captured


def test_password_provider_and_connection_settings(config, fake):
    connection, factory, captured = fake
    called = []
    result = generar_sustituciones(
        config, anio=2026, connection_factory=factory,
        password_provider=lambda: called.append(True) or "secret",
    )
    assert called == [True]
    assert captured == [
        {"host": config.data["postgresql"]["host"], "port": 5432,
         "dbname": config.data["postgresql"]["database"],
         "user": config.data["postgresql"]["user"], "password": "secret"}
    ]
    assert result.estado == "success"
    assert result.version_algoritmo == ALGORITHM_VERSION
    assert connection.calls == ["commit", "close"]



def test_progress_reports_completed_phases(config, fake, monkeypatch):
    """Progress counts completed SQL statements, not rows inside a JOIN."""
    import up_to_postgresql.flows.demanda_sustituciones as executor

    seen = {"descriptions": [], "completed": 0, "messages": []}

    class FakeProgress:
        def __init__(self, *, total, desc, unit, dynamic_ncols):
            assert total == len(build_queries("test"))
            assert desc == "Sustituciones 2026"
            assert unit == "fase"
            assert dynamic_ncols is True

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def set_postfix_str(self, name, refresh):
            assert refresh is True
            seen["descriptions"].append(name)

        def update(self, amount):
            seen["completed"] += amount

    def fake_tqdm(**kwargs):
        return FakeProgress(**kwargs)

    fake_tqdm.write = seen["messages"].append
    monkeypatch.setattr(executor, "tqdm", fake_tqdm)
    _, factory, _ = fake
    generar_sustituciones(
        config, anio=2026, connection_factory=factory, password="unused"
    )
    assert seen["descriptions"] == list(build_queries("test"))
    assert seen["completed"] == len(build_queries("test"))
    assert len(seen["messages"]) == seen["completed"]
    assert all(" s" in message for message in seen["messages"])


def test_progress_failure_does_not_advance_failed_phase(config, fake, monkeypatch):
    """A failed SQL statement is reported, then normal rollback takes place."""
    import up_to_postgresql.flows.demanda_sustituciones as executor

    seen = {"completed": 0, "messages": []}

    class FakeProgress:
        def __init__(self, **_):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def set_postfix_str(self, *_args, **_kwargs):
            pass

        def update(self, amount):
            seen["completed"] += amount

    def fake_tqdm(**kwargs):
        return FakeProgress(**kwargs)

    fake_tqdm.write = seen["messages"].append
    monkeypatch.setattr(executor, "tqdm", fake_tqdm)
    connection, factory, _ = fake
    queries = build_queries("test")
    failed = "match"
    connection.fail_query = queries[failed]
    with pytest.raises(RuntimeError, match="SQL execution failed"):
        generar_sustituciones(
            config, anio=2026, connection_factory=factory, password="unused"
        )
    assert seen["completed"] == list(queries).index(failed)
    assert any(f"{failed}: ERROR" in message for message in seen["messages"])
    assert connection.calls == ["rollback", "close"]


def test_explicit_password_bypasses_provider(config, fake):
    _, factory, captured = fake
    generar_sustituciones(
        config, anio=2026, connection_factory=factory, password="explicit",
        password_provider=lambda: pytest.fail("provider should not be called"),
    )
    assert captured[0]["password"] == "explicit"


def test_sql_order_parameters_and_al_occurrence_counts(config, fake):
    connection, factory, _ = fake
    result = generar_sustituciones(
        config, anio=2026, connection_factory=factory, password="unused"
    )
    queries = build_queries("test")
    executed = connection.executions
    assert [query for query, _ in executed[3:-1]] == list(queries.values())
    assert executed[3][1] == {"anio": 2026}
    assert all(params is None for _, params in executed[4:])
    assert "count(DISTINCT id_al)" in executed[-1][0]
    assert executed[-2][0] == queries["validate_published"]
    assert (result.registros_al, result.candidatas_unicas,
            result.candidatas_multiples, result.sin_candidata,
            result.registros_agregados, result.relaciones_generadas) == (
                3, 1, 1, 0, 1, 3,
            )
    assert result.duracion_segundos >= 0
    assert connection.calls == ["commit", "close"]


@pytest.mark.parametrize("failed_step", ["validate_source", "validate_result",
                                        "delete_year", "validate_published"])
def test_sql_failure_rolls_back(config, fake, failed_step):
    connection, factory, _ = fake
    connection.fail_query = build_queries("test")[failed_step]
    with pytest.raises(RuntimeError, match="SQL execution failed"):
        generar_sustituciones(config, anio=2026,
                              connection_factory=factory, password="unused")
    assert connection.calls == ["rollback", "close"]


def test_commit_failure_rolls_back_and_does_not_report_success(config, fake):
    connection, factory, _ = fake
    connection.fail_query = "commit"
    with pytest.raises(RuntimeError, match="commit failed"):
        generar_sustituciones(config, anio=2026,
                              connection_factory=factory, password="unused")
    assert connection.calls == ["commit", "rollback", "close"]


def test_close_error_after_commit_preserves_commit_information(config, fake):
    connection, factory, _ = fake

    def fail_close():
        connection.calls.append("close")
        raise RuntimeError("close failed")

    connection.close = fail_close
    with pytest.raises(RuntimeError, match="close failed") as caught:
        generar_sustituciones(config, anio=2026,
                              connection_factory=factory, password="unused")
    assert connection.calls == ["commit", "close"]
    assert caught.value.demanda_sustituciones_commit_confirmed is True


@pytest.mark.parametrize("contract_part", ["columns", "constraints",
                                          "primary_key_columns"])
def test_missing_target_contract_prevents_sql(config, fake, contract_part):
    connection, factory, _ = fake
    getattr(connection, contract_part).pop()
    with pytest.raises(DemandaSustitucionesError,
                       match="contract|constraints|primary key"):
        generar_sustituciones(config, anio=2026,
                              connection_factory=factory, password="unused")
    assert len(connection.executions) <= 3
    assert connection.calls == ["rollback", "close"]


def test_absent_target_prevents_sql(config, fake):
    connection, factory, _ = fake
    connection.columns.clear()
    with pytest.raises(DemandaSustitucionesError, match="column contract"):
        generar_sustituciones(config, anio=2026,
                              connection_factory=factory, password="unused")
    assert len(connection.executions) == 1
    assert connection.calls == ["rollback", "close"]


def test_autocommit_is_rejected(config, fake):
    connection, factory, _ = fake
    connection.autocommit = True
    with pytest.raises(DemandaSustitucionesError, match="transactional"):
        generar_sustituciones(config, anio=2026,
                              connection_factory=factory, password="unused")
    assert not connection.executions
    assert connection.calls == ["rollback", "close"]


def test_inconsistent_summary_rolls_back(config, fake):
    connection, factory, _ = fake
    connection.counts = (4, 1, 1, 0, 1, 3)
    with pytest.raises(DemandaSustitucionesError, match="counts"):
        generar_sustituciones(config, anio=2026,
                              connection_factory=factory, password="unused")
    assert connection.calls == ["rollback", "close"]


@pytest.fixture
def local_database(config):
    if os.environ.get("UPTOPG_RUN_EXECUTOR_TESTS") != "1":
        pytest.skip("local PostgreSQL integration requires explicit opt-in")
    if os.environ.get("PGHOSTADDR") or os.environ.get("PGSERVICE"):
        pytest.fail("remove PGHOSTADDR and PGSERVICE before local integration")
    try:
        admin = psycopg.connect(host="/var/run/postgresql", port=5432,
                                dbname="uptopg_test", user="rom", connect_timeout=3)
    except psycopg.OperationalError as error:
        pytest.skip(f"local uptopg_test unavailable: {type(error).__name__}")
    schema_created = False
    tables_created = False
    try:
        with admin.cursor() as cursor:
            cursor.execute(
                "SELECT to_regnamespace('test') IS NOT NULL, "
                "to_regclass('test.demanda') IS NOT NULL, "
                "to_regclass('test.demanda_sustituciones') IS NOT NULL"
            )
            schema_exists, source_exists, target_exists = cursor.fetchone()
            if source_exists or target_exists:
                pytest.skip("local test schema already has demanda tables")
            if not schema_exists:
                cursor.execute("CREATE SCHEMA test")
                schema_created = True
            cursor.execute(
                sql.SQL("CREATE TABLE {} ({})").format(
                    sql.Identifier("test", "demanda"),
                    sql.SQL(", ").join(
                        sql.SQL('{} text COLLATE "C"').format(sql.Identifier(name))
                        for name in SOURCE_COLUMNS
                    ),
                )
            )
            ddl = _DDL_PATH.read_text(encoding="utf-8")
            statement = sql.SQL(ddl.split(':"schema"')[0])
            for part in ddl.split(':"schema"')[1:]:
                statement += sql.Identifier("test") + sql.SQL(part)
            cursor.execute(statement)
        admin.commit()
        tables_created = True
        data = deepcopy(config.data)
        data["postgresql"].update(
            host="/var/run/postgresql", database="uptopg_test", user="rom"
        )
        yield FlowConfig(name="demanda", env="test", data=data), admin
    finally:
        admin.rollback()
        if tables_created:
            with admin.cursor() as cursor:
                cursor.execute("DROP TABLE test.demanda_sustituciones")
                cursor.execute("DROP TABLE test.demanda")
                if schema_created:
                    cursor.execute("DROP SCHEMA test")
            admin.commit()
        admin.close()


def _source_row(year, state, patient, *, count="1", nivel_9="P9-1"):
    row = dict.fromkeys(SOURCE_COLUMNS, "description")
    row.update(any_prestacio=str(year), mes_prestacio="01",
               data_prestacio="15/01/26", numero_prestacions=count,
               pacient_nhc=patient, nivell_1_codi="RX",
               up_solicitant_corregida="UP-1", prestacio_estat_codi=state,
               prestacio_nivell_9_codi=nivel_9)
    return tuple(row[name] for name in SOURCE_COLUMNS)


def test_local_year_isolation_idempotence_and_rollback(local_database):
    config, admin = local_database
    with admin.cursor() as cursor:
        cursor.executemany(
            sql.SQL("INSERT INTO {} VALUES ({})").format(
                sql.Identifier("test", "demanda"),
                sql.SQL(", ").join(sql.Placeholder() for _ in SOURCE_COLUMNS),
            ),
            [_source_row(2025, "AL", "OLD"),
             _source_row(2026, "AL", "NHC-1"),
             _source_row(2026, "CF", "NHC-1"),
             _source_row(2026, "DI", "NHC-1", nivel_9="P9-2"),
             _source_row(2026, "AL", "NHC-2", count="2")],
        )
    admin.commit()
    first_year = generar_sustituciones(config, anio=2025, password="")
    current = generar_sustituciones(config, anio=2026, password="")
    assert first_year.sin_candidata == 1
    assert (current.registros_al, current.candidatas_multiples,
            current.registros_agregados, current.relaciones_generadas) == (2, 1, 1, 2)
    with admin.cursor() as cursor:
        cursor.execute("SELECT any_prestacio, id_relacion FROM test.demanda_sustituciones "
                       "ORDER BY any_prestacio, id_relacion")
        before = cursor.fetchall()
    again = generar_sustituciones(config, anio=2026, password="")
    assert again.relaciones_generadas == current.relaciones_generadas
    with admin.cursor() as cursor:
        cursor.execute("SELECT any_prestacio, id_relacion FROM test.demanda_sustituciones "
                       "ORDER BY any_prestacio, id_relacion")
        assert cursor.fetchall() == before

    class FailingCursor:
        def __init__(self, inner):
            self.inner = inner

        def __enter__(self):
            self.inner.__enter__()
            return self

        def __exit__(self, *args):
            return self.inner.__exit__(*args)

        def execute(self, query, params=None):
            if query == build_queries("test")["insert_year"]:
                raise RuntimeError("injected failure after delete")
            return self.inner.execute(query, params)

        def fetchall(self):
            return self.inner.fetchall()

        def fetchone(self):
            return self.inner.fetchone()

    class FailingConnection:
        def __init__(self, inner):
            self.inner = inner

        @property
        def autocommit(self):
            return self.inner.autocommit

        def cursor(self):
            return FailingCursor(self.inner.cursor())

        def commit(self):
            self.inner.commit()

        def rollback(self):
            self.inner.rollback()

        def close(self):
            self.inner.close()

    def failing_factory(**kwargs):
        return FailingConnection(psycopg.connect(**kwargs))

    with pytest.raises(RuntimeError, match="injected failure"):
        generar_sustituciones(config, anio=2026, password="",
                              connection_factory=failing_factory)
    with admin.cursor() as cursor:
        cursor.execute("SELECT any_prestacio, id_relacion FROM test.demanda_sustituciones "
                       "ORDER BY any_prestacio, id_relacion")
        assert cursor.fetchall() == before
