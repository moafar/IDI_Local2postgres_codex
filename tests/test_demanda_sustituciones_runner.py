"""Test automatic and manual demanda orchestration, reports and lock behavior.

Depends on pytest, pandas and the real flow configuration. Injected database
fakes verify ordering without Aurora; optional lock integration uses only
local uptopg_test over the Unix socket and writes no database tables.
"""

from __future__ import annotations

from copy import deepcopy
import csv
from dataclasses import replace
from datetime import datetime
import json
import os
from pathlib import Path

import pandas as pd
import psycopg
import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.flows import runner
from up_to_postgresql.flows.demanda_sustituciones import DemandaSustitucionesResult
from up_to_postgresql.flows.runner import FlowRunError, run_flow
from up_to_postgresql.loading import PostgresqlLoadResult


class LockConnection:
    def __init__(self, registry):
        self.registry = registry
        self.autocommit = False
        self.key = None
        self.row = None
        self.closed = False

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params):
        key = params[0]
        assert self.autocommit is True
        if "pg_try_advisory_lock" in query:
            self.row = (key not in self.registry.active,)
            if self.row[0]:
                self.registry.active.add(key)
                self.key = key
                self.registry.events.append("lock")
        elif "pg_advisory_unlock" in query:
            self.row = (key in self.registry.active,)
            self.registry.active.discard(key)
            self.registry.events.append("unlock")
        else:
            raise AssertionError(query)

    def fetchone(self):
        return self.row

    def close(self):
        self.closed = True
        if self.key is not None:
            self.registry.active.discard(self.key)
        self.registry.events.append("close")


class LockRegistry:
    def __init__(self):
        self.active = set()
        self.events = []
        self.connections = []
        self.credentials = []

    def connect(self, **kwargs):
        self.credentials.append(kwargs)
        connection = LockConnection(self)
        self.connections.append(connection)
        return connection


def _demanda_config(tmp_path: Path, years: list[str | None]) -> FlowConfig:
    resolved = resolve_flow_config("demanda", "test")
    data = deepcopy(resolved.data)
    data["source"]["path"] = "demanda.csv"
    data["paths"]["input_base_dir"] = str(tmp_path)
    data["paths"]["output_base_dir"] = str(tmp_path / "output")
    columns = [item["source"] for item in data["load"]["column_mapping"]]
    year_column = next(
        item["source"] for item in data["load"]["column_mapping"]
        if item["target"] == "any_prestacio"
    )
    with (tmp_path / "demanda.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, delimiter=";")
        writer.writeheader()
        for index, year in enumerate(years):
            row = dict.fromkeys(columns, "valor")
            row[year_column] = year or ""
            row["Pacient (NHC)"] = f"NHC-{index}"
            writer.writerow(row)
    return FlowConfig(name="demanda", env="test", data=data)


def _loaded(config: FlowConfig) -> PostgresqlLoadResult:
    return PostgresqlLoadResult(
        target_schema=config.env, target_table="demanda", load_mode="replace_partition",
        source_filename="demanda.csv", source_hash="hash", rows_loaded=1,
        status="success",
    )


def _analyzed(year: int = 2026) -> DemandaSustitucionesResult:
    return DemandaSustitucionesResult(
        anio_procesado=year, version_algoritmo="1.0.0", registros_al=2,
        candidatas_unicas=1, candidatas_multiples=0, sin_candidata=1,
        registros_agregados=0, relaciones_generadas=1, duracion_segundos=0.1,
        estado="success",
    )


def test_demand_load_runs_two_phases_under_one_lock(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2026"])
    registry = LockRegistry()
    monkeypatch.setattr(runner, "_current_demanda_year", lambda: 2026)
    password_calls = []

    def load(config, frame, **kwargs):
        assert registry.active == {runner._demanda_lock_key("test")}
        assert kwargs["password"] == "secret"
        assert kwargs["confirmed"] is True
        registry.events.append("load_commit")
        return _loaded(config)

    def analyze(config, *, anio, **kwargs):
        assert registry.active == {runner._demanda_lock_key("test")}
        assert anio == 2026
        assert kwargs["password"] == "secret"
        registry.events.append("analysis_commit")
        return _analyzed(anio)

    monkeypatch.setattr(runner, "load_to_postgresql", load)
    monkeypatch.setattr(runner, "generar_sustituciones", analyze)
    result = run_flow(
        config, load=True, connection_factory=registry.connect,
        password_provider=lambda: password_calls.append(True) or "secret",
        confirm_callback=lambda _: True,
    )
    assert result.postgresql.status == "success"
    assert password_calls == [True]
    assert registry.credentials[0]["password"] == "secret"
    assert registry.events == ["lock", "load_commit", "analysis_commit",
                               "unlock", "close"]
    assert registry.active == set()
    assert registry.connections[0].closed
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["postgresql"]["status"] == "success"
    assert report["demanda_sustituciones"]["relaciones_generadas"] == 1
    assert report["estado_ejecucion"] == "success"


@pytest.mark.parametrize("years", [[], [""], [None], ["202x"], ["0000"],
                                        ["02026"], ["2026", "2025"], ["2025"]])
def test_invalid_or_historical_year_cannot_start_load(tmp_path, monkeypatch, years):
    config = _demanda_config(tmp_path, years)
    monkeypatch.setattr(runner, "_current_demanda_year", lambda: 2026)
    monkeypatch.setattr(
        runner, "load_to_postgresql", lambda *_, **__: pytest.fail("unexpected load")
    )
    monkeypatch.setattr(
        runner, "generar_sustituciones", lambda *_, **__: pytest.fail("unexpected analysis")
    )
    with pytest.raises(FlowRunError):
        run_flow(config, load=True,
                 connection_factory=lambda **_: pytest.fail("unexpected connection"),
                 password_provider=lambda: pytest.fail("unexpected password prompt"),
                 confirm_callback=lambda _: pytest.fail("unexpected confirmation"))
    report = json.loads((tmp_path / "output" / "demanda_report.json").read_text())
    assert report["estado_ejecucion"] == "error"
    assert report["postgresql"]["status"] == "skipped"
    assert report["demanda_sustituciones"]["registros_al"] is None


def test_year_is_read_from_mapping_not_a_fixed_header(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2026"])
    data = deepcopy(config.data)
    mapping = data["load"]["column_mapping"]
    year_source = next(item["source"] for item in mapping
                       if item["target"] == "any_prestacio")
    next(item for item in mapping if item["target"] == "any_prestacio")["source"] = "AÑO"
    changed = FlowConfig(name="demanda", env="test", data=data)
    monkeypatch.setattr(runner, "_current_demanda_year", lambda: 2026)
    assert runner._demanda_year(changed, pd.DataFrame({"AÑO": ["2026"]})) == 2026
    with pytest.raises(FlowRunError, match="mapeo"):
        runner._demanda_year(changed, pd.DataFrame({year_source: ["2026"]}))


def test_january_does_not_allow_previous_year(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2025"])

    class JanuaryDateTime:
        @classmethod
        def now(cls, timezone):
            assert str(timezone) == "Europe/Madrid"
            return datetime(2026, 1, 5, tzinfo=timezone)

    monkeypatch.setattr(runner, "datetime", JanuaryDateTime)
    with pytest.raises(FlowRunError, match="año calendario actual"):
        runner._demanda_year(config, pd.DataFrame({
            next(item["source"] for item in config.data["load"]["column_mapping"]
                 if item["target"] == "any_prestacio"): ["2025"]
        }))


def test_demand_without_load_never_connects_or_analyzes(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2025"])
    monkeypatch.setattr(
        runner, "generar_sustituciones", lambda *_, **__: pytest.fail("unexpected analysis")
    )
    result = run_flow(
        config, connection_factory=lambda **_: pytest.fail("unexpected connection")
    )
    assert result.postgresql is None


def test_other_flow_load_uses_existing_path(tmp_path, monkeypatch):
    (tmp_path / "sample.csv").write_text("id\n1\n", encoding="utf-8")
    config = FlowConfig(
        name="sample", env="test",
        data={"source": {"type": "csv", "path": "sample.csv"},
              "paths": {"input_base_dir": str(tmp_path),
                        "output_base_dir": str(tmp_path / "out")},
              "postgresql": {"host": "unused", "port": 5432, "database": "unused",
                             "user": "unused", "schema": "test"},
              "load": {"target_table": "sample", "load_mode": "append"}},
    )
    events = []
    def fake_prepare(*_, **__):
        events.append("prepare")
        return "secret"
    def fake_load(config, frame, **kwargs):
        events.append("load")
        return _loaded(config)
    monkeypatch.setattr(runner, "prepare_postgresql_load", fake_prepare)
    monkeypatch.setattr(runner, "load_to_postgresql", fake_load)
    monkeypatch.setattr(
        runner, "generar_sustituciones", lambda *_, **__: pytest.fail("unexpected analysis")
    )
    result = run_flow(
        config, load=True,
        connection_factory=lambda **_: pytest.fail("unexpected control connection")
    )
    assert events == ["prepare", "load"]
    assert result.postgresql is not None
    assert "demanda_sustituciones" not in json.loads(result.report_path.read_text())


@pytest.mark.parametrize("stage", ["load", "analysis"])
def test_failure_releases_lock_and_preserves_transaction_boundary(
    tmp_path, monkeypatch, stage
):
    config = _demanda_config(tmp_path, ["2026"])
    registry = LockRegistry()
    loaded = []
    monkeypatch.setattr(runner, "_current_demanda_year", lambda: 2026)

    def fake_load(config, frame, **kwargs):
        if stage == "load":
            raise RuntimeError("load failed")
        loaded.append(True)  # The real loader has committed before returning.
        return _loaded(config)

    def fake_analysis(config, *, anio, **kwargs):
        assert loaded == [True]
        assert registry.active
        raise RuntimeError("analysis failed")

    monkeypatch.setattr(runner, "load_to_postgresql", fake_load)
    monkeypatch.setattr(runner, "generar_sustituciones", fake_analysis)
    with pytest.raises(RuntimeError, match=f"{stage} failed"):
        run_flow(config, load=True, connection_factory=registry.connect,
                 password_provider=lambda: "secret", confirm_callback=lambda _: True)
    assert loaded == ([] if stage == "load" else [True])
    assert registry.events[-2:] == ["unlock", "close"]
    assert not registry.active
    report = json.loads((tmp_path / "output" / "demanda_report.json").read_text())
    assert report["estado_ejecucion"] == "error"
    assert report["postgresql"]["status"] == ("error" if stage == "load" else "success")
    assert report["demanda_sustituciones"]["registros_al"] is None
    assert report["error"]["commit_demanda_confirmado"] == (
        None if stage == "load" else True
    )
    assert report["error"]["commit_sustituciones_confirmado"] is None


def test_manual_historical_recalculation_skips_file_and_load(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2026"])
    (tmp_path / "demanda.csv").unlink()
    registry = LockRegistry()
    prompts = []
    passwords = []
    monkeypatch.setattr(
        runner, "read_source", lambda *_: pytest.fail("unexpected file read")
    )
    monkeypatch.setattr(
        runner, "load_to_postgresql", lambda *_, **__: pytest.fail("unexpected load")
    )

    def analyze(config, *, anio, **kwargs):
        assert anio == 2025
        assert registry.active == {runner._demanda_lock_key("test")}
        assert kwargs["password"] == "secret"
        return _analyzed(anio)

    monkeypatch.setattr(runner, "generar_sustituciones", analyze)
    result = runner.recalcular_demanda_sustituciones(
        config, anio=2025, connection_factory=registry.connect,
        password_provider=lambda: passwords.append(True) or "secret",
        confirm_callback=lambda message: prompts.append(message) or True,
    )
    assert "test.demanda_sustituciones" in prompts[0]
    assert "2025" in prompts[0]
    assert passwords == [True]
    assert result.sustituciones.anio_procesado == 2025
    assert result.report_path.name == "demanda_sustituciones_recalculo_2025.json"
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["modo"] == "recalculo_manual"
    assert report["carga_archivo_ejecutada"] is False
    assert report["postgresql"]["status"] == "skipped"
    assert report["demanda_sustituciones"]["registros_al"] == 2
    assert "rows_read" not in report
    assert registry.events == ["lock", "unlock", "close"]


def test_manual_cancellation_prevents_connection(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2026"])
    monkeypatch.setattr(
        runner, "generar_sustituciones", lambda *_, **__: pytest.fail("unexpected analysis")
    )
    with pytest.raises(FlowRunError, match="cancelado"):
        runner.recalcular_demanda_sustituciones(
            config, anio=2025,
            connection_factory=lambda **_: pytest.fail("unexpected connection"),
            password_provider=lambda: pytest.fail("unexpected password prompt"),
            confirm_callback=lambda _: False,
        )
    report_path = tmp_path / "output" / "demanda_sustituciones_recalculo_2025.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["estado_ejecucion"] == "error"
    assert report["postgresql"]["status"] == "skipped"


def test_manual_failure_releases_lock_and_reports_unknown_metrics(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2026"])
    registry = LockRegistry()

    def fail_analysis(*_, **__):
        assert registry.active
        raise RuntimeError("analytical transaction rolled back")

    monkeypatch.setattr(runner, "generar_sustituciones", fail_analysis)
    with pytest.raises(RuntimeError, match="rolled back"):
        runner.recalcular_demanda_sustituciones(
            config, anio=2025, connection_factory=registry.connect,
            password_provider=lambda: "secret", confirm_callback=lambda _: True,
        )
    assert not registry.active
    report = json.loads((tmp_path / "output" /
                         "demanda_sustituciones_recalculo_2025.json").read_text())
    assert report["postgresql"]["status"] == "skipped"
    assert report["demanda_sustituciones"]["estado"] == "error"
    assert report["demanda_sustituciones"]["registros_al"] is None
    assert report["error"]["commit_sustituciones_confirmado"] is None


def test_analytical_close_error_reports_confirmed_commit(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2026"])
    registry = LockRegistry()
    monkeypatch.setattr(runner, "_current_demanda_year", lambda: 2026)
    monkeypatch.setattr(runner, "load_to_postgresql",
                        lambda config, frame, **kwargs: _loaded(config))

    def fail_after_commit(*_, **__):
        error = RuntimeError("close failed")
        error.demanda_sustituciones_commit_confirmed = True
        raise error

    monkeypatch.setattr(runner, "generar_sustituciones", fail_after_commit)
    with pytest.raises(RuntimeError, match="close failed"):
        run_flow(config, load=True, connection_factory=registry.connect,
                 password_provider=lambda: "secret", confirm_callback=lambda _: True)
    report = json.loads((tmp_path / "output" / "demanda_report.json").read_text())
    assert report["postgresql"]["status"] == "success"
    assert report["demanda_sustituciones"]["registros_al"] is None
    assert report["error"]["commit_demanda_confirmado"] is True
    assert report["error"]["commit_sustituciones_confirmado"] is True


def test_manual_and_automatic_paths_share_lock(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2026"])
    registry = LockRegistry()
    monkeypatch.setattr(
        runner, "generar_sustituciones", lambda *_, **__: pytest.fail("unexpected analysis")
    )
    with runner._demanda_advisory_lock(config, registry.connect, "secret"):
        with pytest.raises(FlowRunError, match="Ya hay una carga"):
            runner.recalcular_demanda_sustituciones(
                config, anio=2025, connection_factory=registry.connect,
                password_provider=lambda: "secret", confirm_callback=lambda _: True,
            )
    assert not registry.active
    assert all(connection.closed for connection in registry.connections)


def test_report_failure_does_not_claim_complete_pipeline(tmp_path, monkeypatch, capsys):
    config = _demanda_config(tmp_path, ["2026"])
    registry = LockRegistry()
    monkeypatch.setattr(runner, "_current_demanda_year", lambda: 2026)
    monkeypatch.setattr(runner, "load_to_postgresql",
                        lambda config, frame, **kwargs: _loaded(config))
    monkeypatch.setattr(runner, "generar_sustituciones",
                        lambda config, *, anio, **kwargs: _analyzed(anio))
    original_write = runner._write_json_report
    attempts = []

    def fail_once(path, report):
        attempts.append(True)
        if len(attempts) == 1:
            raise FlowRunError("report write failed")
        return original_write(path, report)

    monkeypatch.setattr(runner, "_write_json_report", fail_once)
    with pytest.raises(FlowRunError, match="report write failed"):
        run_flow(config, load=True, connection_factory=registry.connect,
                 password_provider=lambda: "secret", confirm_callback=lambda _: True)
    assert "Pipeline completado" not in capsys.readouterr().out
    assert not registry.active
    report = json.loads((tmp_path / "output" / "demanda_report.json").read_text())
    assert report["estado_ejecucion"] == "error"
    assert report["error"]["stage"] == "reporte"
    assert report["postgresql"]["status"] == "success"
    assert report["demanda_sustituciones"]["estado"] == "success"
    assert report["error"]["commit_sustituciones_confirmado"] is True


def test_non_successful_load_cannot_start_analysis(tmp_path, monkeypatch):
    config = _demanda_config(tmp_path, ["2026"])
    registry = LockRegistry()
    monkeypatch.setattr(runner, "_current_demanda_year", lambda: 2026)
    monkeypatch.setattr(
        runner, "load_to_postgresql",
        lambda config, frame, **kwargs: replace(_loaded(config), status="error"),
    )
    monkeypatch.setattr(
        runner, "generar_sustituciones", lambda *_, **__: pytest.fail("unexpected analysis")
    )
    with pytest.raises(FlowRunError, match="no finalizó"):
        run_flow(config, load=True, connection_factory=registry.connect,
                 password_provider=lambda: "secret", confirm_callback=lambda _: True)
    assert registry.events[-2:] == ["unlock", "close"]


def test_same_schema_lock_excludes_overlap_and_other_schema_is_independent():
    base = resolve_flow_config("demanda", "test")
    prd = resolve_flow_config("demanda", "prd")
    registry = LockRegistry()
    assert runner._demanda_lock_key("test") != runner._demanda_lock_key("prd")
    with runner._demanda_advisory_lock(base, registry.connect, "secret"):
        with pytest.raises(FlowRunError, match="Ya hay una carga"):
            with runner._demanda_advisory_lock(base, registry.connect, "secret"):
                pytest.fail("overlapping demand runs")
        with runner._demanda_advisory_lock(prd, registry.connect, "secret"):
            assert len(registry.active) == 2
    assert not registry.active
    assert all(connection.closed for connection in registry.connections)


def test_local_session_lock_is_exclusive_and_released():
    if os.environ.get("UPTOPG_RUN_LOCK_TESTS") != "1":
        pytest.skip("local PostgreSQL lock test requires explicit opt-in")
    if os.environ.get("PGHOSTADDR") or os.environ.get("PGSERVICE"):
        pytest.fail("remove PGHOSTADDR and PGSERVICE before local integration")
    def connect(**kwargs):
        assert kwargs["host"] == "/var/run/postgresql"
        assert kwargs["dbname"] == "uptopg_test"
        assert kwargs["user"] == "rom"
        return psycopg.connect(**kwargs, connect_timeout=3)
    resolved = resolve_flow_config("demanda", "test")
    data = deepcopy(resolved.data)
    data["postgresql"].update(host="/var/run/postgresql", database="uptopg_test",
                              user="rom")
    config = FlowConfig(name="demanda", env="test", data=data)
    try:
        probe = connect(host="/var/run/postgresql", port=5432,
                        dbname="uptopg_test", user="rom", password="")
    except psycopg.OperationalError as error:
        pytest.skip(f"local uptopg_test unavailable: {type(error).__name__}")
    probe.close()
    with runner._demanda_advisory_lock(config, connect, ""):
        with pytest.raises(FlowRunError, match="Ya hay una carga"):
            with runner._demanda_advisory_lock(config, connect, ""):
                pytest.fail("overlapping sessions")
    with runner._demanda_advisory_lock(config, connect, ""):
        pass
