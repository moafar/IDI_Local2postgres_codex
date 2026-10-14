# tests/test_cli.py
"""Test normal CLI routing and explicit demanda recalculation arguments.

Depends on pytest and injected runner functions. No test here opens database
connections or reads demand files; incompatible options must fail before work.
"""

from pathlib import Path

import pytest

from up_to_postgresql.cli import main
from up_to_postgresql.flows.demanda_sustituciones import DemandaSustitucionesResult
from up_to_postgresql.flows.runner import DemandaRecalculationResult, FlowRunError


def test_cli_accepts_flow_and_env(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--flow", "llee_centreprova", "--env", "test"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "flow=llee_centreprova" in captured.out
    assert "env=test" in captured.out


def test_cli_accepts_prd_env(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--flow", "demanda", "--env", "prd"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "flow=demanda" in captured.out
    assert "env=prd" in captured.out


def test_cli_rejects_invalid_env() -> None:
    with pytest.raises(SystemExit) as error:
        main(["--flow", "llee_centreprova", "--env", "dev"])

    assert error.value.code == 2


def test_cli_rejects_missing_required_arguments() -> None:
    with pytest.raises(SystemExit) as error:
        main([])

    assert error.value.code == 2


def test_cli_shows_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        main(["--help"])

    captured = capsys.readouterr()

    assert error.value.code == 0
    assert "Run a configured data flow." in captured.out
    assert "--flow" in captured.out
    assert "--env" in captured.out


def test_cli_execute_without_load_does_not_enable_postgresql(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls = []

    def fake_run_flow(config: object, *, load: bool = False) -> object:
        calls.append(load)
        return None

    monkeypatch.setattr("up_to_postgresql.cli.run_flow", fake_run_flow)

    exit_code = main(["--flow", "llee_centreprova", "--env", "test", "--execute"])

    assert exit_code == 0
    assert calls == [False]
    assert "Type LOAD" not in capsys.readouterr().out


def test_cli_source_overrides_config_only_for_run(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_run_flow(config: object, *, load: bool = False) -> None:
        captured["source"] = config.data["source"]
        captured["load"] = load

    monkeypatch.setattr("up_to_postgresql.cli.run_flow", fake_run_flow)

    exit_code = main(
        [
            "--flow",
            "llee_centreprova",
            "--env",
            "test",
            "--execute",
            "--source",
            "overrides/llee_mensual.xlsx",
        ]
    )

    assert exit_code == 0
    assert captured["source"]["path"] == "overrides/llee_mensual.xlsx"
    assert captured["load"] is False


def test_cli_rejects_load_without_execute() -> None:
    with pytest.raises(SystemExit) as error:
        main(["--flow", "llee_centreprova", "--env", "test", "--load"])

    assert error.value.code == 2


def test_cli_routes_manual_recalculation_without_running_flow(monkeypatch, capsys):
    calls = []

    def fake_recalculate(config, *, anio):
        calls.append((config.name, config.env, anio))
        return DemandaRecalculationResult(
            sustituciones=DemandaSustitucionesResult(
                anio_procesado=anio, version_algoritmo="1.0.0", registros_al=1,
                candidatas_unicas=0, candidatas_multiples=0, sin_candidata=1,
                registros_agregados=0, relaciones_generadas=0,
                duracion_segundos=0.1, estado="success",
            ),
            report_path=Path("manual_2025.json"),
        )

    monkeypatch.setattr("up_to_postgresql.cli.recalcular_demanda_sustituciones",
                        fake_recalculate)
    monkeypatch.setattr("up_to_postgresql.cli.run_flow",
                        lambda *_, **__: pytest.fail("unexpected file flow"))
    assert main(["--flow", "demanda", "--env", "test", "--execute",
                 "--recalcular-sustituciones", "--anio", "2025"]) == 0
    assert calls == [("demanda", "test", 2025)]
    assert "report_path=manual_2025.json" in capsys.readouterr().out


@pytest.mark.parametrize("extra", [
    ["--recalcular-sustituciones"],
    ["--execute", "--recalcular-sustituciones"],
    ["--execute", "--recalcular-sustituciones", "--anio", "2026", "--load"],
    ["--execute", "--recalcular-sustituciones", "--anio", "2026",
     "--source", "other.csv"],
    ["--execute", "--anio", "2026"],
])
def test_cli_rejects_incompatible_manual_options(extra):
    with pytest.raises(SystemExit) as caught:
        main(["--flow", "demanda", "--env", "test", *extra])
    assert caught.value.code != 0


def test_cli_rejects_manual_mode_for_other_flows():
    with pytest.raises(SystemExit) as caught:
        main(["--flow", "llee_centreprova", "--env", "test", "--execute",
              "--recalcular-sustituciones", "--anio", "2026"])
    assert caught.value.code != 0


def test_cli_manual_failure_has_nonzero_exit(monkeypatch, capsys):
    def fail(*_, **__):
        error = FlowRunError("falló el recálculo")
        error.add_note("Etapa de recálculo: analisis")
        raise error

    monkeypatch.setattr("up_to_postgresql.cli.recalcular_demanda_sustituciones", fail)
    with pytest.raises(SystemExit) as caught:
        main(["--flow", "demanda", "--env", "test", "--execute",
              "--recalcular-sustituciones", "--anio", "2026"])
    assert caught.value.code != 0
    assert "Etapa de recálculo: analisis" in capsys.readouterr().err


def test_cli_automatic_analysis_failure_has_nonzero_exit(monkeypatch, capsys):
    def fail(*_, **__):
        error = FlowRunError("falló el análisis")
        error.add_note("Etapa de demanda: analisis")
        raise error

    monkeypatch.setattr("up_to_postgresql.cli.run_flow", fail)
    with pytest.raises(SystemExit) as caught:
        main(["--flow", "demanda", "--env", "test", "--execute", "--load"])
    assert caught.value.code != 0
    assert "Etapa de demanda: analisis" in capsys.readouterr().err
