# tests/test_cli.py
"""Tests for the command line interface."""

import pytest

from up_to_postgresql.cli import main


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
