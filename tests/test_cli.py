import pytest

from up_to_postgresql.cli import main


def test_cli_accepts_flow_and_env(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--flow", "clientes", "--env", "test"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "flow=clientes" in captured.out
    assert "env=test" in captured.out


def test_cli_accepts_prd_env(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--flow", "ventas", "--env", "prd"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "flow=ventas" in captured.out
    assert "env=prd" in captured.out


def test_cli_rejects_invalid_env() -> None:
    with pytest.raises(SystemExit) as error:
        main(["--flow", "clientes", "--env", "dev"])

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

