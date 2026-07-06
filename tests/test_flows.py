# tests/test_flows.py
"""Tests for configured flow execution."""

from pathlib import Path
import json

import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.flows.runner import FlowRunError, run_flow
from up_to_postgresql.source import SourcePathError, with_source_path


def test_run_flow_cleans_trims_validates_and_reports(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "input.csv"
    source.write_text(
        "id;name;empty\n001; Ana ;\n ; ;\n002;Bob;\n",
        encoding="utf-8",
    )
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_dir": str(tmp_path), "output_dir": str(tmp_path)},
            "source": {"type": "csv", "path": "input.csv", "delimiter": ";"},
            "processing": {
                "drop_empty_rows": True,
                "drop_empty_columns": True,
                "trim_strings": "all",
            },
            "validation": {
                "required_columns": ["id", "name"],
                "duplicate_key": ["id"],
                "duplicate_policy": "report",
            },
        },
    )

    result = run_flow(config)

    assert result.rows_read == 3
    assert result.empty_rows_removed == 1
    assert result.empty_columns_removed == ("empty",)
    assert result.rows_written == 2
    assert result.duplicate_rows == 0
    assert result.processed_path.read_text(encoding="utf-8").splitlines() == [
        "id,name",
        "001,Ana",
        "002,Bob",
    ]
    assert '"trim_strings"' in result.report_path.read_text(encoding="utf-8")


def test_run_flow_rejects_missing_required_columns(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "input.csv"
    source.write_text("id\n001\n", encoding="utf-8")
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_dir": str(tmp_path), "output_dir": str(tmp_path)},
            "source": {"type": "csv", "path": "input.csv"},
            "validation": {
                "required_columns": ["id", "name"],
                "missing_columns_policy": "error",
            },
        },
    )

    with pytest.raises(FlowRunError, match="Required columns"):
        run_flow(config)


def test_run_flow_requires_source_for_execution_when_config_has_no_path(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pandas")
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_base_dir": str(tmp_path), "output_base_dir": str(tmp_path)},
            "source": {"type": "csv"},
        },
    )

    with pytest.raises(SourcePathError, match="pass --source"):
        run_flow(config)


def test_run_flow_uses_source_override_and_reports_real_file(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "actual.csv"
    source.write_text("id,name\n1,Ana\n", encoding="utf-8")
    config = with_source_path(
        FlowConfig(
            name="sample",
            env="test",
            data={
                "paths": {
                    "input_base_dir": str(tmp_path),
                    "output_base_dir": str(tmp_path / "output"),
                },
                "source": {"type": "csv"},
            },
        ),
        "actual.csv",
    )

    result = run_flow(config)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))

    assert result.rows_written == 1
    assert report["source"]["path"] == str(source.resolve())


@pytest.mark.parametrize(
    ("raw_path", "message"),
    [
        ("/tmp/input.csv", "relative"),
        ("../input.csv", r"\.\."),
        ("missing.csv", "Source file not found"),
    ],
)
def test_run_flow_rejects_invalid_source_override(
    tmp_path: Path, raw_path: str, message: str
) -> None:
    pytest.importorskip("pandas")
    config = with_source_path(
        FlowConfig(
            name="sample",
            env="test",
            data={
                "paths": {"input_base_dir": str(tmp_path), "output_base_dir": str(tmp_path)},
                "source": {"type": "csv"},
            },
        ),
        raw_path,
    )

    with pytest.raises((SourcePathError, FileNotFoundError), match=message):
        run_flow(config)


def test_source_override_does_not_change_load_contract(tmp_path: Path) -> None:
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_base_dir": str(tmp_path), "output_base_dir": str(tmp_path)},
            "source": {"type": "csv"},
            "load": {
                "target_table": "items",
                "load_mode": "append",
                "column_mapping": [{"source": "id", "target": "id"}],
            },
        },
    )

    overridden = with_source_path(config, "actual.csv")

    assert overridden.data["source"]["path"] == "actual.csv"
    assert overridden.data["load"] == config.data["load"]
    assert "path" not in config.data["source"]


def test_run_flow_warns_for_missing_columns_policy_warning(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "input.csv"
    source.write_text("id\n001\n", encoding="utf-8")
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_dir": str(tmp_path), "output_dir": str(tmp_path)},
            "source": {"type": "csv", "path": "input.csv"},
            "validation": {
                "required_columns": ["id", "name"],
                "missing_columns_policy": "warning",
            },
        },
    )

    result = run_flow(config)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))

    assert result.missing_required_columns == ("name",)
    assert report["warnings"] == ["Required columns not found: ['name']"]


def test_run_flow_reports_missing_columns_policy_report(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "input.csv"
    source.write_text("id\n001\n", encoding="utf-8")
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_dir": str(tmp_path), "output_dir": str(tmp_path)},
            "source": {"type": "csv", "path": "input.csv"},
            "validation": {
                "required_columns": ["id", "name"],
                "missing_columns_policy": "report",
            },
        },
    )

    result = run_flow(config)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))

    assert result.missing_required_columns == ("name",)
    assert report["missing_required_columns"] == ["name"]
    assert report["warnings"] == []


def test_run_flow_rejects_duplicates_policy_error(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "input.csv"
    source.write_text("id\n001\n001\n", encoding="utf-8")
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_dir": str(tmp_path), "output_dir": str(tmp_path)},
            "source": {"type": "csv", "path": "input.csv"},
            "validation": {
                "duplicate_key": ["id"],
                "duplicate_policy": "error",
            },
        },
    )

    with pytest.raises(FlowRunError, match="Duplicate rows"):
        run_flow(config)


def test_run_flow_warns_for_duplicates_policy_warning(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "input.csv"
    source.write_text("id\n001\n001\n", encoding="utf-8")
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_dir": str(tmp_path), "output_dir": str(tmp_path)},
            "source": {"type": "csv", "path": "input.csv"},
            "validation": {
                "duplicate_key": ["id"],
                "duplicate_policy": "warning",
            },
        },
    )

    result = run_flow(config)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))

    assert result.duplicate_rows == 2
    assert report["warnings"] == ["Duplicate rows found: 2"]


def test_run_flow_reports_full_row_duplicates_without_duplicate_key(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "input.csv"
    source.write_text("id;name\n001;Ana\n001;Ana\n001;Bob\n", encoding="utf-8")
    config = FlowConfig(
        name="sample",
        env="test",
        data={
            "paths": {"input_dir": str(tmp_path), "output_dir": str(tmp_path)},
            "source": {"type": "csv", "path": "input.csv", "delimiter": ";"},
            "validation": {"duplicate_policy": "report"},
        },
    )

    result = run_flow(config)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))

    assert result.duplicate_key == ()
    assert result.duplicate_rows == 2
    assert report["duplicate_key"] == []
    assert report["duplicate_rows"] == 2


def test_llee_centreprova_reads_real_xlsx_and_writes_outputs(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    resolved = resolve_flow_config("llee_centreprova", "test")
    data = dict(resolved.data)
    data["paths"] = dict(data["paths"])
    data["paths"]["output_dir"] = str(tmp_path)
    config = FlowConfig(name=resolved.name, env=resolved.env, data=data)

    result = run_flow(config)

    assert result.rows_read == 252
    assert result.columns_read == 15
    assert result.rows_written == 252
    assert result.columns_written == 15
    assert result.duplicate_rows == 0
    assert result.processed_path.exists()
    assert result.report_path.exists()
