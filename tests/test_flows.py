# tests/test_flows.py
"""Tests for configured flow execution."""

from pathlib import Path
import json

import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.flows.runner import FlowRunError, run_flow


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
