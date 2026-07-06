# tests/test_llee_flow.py
"""Tests for the real LLEE centre/prova flow."""

from pathlib import Path
import json

import pandas as pd

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.flows import run_flow


def test_llee_centreprova_reads_real_file_and_writes_outputs(tmp_path: Path) -> None:
    config = resolve_flow_config("llee_centreprova", "test")
    config.data["paths"]["output_base_dir"] = str(tmp_path)

    result = run_flow(config)

    assert result.rows_read == 252
    assert result.columns_read == 15
    assert result.rows_written == 252
    assert result.columns_written == 15
    assert result.duplicate_rows == 0
    assert result.processed_path == tmp_path / "llee_centreprova_processed.csv"
    assert result.report_path == tmp_path / "llee_centreprova_report.json"

    processed = pd.read_csv(result.processed_path, dtype=str, keep_default_na=False)
    assert processed.shape == (252, 15)
    assert list(processed.columns[:4]) == ["any", "mes", "Centre", "Proves"]
    assert processed.iloc[0]["any"] == "2026"
    assert processed.iloc[0]["mes"] == "1"

    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["source"]["sheet"] == "LLEE per Centre i Prova"
    assert report["source"]["header_row"] == 1
    assert report["duplicate_key"] == ["any", "mes", "Centre", "Proves"]
    assert report["duplicate_rows"] == 0
    assert report["postgresql"] == {"status": "skipped"}


def test_flow_drops_empty_rows_and_columns_and_detects_duplicates(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.xlsx"
    frame = pd.DataFrame(
        [
            ["any", "mes", "Centre", "Proves", "empty_column"],
            ["2026", "1", "A", "P1", ""],
            ["2026", "1", "A", "P1", ""],
            ["", "", "", "", ""],
            ["2026", "1", "B", "P2", ""],
        ]
    )
    with pd.ExcelWriter(source) as writer:
        frame.to_excel(
            writer,
            sheet_name="LLEE per Centre i Prova",
            header=False,
            index=False,
        )

    config = FlowConfig(
        name="llee_centreprova",
        env="test",
        data={
            "paths": {
                "input_base_dir": str(tmp_path),
                "output_base_dir": str(tmp_path / "output"),
            },
            "source": {
                "type": "xlsx",
                "path": "input.xlsx",
                "sheet": "LLEE per Centre i Prova",
                "header_row": 1,
            },
            "processing": {
                "drop_empty_rows": True,
                "drop_empty_columns": True,
            },
            "validation": {
                "duplicate_key": ["any", "mes", "Centre", "Proves"],
                "duplicate_policy": "report",
            },
        },
    )

    result = run_flow(config)
    processed = pd.read_csv(result.processed_path, dtype=str, keep_default_na=False)

    assert result.rows_read == 4
    assert result.rows_written == 3
    assert result.columns_written == 4
    assert result.duplicate_rows == 2
    assert "empty_column" not in processed.columns
