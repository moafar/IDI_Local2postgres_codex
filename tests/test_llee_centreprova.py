# tests/test_llee_centreprova.py
"""Acceptance coverage for the real llee_centreprova input."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.flows import run_flow
from up_to_postgresql.readers import read_source


FLOW_NAME = "llee_centreprova"
SHEET_NAME = "LLEE per Centre i Prova"
SOURCE_FILE = Path("data/input/test/llee_mensual.xlsx")
EXPECTED_COLUMNS = [
    "any",
    "mes",
    "Centre",
    "Proves",
    "Prestacions",
    "Pendents de programar",
    "% pendents programar",
    "Mes de 90 dies",
    "% mes 90 dies",
    "Entrades",
    "Sortides",
    "Diferència Entrades-Sortides",
    "Temps de demora (dies)",
    "Temps d'espera (dies)",
    "TD/TE (%)",
]
DUPLICATE_KEY = ["any", "mes", "Centre", "Proves"]


def test_llee_centreprova_config_points_to_real_sheet_and_outputs() -> None:
    config = resolve_flow_config(FLOW_NAME, "test")

    assert config.data["paths"]["input_base_dir"] == "data/input/test"
    assert config.data["source"] == {
        "encoding": "utf-8",
        "delimiter": ",",
        "type": "xlsx",
        "path": "llee_mensual.xlsx",
        "sheet": SHEET_NAME,
        "header_row": "1",
    }
    assert config.data["processing"] == {
        "drop_empty_rows": True,
        "drop_empty_columns": True,
        "detect_duplicates": True,
        "duplicate_key": DUPLICATE_KEY,
    }
    assert config.data["output"] == {
        "processed_path": "llee_centreprova_processed.csv",
        "report_path": "llee_centreprova_report.json",
    }


def test_llee_centreprova_reads_real_xlsx_as_text() -> None:
    pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    assert SOURCE_FILE.exists(), f"Missing real fixture: {SOURCE_FILE}"

    frame = read_source(resolve_flow_config(FLOW_NAME, "test"))

    assert frame.shape == (252, 15)
    assert list(frame.columns) == EXPECTED_COLUMNS
    assert frame.iloc[0].to_dict() == {
        "any": "2026",
        "mes": "1",
        "Centre": "IDI Metropolitana Nord",
        "Proves": "19-RM neurològica (Crani-columna vertebral)",
        "Prestacions": "832",
        "Pendents de programar": "490",
        "% pendents programar": "0.588942307692308",
        "Mes de 90 dies": "21",
        "% mes 90 dies": "0.0252403846153846",
        "Entrades": "609",
        "Sortides": "196",
        "Diferència Entrades-Sortides": "413",
        "Temps de demora (dies)": "32.6334134615385",
        "Temps d'espera (dies)": "64.7345679012346",
        "TD/TE (%)": "0.504111088087082",
    }
    assert frame["any"].unique().tolist() == ["2026"]
    assert sorted(frame["mes"].unique().tolist(), key=int) == ["1", "2", "3", "4"]
    assert "IDI Terres de l'Ebre" in set(frame["Centre"])
    assert "22-Gammagrafia òssia (osteo-muscular)" in set(frame["Proves"])


def test_llee_centreprova_real_input_has_no_blank_rows_columns_or_duplicates() -> None:
    pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")

    frame = read_source(resolve_flow_config(FLOW_NAME, "test"))
    stripped = frame.map(lambda value: str(value).strip())

    assert not stripped.eq("").all(axis=1).any()
    assert not stripped.eq("").all(axis=0).any()
    assert not frame.duplicated(subset=DUPLICATE_KEY).any()
    assert not frame.duplicated().any()


def test_llee_centreprova_processed_output_and_report_contract(tmp_path: Path) -> None:
    config = resolve_flow_config(FLOW_NAME, "test")
    config.data["paths"]["output_base_dir"] = str(tmp_path)

    result = run_flow(config)

    processed_path = tmp_path / "llee_centreprova_processed.csv"
    report_path = tmp_path / "llee_centreprova_report.json"
    assert result.processed_path == processed_path
    assert result.report_path == report_path
    assert processed_path.exists()
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["rows_written"] == 252
    assert report["duplicate_key"] == DUPLICATE_KEY
    assert report["duplicate_rows"] == 0
    assert report["postgresql"] == "not_implemented"
