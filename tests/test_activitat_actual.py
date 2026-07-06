"""Acceptance coverage for the real activitat_actual CSV input."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.flows import run_flow
from up_to_postgresql.readers import read_source


FLOW_NAME = "activitat_actual"
SOURCE_FILE = Path("data/input/test/activitat_actual_2006-ene-may__.csv")
EXPECTED_COLUMNS = [
    "Any prestació (YYYY)",
    "Centre SAP codi (Hospital)",
    "Centre SAP desc (Hospital)",
    "Data prestació",
    "Dia setmana prestació (desc)",
    "Hora prestació",
    "Institució",
    "Interlocutor/Metge sol·licitant codi (ordre)",
    "Interlocutor/Metge sol·licitant desc (ordre)",
    "Màquina de la prestació codi",
    "Màquina de la prestació desc",
    "Mes prestació (MM)",
    "Metge Radiologia Docum. Allibera codi",
    "Metge Radiologia Docum. Allibera desc",
    "Nivell 1 codi",
    "Nivell 1 desc",
    "Pacient (NHC)",
    "Prestació nivell 9 codi",
    "Prestació nivell 9 desc",
    "Servei sol·licitant codi (ordre)",
    "Servei sol·licitant desc (ordre)",
    "Tipus episodi codi",
    "UP sol·licitant codi",
    "UP sol·licitant desc",
    "UP sol·licitant entitat proveïdora codi",
    "UP sol·licitant entitat proveïdora desc",
    "UP sol·licitant tipus",
    "UT Gestora codi",
    "UT Gestora desc",
    "UT sol·licitant codi",
    "UT sol·licitant desc",
    "Número de prestacions",
    "Temps entre ordre clínica i data de prestació realitzada (dies)",
    "Temps entre ordre clínica i data de Realitazacio Informe (dies)",
    "Temps entre ordre clínica i data informe (dies)",
    "Temps entre prestació realitzada i data d'informe (dies)",
    "v_Centre Codi",
    "v_Direcció Clínica",
    "v_Institució corregida",
    "v_Màquina descripció",
    "v_Prestació descripció",
    "v_Resum Sol·licitant",
    "v_Torn",
    "v_UP Sol·licitant Corregida",
    "v_UP Sol·licitant Entitat Corregida",
    "v_UT Gestora corregida",
]


def test_activitat_actual_config_points_to_real_csv_and_outputs() -> None:
    config = resolve_flow_config(FLOW_NAME, "test")

    assert config.data["paths"]["input_base_dir"] == "data/input/test"
    assert config.data["source"] == {
        "encoding": "utf-8",
        "delimiter": ";",
        "type": "csv",
        "path": "activitat_actual_2006-ene-may__.csv",
        "header_row": 1,
    }
    assert config.data["processing"] == {
        "drop_empty_rows": True,
        "drop_empty_columns": True,
        "trim_strings": "all",
    }
    assert config.data["validation"]["required_columns"] == EXPECTED_COLUMNS
    assert "duplicate_key" not in config.data["validation"]
    assert config.data["validation"]["missing_columns_policy"] == "error"
    assert config.data["validation"]["duplicate_policy"] == "report"
    assert config.data["output"] == {
        "processed_filename": "activitat_actual_processed.csv",
        "report_filename": "activitat_actual_report.json",
    }


def test_activitat_actual_reads_real_csv_as_text() -> None:
    pytest.importorskip("pandas")
    assert SOURCE_FILE.exists(), f"Missing real fixture: {SOURCE_FILE}"

    frame = read_source(resolve_flow_config(FLOW_NAME, "test"))

    assert frame.shape == (423933, 46)
    assert list(frame.columns) == EXPECTED_COLUMNS
    assert frame.iloc[0].to_dict() == {
        "Any prestació (YYYY)": "2026",
        "Centre SAP codi (Hospital)": "AR",
        "Centre SAP desc (Hospital)": "Hospital Arnau de Vilanova de Lleida",
        "Data prestació": "01/01/26",
        "Dia setmana prestació (desc)": "DIJOUS   ",
        "Hora prestació": "014150",
        "Institució": "IDI",
        "Interlocutor/Metge sol·licitant codi (ordre)": "66473",
        "Interlocutor/Metge sol·licitant desc (ordre)": "BARCO LOPEZ,ANTONIETA",
        "Màquina de la prestació codi": "MQ8B0S05",
        "Màquina de la prestació desc": "IDI - RM",
        "Mes prestació (MM)": "01",
        "Metge Radiologia Docum. Allibera codi": "36147",
        "Metge Radiologia Docum. Allibera desc": "DIEZ GARCIA,JAVIER",
        "Nivell 1 codi": "08",
        "Nivell 1 desc": "RM",
        "Pacient (NHC)": "19162940",
        "Prestació nivell 9 codi": "RA00335",
        "Prestació nivell 9 desc": "RM de genoll sense contrast",
        "Servei sol·licitant codi (ordre)": "MFCSVLL",
        "Servei sol·licitant desc (ordre)": "MEDICINA FAMILIAR I COMUNITARI",
        "Tipus episodi codi": "CX",
        "UP sol·licitant codi": "00029",
        "UP sol·licitant desc": "EAP Lleida 5 - Cappont",
        "UP sol·licitant entitat proveïdora codi": "0208",
        "UP sol·licitant entitat proveïdora desc": "Institut Català de la Salut",
        "UP sol·licitant tipus": "EAP",
        "UT Gestora codi": "ARUC7102",
        "UT Gestora desc": "IDI Diagnostic per la imatge",
        "UT sol·licitant codi": "LLAG3139",
        "UT sol·licitant desc": "ABS CAPPONT",
        "Número de prestacions": "1",
        "Temps entre ordre clínica i data de prestació realitzada (dies)": "12,52",
        "Temps entre ordre clínica i data de Realitazacio Informe (dies)": "13,44",
        "Temps entre ordre clínica i data informe (dies)": "14,13",
        "Temps entre prestació realitzada i data d'informe (dies)": "1,62",
        "v_Centre Codi": "AR",
        "v_Direcció Clínica": "IDI Lleida",
        "v_Institució corregida": "IDI",
        "v_Màquina descripció": "IDI - RM",
        "v_Prestació descripció": "RM",
        "v_Resum Sol·licitant": "ICS - Atenció Primària",
        "v_Torn": "Nit",
        "v_UP Sol·licitant Corregida": "EAP Lleida 5 - Cappont",
        "v_UP Sol·licitant Entitat Corregida": "Institut Català de la Salut",
        "v_UT Gestora corregida": "IDI Lleida",
    }
    assert frame["Any prestació (YYYY)"].unique().tolist() == ["2026"]
    assert sorted(frame["Mes prestació (MM)"].unique().tolist()) == [
        "01",
        "02",
        "03",
        "04",
        "05",
    ]


def test_activitat_actual_real_input_has_no_blank_rows_columns_or_duplicates() -> None:
    pytest.importorskip("pandas")

    frame = read_source(resolve_flow_config(FLOW_NAME, "test"))
    stripped = frame.map(lambda value: str(value).strip())

    assert not stripped.eq("").all(axis=1).any()
    assert not stripped.eq("").all(axis=0).any()
    assert not stripped.duplicated().any()


def test_activitat_actual_processed_output_and_report_contract(tmp_path: Path) -> None:
    config = resolve_flow_config(FLOW_NAME, "test")
    config.data["paths"]["output_base_dir"] = str(tmp_path)

    result = run_flow(config)

    processed_path = tmp_path / "activitat_actual_processed.csv"
    report_path = tmp_path / "activitat_actual_report.json"
    assert result.processed_path == processed_path
    assert result.report_path == report_path
    assert processed_path.exists()
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["rows_read"] == 423933
    assert report["rows_written"] == 423933
    assert report["columns_written"] == 46
    assert report["empty_rows_removed"] == 0
    assert report["empty_columns_removed"] == []
    assert report["duplicate_key"] == []
    assert report["duplicate_rows"] == 0
    assert report["postgresql"] == {"status": "skipped"}
