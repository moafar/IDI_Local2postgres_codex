"""Acceptance coverage for the remaining real PostgreSQL-bound flows."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.flows import run_flow
from up_to_postgresql.readers import read_source


FLOW_CASES = {
    "llee_trams_demora": {
        "source": "llee_mensual.xlsx",
        "sheet": "Trams demora per Centre i Prova",
        "shape": (473, 15),
        "target_table": "llee_trams_demora",
        "columns": [
            "any",
            "mes",
            "Centre",
            "Prioritat",
            "Grup de monitorització",
            "Total pacients",
            "porc_oportunidad",
            "0-30 d",
            "31-60 d",
            "61-90 d",
            "91-120 d",
            "121-150 d",
            "151-180 d",
            "181-365 d",
            "Mes d'1 any",
        ],
        "duplicate_key": [
            "any",
            "mes",
            "Centre",
            "Prioritat",
            "Grup de monitorització",
        ],
        "duplicate_rows": 0,
        "targets": [
            "any_llee",
            "mes_llee",
            "centre",
            "prioritat",
            "grup_monitoritzacio",
            "total_pacients",
            "percentatge_oportunitat",
            "tram_0_30_dies",
            "tram_31_60_dies",
            "tram_61_90_dies",
            "tram_91_120_dies",
            "tram_121_150_dies",
            "tram_151_180_dies",
            "tram_181_365_dies",
            "mes_1_any",
        ],
    },
    "td_ambulatoris": {
        "source": "td.xlsx",
        "sheet": "td_ambulatoris",
        "shape": (12996, 17),
        "target_table": "td_ambulatoris",
        "columns": [
            "any",
            "mes",
            "centre_realitzacio",
            "sala_realitzacio",
            "data_realitzacio",
            "dies_previstos",
            "data_conveni",
            "prestacio_modalitat",
            "pte_num",
            "prestacio_denominacio",
            "especialista",
            "situacio",
            "data_resultats",
            "pte_cip",
            "data_assignacio",
            "dias_trigats",
            "dias_retard",
        ],
        "duplicate_key": [],
        "duplicate_rows": 492,
        "targets": [
            "any_ambulatoris",
            "mes_ambulatoris",
            "centre_realitzacio",
            "sala_realitzacio",
            "data_realitzacio",
            "dies_previstos",
            "data_conveni",
            "prestacio_modalitat",
            "pacient_numero",
            "prestacio_denominacio",
            "especialista",
            "situacio",
            "data_resultats",
            "pacient_cip",
            "data_assignacio",
            "dies_trigats",
            "dies_retard",
        ],
    },
    "td_urgencies": {
        "source": "td.xlsx",
        "sheet": "td_urgencies",
        "shape": (588, 8),
        "target_table": "td_urgencies",
        "columns": [
            "solicitant",
            "any",
            "mes",
            "episodi",
            "prestació",
            "proves",
            "lat",
            "lon",
        ],
        "duplicate_key": ["solicitant", "any", "mes", "episodi", "prestació"],
        "duplicate_rows": 0,
        "targets": [
            "solicitant",
            "any_urgencies",
            "mes_urgencies",
            "episodi",
            "prestacio",
            "proves",
            "latitud",
            "longitud",
        ],
    },
    "demanda": {
        "source": "demanda._samplexlsx.xlsx",
        "sheet": "Demanda",
        "shape": (9, 23),
        "target_table": "demanda",
        "columns": [
            "Any prestació (YYYY)",
            "Mes prestació (MM)",
            "Data prestació",
            "Número de prestacions",
            "Centre SAP codi (Hospital)",
            "Centre Codi",
            "Institució",
            "Institució corregida",
            "Màquina de la prestació codi",
            "Màquina de la prestació desc",
            "Màquina Descripció",
            "Prestació descripció",
            "Prestació nivell 9 codi",
            "Prestació nivell 9 desc",
            "Nivell 1 codi",
            "Nivell 1 desc",
            "UP sol·licitant desc",
            "UP sol·licitant entitat proveïdora desc",
            "UP Sol·licitant Corregida",
            "UP Sol·licitant Entitat Corregida",
            "UT sol·licitant desc",
            "Prestació estat codi",
            "Pacient (NHC)",
        ],
        "duplicate_key": [
            "Data prestació",
            "Prestació nivell 9 codi",
            "Pacient (NHC)",
        ],
        "duplicate_rows": 0,
        "targets": [
            "any_prestacio",
            "mes_prestacio",
            "data_prestacio",
            "numero_prestacions",
            "centre_sap_codi",
            "centre_codi",
            "institucio",
            "institucio_corregida",
            "maquina_prestacio_codi",
            "maquina_prestacio_descripcio",
            "maquina_descripcio",
            "prestacio_descripcio",
            "prestacio_nivell_9_codi",
            "prestacio_nivell_9_descripcio",
            "nivell_1_codi",
            "nivell_1_descripcio",
            "up_solicitant_descripcio",
            "up_solicitant_entitat_proveidora_descripcio",
            "up_solicitant_corregida",
            "up_solicitant_entitat_corregida",
            "ut_solicitant_descripcio",
            "prestacio_estat_codi",
            "pacient_nhc",
        ],
    },
}


@pytest.mark.parametrize("flow_name", FLOW_CASES)
def test_remaining_real_flow_config_contract(flow_name: str) -> None:
    case = FLOW_CASES[flow_name]
    config = resolve_flow_config(flow_name, "test")

    assert config.data["source"] == {
        "encoding": "utf-8",
        "delimiter": ",",
        "type": "xlsx",
        "path": case["source"],
        "sheet": case["sheet"],
        "header_row": 1,
    }
    assert config.data["processing"] == {
        "drop_empty_rows": True,
        "drop_empty_columns": True,
        "trim_strings": "all",
    }
    assert config.data["validation"]["required_columns"] == case["columns"]
    assert config.data["validation"].get("duplicate_key", []) == case["duplicate_key"]
    assert config.data["validation"]["duplicate_policy"] == "report"
    assert config.data["validation"]["missing_columns_policy"] == "error"
    assert config.data["load"]["target_table"] == case["target_table"]
    assert config.data["load"]["load_mode"] == "fail"
    assert [item["source"] for item in config.data["load"]["column_mapping"]] == case[
        "columns"
    ]
    assert [item["target"] for item in config.data["load"]["column_mapping"]] == case[
        "targets"
    ]


@pytest.mark.parametrize("flow_name", FLOW_CASES)
def test_remaining_real_flow_reads_expected_xlsx_columns(flow_name: str) -> None:
    pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    case = FLOW_CASES[flow_name]
    source = Path("data/input/test") / str(case["source"])
    assert source.exists(), f"Missing real fixture: {source}"

    frame = read_source(resolve_flow_config(flow_name, "test"))

    assert frame.shape == case["shape"]
    assert list(frame.columns) == case["columns"]


@pytest.mark.parametrize("flow_name", FLOW_CASES)
def test_remaining_real_flow_writes_outputs_without_loading(
    flow_name: str, tmp_path: Path
) -> None:
    pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    case = FLOW_CASES[flow_name]
    config = resolve_flow_config(flow_name, "test")
    config.data["paths"]["output_base_dir"] = str(tmp_path)

    result = run_flow(config)

    assert result.rows_read == case["shape"][0]
    assert result.columns_read == case["shape"][1]
    assert result.rows_written == case["shape"][0]
    assert result.columns_written == case["shape"][1]
    assert result.empty_rows_removed == 0
    assert result.empty_columns_removed == ()
    assert result.duplicate_rows == case["duplicate_rows"]
    assert result.processed_path.exists()
    assert result.report_path.exists()

    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["duplicate_key"] == case["duplicate_key"]
    assert report["duplicate_rows"] == case["duplicate_rows"]
    assert report["postgresql"] == {"status": "skipped"}
