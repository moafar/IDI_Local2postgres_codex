"""Acceptance coverage for the real act_compl XLSX input."""

from __future__ import annotations

import json
from pathlib import Path
import copy

import pytest

from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.flows import run_flow
from up_to_postgresql.readers import read_source


FLOW_NAME = "act_compl"
SOURCE_PATH = "act_compl/_EXT_ACT-COMPL_DATA-REVISAT_202601.xlsx"
SHEET_NAME = "_activitat_complementaria"
EXPECTED_COLUMNS = [
    "Any prestació (YYYY)",
    "Centre SAP codi (Hospital)",
    "Codificació genèrica codi",
    "Codificació genèrica desc",
    "Data Revisat",
    "Institució",
    "Màquina de la prestació codi",
    "Màquina de la prestació desc",
    "Mes prestació (MM)",
    "Metge Radiologia Docum. Allibera codi",
    "Metge Radiologia Docum. Allibera desc",
    "Nivell 1 codi",
    "Nivell 1 desc",
    "Prestació nivell 9 codi",
    "Prestació nivell 9 desc",
    "UP sol·licitant codi",
    "UP sol·licitant desc",
    "UP sol·licitant entitat proveïdora desc",
    "UT sol·licitant codi",
    "UT sol·licitant desc",
    "Número de prestacions",
    "Centre Codi",
    "Institució Corregida",
    "Màquina Descripció",
    "Prestació descripció",
    "UP Sol·licitant Corregida",
    "UP Sol·licitant Entitat Corregida",
]
EXPECTED_TARGETS = [
    "any_prestacio",
    "centre_sap_codi",
    "codificacio_generica_codi",
    "codificacio_generica_descripcio",
    "data_revisat",
    "institucio",
    "maquina_prestacio_codi",
    "maquina_prestacio_descripcio",
    "mes_prestacio",
    "metge_radiologia_allibera_codi",
    "metge_radiologia_allibera_descripcio",
    "nivell_1_codi",
    "nivell_1_descripcio",
    "prestacio_nivell_9_codi",
    "prestacio_nivell_9_descripcio",
    "up_solicitant_codi",
    "up_solicitant_descripcio",
    "up_solicitant_entitat_proveidora_descripcio",
    "ut_solicitant_codi",
    "ut_solicitant_descripcio",
    "numero_prestacions",
    "centre_codi",
    "institucio_corregida",
    "maquina_descripcio",
    "prestacio_descripcio",
    "up_solicitant_corregida",
    "up_solicitant_entitat_corregida",
]


def test_act_compl_config_uses_real_xlsx_and_replace_partition() -> None:
    config = resolve_flow_config(FLOW_NAME, "test")

    assert config.data["source"] == {
        "encoding": "utf-8",
        "delimiter": ",",
        "type": "xlsx",
        "path": SOURCE_PATH,
        "sheet": SHEET_NAME,
        "header_row": 2,
    }
    assert config.data["processing"] == {
        "drop_empty_rows": True,
        "drop_empty_columns": True,
        "trim_strings": "all",
    }
    assert config.data["validation"]["required_columns"] == EXPECTED_COLUMNS
    assert config.data["validation"]["missing_columns_policy"] == "error"
    assert config.data["validation"]["duplicate_policy"] == "report"
    assert "duplicate_key" not in config.data["validation"]
    assert config.data["load"]["target_table"] == "act_compl"
    assert config.data["load"]["load_mode"] == "replace_partition"
    assert config.data["load"]["partition_column"] == "data_revisat"
    assert [item["source"] for item in config.data["load"]["column_mapping"]] == (
        EXPECTED_COLUMNS
    )
    assert [item["target"] for item in config.data["load"]["column_mapping"]] == (
        EXPECTED_TARGETS
    )


def test_act_compl_reads_xlsx_with_empty_first_column(tmp_path: Path) -> None:
    pytest.importorskip("openpyxl")
    source = _write_act_compl_xlsx(tmp_path)

    frame = read_source(_config_for_source(tmp_path, source.name))

    assert frame.shape == (2, 28)
    assert frame.columns[0] == "Unnamed: 0"
    assert frame.iloc[:, 0].astype(str).str.strip().eq("").all()
    assert list(frame.columns[1:]) == EXPECTED_COLUMNS
    assert frame["Data Revisat"].min() == "2026-01-01 00:00:00"
    assert frame["Data Revisat"].max() == "2026-01-02 00:00:00"


def test_act_compl_processed_output_drops_empty_column_and_projects_mapping(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    pd = pytest.importorskip("pandas")
    source = _write_act_compl_xlsx(tmp_path)
    config = _config_for_source(tmp_path, source.name)
    config.data["paths"]["output_base_dir"] = str(tmp_path)

    result = run_flow(config)

    assert result.rows_read == 2
    assert result.columns_read == 28
    assert result.rows_written == 2
    assert result.columns_written == 27
    assert result.empty_columns_removed == ("Unnamed: 0",)
    assert result.duplicate_key == ()
    assert result.duplicate_rows == 0

    processed = pd.read_csv(result.processed_path, dtype=str, keep_default_na=False)
    assert processed.shape == (2, 27)
    assert list(processed.columns) == EXPECTED_COLUMNS
    assert "Unnamed: 0" not in processed.columns

    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["source"]["sheet"] == SHEET_NAME
    assert report["source"]["header_row"] == 2
    assert report["empty_columns_removed"] == ["Unnamed: 0"]
    assert report["postgresql"] == {"status": "skipped"}


def _config_for_source(tmp_path: Path, source_name: str) -> FlowConfig:
    resolved = resolve_flow_config(FLOW_NAME, "test")
    data = copy.deepcopy(resolved.data)
    data["paths"]["input_base_dir"] = str(tmp_path)
    data["paths"]["output_base_dir"] = str(tmp_path / "output")
    data["source"]["path"] = source_name
    return FlowConfig(name=resolved.name, env=resolved.env, data=data)


def _write_act_compl_xlsx(tmp_path: Path) -> Path:
    pd = pytest.importorskip("pandas")
    source = tmp_path / "act_compl.xlsx"
    rows = [
        ["ignored", *["ignored"] * len(EXPECTED_COLUMNS)],
        ["", *EXPECTED_COLUMNS],
        [
            "",
            "2026",
            "GT",
            "CG1",
            "Genèrica A",
            "2026-01-01 00:00:00",
            "ICS",
            "MQ1",
            "Sala 1",
            "01",
            "40905",
            "Metge A",
            "06",
            "Ecografia",
            "RA01172",
            "Ecografia de tiroide",
            "01083",
            "EAP A",
            "Institut Català de la Salut",
            "GTAP1083",
            "UT A",
            "1",
            "GT",
            "ICS",
            "Sala 1",
            "Ecografia",
            "EAP A",
            "Institut Català de la Salut",
        ],
        [
            "",
            "2026",
            "GT",
            "CG2",
            "Genèrica B",
            "2026-01-02 00:00:00",
            "ICS",
            "MQ2",
            "Sala 2",
            "01",
            "40906",
            "Metge B",
            "01",
            "Radiologia Convencional",
            "RA00063",
            "Rx d'espatlla",
            "00278",
            "EAP B",
            "Institut Català de la Salut",
            "GTAP9047",
            "UT B",
            "1",
            "GT",
            "ICS",
            "Sala 2",
            "Radiologia Convencional",
            "EAP B",
            "Institut Català de la Salut",
        ],
    ]
    frame = pd.DataFrame(rows)
    with pd.ExcelWriter(source) as writer:
        frame.to_excel(writer, sheet_name=SHEET_NAME, header=False, index=False)
    return source
