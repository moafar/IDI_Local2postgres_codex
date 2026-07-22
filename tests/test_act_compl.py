"""Acceptance coverage for the real act_compl XLSX input."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.flows import run_flow
from up_to_postgresql.readers import read_source


FLOW_NAME = "act_compl"
SOURCE_PATH = "act_compl/_EXTRACCIO_activitat_complementaria_sample.xlsx"
SHEET_NAME = "activitat_complementaria"

EXPECTED_COLUMNS = [
    "Any prestació (YYYY)",
    "Data Revisat",
    "Codificació genèrica codi",
    "v_Codificació genèric desc corregida",
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

EXPECTED_MAPPING_SOURCES = [
    "Any prestació (YYYY)",
    "Centre SAP codi (Hospital)",
    "Codificació genèrica codi",
    "v_Codificació genèric desc corregida",
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
    "v_Centre Codi",
    "v_Institució corregida",
    "v_Màquina descripció",
    "v_Prestació descripció",
    "v_UP Sol·licitant Corregida",
    "v_UP Sol·licitant Entitat Corregida",
    "Centre SAP desc (Hospital)",
    "Data prestació",
    "Dia setmana prestació (desc)",
    "Hora prestació",
    "Interlocutor/Metge sol·licitant codi (ordre)",
    "Interlocutor/Metge sol·licitant desc (ordre)",
    "Pacient (NHC)",
    "Servei sol·licitant codi (ordre)",
    "Servei sol·licitant desc (ordre)",
    "Tipus episodi codi",
    "UP sol·licitant entitat proveïdora codi",
    "UP sol·licitant tipus",
    "UT Gestora codi",
    "UT Gestora desc",
    "v_Direcció Clínica",
    "v_Resum Sol·licitant",
    "v_Torn",
    "v_UT Gestora corregida",
    "any_revisat",
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
    "centre_sap_descripcio",
    "data_prestacio",
    "dia_setmana_prestacio",
    "hora_prestacio",
    "metge_solicitant_codi",
    "metge_solicitant_descripcio",
    "pacient_nhc",
    "servei_solicitant_codi",
    "servei_solicitant_descripcio",
    "tipus_episodi_codi",
    "up_solicitant_entitat_proveidora_codi",
    "up_solicitant_tipus",
    "ut_gestora_codi",
    "ut_gestora_descripcio",
    "direccio_clinica",
    "resum_solicitant",
    "torn",
    "ut_gestora_corregida",
    "any_revisat",
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

    assert config.data["transformations"] == [
        {
            "name": "derive_year",
            "source_column": "Data Revisat",
            "target_column": "any_revisat",
        }
    ]

    assert config.data["validation"]["required_columns"] == EXPECTED_COLUMNS
    assert config.data["validation"]["missing_columns_policy"] == "error"
    assert config.data["validation"]["duplicate_policy"] == "report"
    assert "duplicate_key" not in config.data["validation"]

    assert config.data["load"]["target_table"] == "act_compl"
    assert config.data["load"]["load_mode"] == "replace_partition"
    assert config.data["load"]["partition_column"] == "any_revisat"
    assert config.data["load"]["reload_existing_hash"] is False

    mapping = config.data["load"]["column_mapping"]

    assert [item["source"] for item in mapping] == EXPECTED_MAPPING_SOURCES
    assert [item["target"] for item in mapping] == EXPECTED_TARGETS

    assert set(EXPECTED_MAPPING_SOURCES) == set(
        EXPECTED_COLUMNS + ["any_revisat"]
    )


def test_act_compl_reads_xlsx_with_empty_first_column(tmp_path: Path) -> None:
    pytest.importorskip("openpyxl")
    source = _write_act_compl_xlsx(tmp_path)

    frame = read_source(_config_for_source(tmp_path, source.name))

    assert frame.shape == (2, 46)
    assert frame.columns[0] == "Unnamed: 0"
    assert frame.iloc[:, 0].astype(str).str.strip().eq("").all()
    assert list(frame.columns[1:]) == EXPECTED_COLUMNS

    assert frame["Data Revisat"].min() == "2025-01-01 00:00:00"
    assert frame["Data Revisat"].max() == "2025-01-02 00:00:00"


def test_act_compl_processed_output_drops_empty_column_and_projects_mapping(
    tmp_path: Path,
) -> None:
    pytest.importorskip("openpyxl")
    pd = pytest.importorskip("pandas")

    source = _write_act_compl_xlsx(tmp_path)
    config = _config_for_source(tmp_path, source.name)
    config.data["paths"]["output_base_dir"] = str(tmp_path)

    result = run_flow(config)

    assert result.rows_read == 2
    assert result.columns_read == 46
    assert result.rows_written == 2
    assert result.columns_written == 46
    assert result.empty_columns_removed == ("Unnamed: 0",)
    assert result.duplicate_key == ()
    assert result.duplicate_rows == 0
    assert result.transformations == ("trim_strings", "derive_year")

    processed = pd.read_csv(
        result.processed_path,
        dtype=str,
        keep_default_na=False,
    )

    assert processed.shape == (2, 46)
    assert list(processed.columns) == EXPECTED_MAPPING_SOURCES
    assert "Unnamed: 0" not in processed.columns
    assert processed["any_revisat"].tolist() == ["2025", "2025"]

    report = json.loads(
        result.report_path.read_text(encoding="utf-8")
    )

    assert report["source"]["sheet"] == SHEET_NAME
    assert report["source"]["header_row"] == 2
    assert report["empty_columns_removed"] == ["Unnamed: 0"]
    assert report["transformations"] == ["trim_strings", "derive_year"]
    assert report["missing_required_columns"] == []
    assert report["postgresql"] == {"status": "skipped"}


def _config_for_source(
    tmp_path: Path,
    source_name: str,
) -> FlowConfig:
    resolved = resolve_flow_config(FLOW_NAME, "test")
    data = copy.deepcopy(resolved.data)

    data["paths"]["input_base_dir"] = str(tmp_path)
    data["paths"]["output_base_dir"] = str(tmp_path / "output")
    data["source"]["path"] = source_name

    return FlowConfig(
        name=resolved.name,
        env=resolved.env,
        data=data,
    )


def _write_act_compl_xlsx(tmp_path: Path) -> Path:
    pd = pytest.importorskip("pandas")
    source = tmp_path / "act_compl.xlsx"

    row_1 = {
        column: f"valor_1_{index}"
        for index, column in enumerate(EXPECTED_COLUMNS)
    }
    row_2 = {
        column: f"valor_2_{index}"
        for index, column in enumerate(EXPECTED_COLUMNS)
    }

    row_1.update(
        {
            "Any prestació (YYYY)": "2025",
            "Data Revisat": "2025-01-01 00:00:00",
            "Data prestació": "2025-01-01 08:30:00",
            "Número de prestacions": "1",
        }
    )

    row_2.update(
        {
            "Any prestació (YYYY)": "2025",
            "Data Revisat": "2025-01-02 00:00:00",
            "Data prestació": "2025-01-02 09:45:00",
            "Número de prestacions": "2",
        }
    )

    rows = [
        ["ignored", *["ignored"] * len(EXPECTED_COLUMNS)],
        ["", *EXPECTED_COLUMNS],
        ["", *[row_1[column] for column in EXPECTED_COLUMNS]],
        ["", *[row_2[column] for column in EXPECTED_COLUMNS]],
    ]

    frame = pd.DataFrame(rows)

    with pd.ExcelWriter(source) as writer:
        frame.to_excel(
            writer,
            sheet_name=SHEET_NAME,
            header=False,
            index=False,
        )

    return source