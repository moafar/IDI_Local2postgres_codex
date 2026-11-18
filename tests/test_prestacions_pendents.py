"""Tests for the pending prestations snapshot using the configured runner."""

import csv
import json
from pathlib import Path

import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import ConfigError, validate_flow_config
from up_to_postgresql.flows import runner
from up_to_postgresql.loading import PostgresqlLoadResult
from up_to_postgresql.transforms import prestacions_pendents as pending


SOURCE = Path("data/input/test") / pending.SOURCE_FILENAME
REPORT = "prestacions_pendents_report"
INTERVENTIONAL = "Procediments intervencionistes"
IDENTIFIER = pending.KEY[1]


@pytest.fixture
def config(tmp_path):
    config = resolve_flow_config(pending.FLOW_NAME, "test")
    config.data["paths"].update(
        input_base_dir=str(tmp_path), output_base_dir=str(tmp_path)
    )
    return config


def input_columns(config):
    return [item["source"] for item in config.data["load"]["column_mapping"]]


def row(identifier="000042", **overrides):
    return {
        pending.CENTRE: "AR",
        IDENTIFIER: identifier,
        pending.PERIOD: "2026-10",
        pending.YEAR: "2026",
        pending.SERVICE_DATE: "31/10/26",
        pending.DESCRIPTION: "Altres",
        pending.COUNT: "1",
        **overrides,
    }


def write_source(config, rows, *, header=None):
    header = header if header is not None else input_columns(config)[:25]
    source = Path(config.data["paths"]["input_base_dir"]) / pending.SOURCE_FILENAME
    with source.open("w", encoding="utf-8", newline="") as handle:
        handle.write('\n\n"Detall"\n\n\n\n')
        writer = csv.writer(handle, delimiter=";", quoting=csv.QUOTE_ALL)
        writer.writerow(header)
        for entry in rows:
            handle.write("\n")
            writer.writerow(entry.get(name, "") for name in header)
    return source


def assert_controls(report, **expected):
    assert {field: report[field] for field in expected} == expected


@pytest.mark.parametrize("env", ["test", "prd"])
def test_resolves_snapshot_contract(env):
    config = resolve_flow_config(pending.FLOW_NAME, env)
    assert config.data["postgresql"]["schema"] == env
    assert config.data["source"]["path"] == pending.SOURCE_FILENAME
    assert config.data["source"]["header_row"] == 2
    assert config.data["load"]["target_table"] == pending.FLOW_NAME
    assert config.data["load"]["load_mode"] == "replace"
    assert tuple(input_columns(config)[25:]) == pending.DERIVED_COLUMNS


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("periode_informe", "2026-13"),
        ("data_tall", "2026-02-29"),
        ("nivell_9_fragments", [""]),
    ],
)
def test_invalid_config(config, key, value):
    config.data["transformations"][0][key] = value
    with pytest.raises(ConfigError, match=key):
        validate_flow_config(config.data)


def test_snapshot_rules_and_report(config):
    rows = [
        row("00001", **{pending.COUNT: "4", pending.LEVEL_9: ""}),
        row(
            "00002",
            **{pending.DESCRIPTION: INTERVENTIONAL, pending.LEVEL_9: "EcO GUIA"},
        ),
        row(
            "00003",
            **{pending.DESCRIPTION: INTERVENTIONAL, pending.LEVEL_9: "tÈCNICA DE roll"},
        ),
        row(
            "00004",
            **{pending.DESCRIPTION: INTERVENTIONAL, pending.LEVEL_9: "RADIOFREQÜÈNCIA"},
        ),
        row(
            "00005",
            **{
                pending.DESCRIPTION: INTERVENTIONAL,
                pending.LEVEL_9: "MARCADORES METÁLICOS",
            },
        ),
        row(
            "00006",
            **{
                pending.DESCRIPTION: INTERVENTIONAL,
                pending.LEVEL_9: "paaf d’adenopaties cervicals (a cegues)",
            },
        ),
        row(
            "00007",
            **{pending.DESCRIPTION: INTERVENTIONAL, pending.LEVEL_9: "RADIOFREQUENCIA"},
        ),
        row(
            "00008",
            **{
                pending.DESCRIPTION: INTERVENTIONAL,
                pending.LEVEL_9: " Marcadores metálicos",
            },
        ),
        row(
            "00009",
            **{
                pending.DESCRIPTION: INTERVENTIONAL,
                pending.LEVEL_9: "PAAF d'adenopaties cervicals (a cegues)",
            },
        ),
        row("00010", **{pending.DESCRIPTION: INTERVENTIONAL, pending.LEVEL_9: ""}),
        row("00011", **{pending.DESCRIPTION: INTERVENTIONAL.lower()}),
        row(
            "00012",
            **{
                pending.CENTRE: "ZZ",
                pending.PERIOD: "2027-01",
                pending.YEAR: "2027",
                pending.SERVICE_DATE: "01/01/27",
                pending.COUNT: "12",
            },
        ),
        row(
            "00013",
            **{
                pending.PERIOD: "2024-02",
                pending.YEAR: "2024",
                pending.SERVICE_DATE: "29/02/24",
            },
        ),
    ]
    write_source(config, rows)
    result = runner.run_flow(config)
    assert result.report_path.name == f"{REPORT}_preview.json"
    with result.processed_path.open(encoding="utf-8", newline="") as handle:
        output = csv.DictReader(handle)
        assert output.fieldnames == input_columns(config)
        records = {record[IDENTIFIER]: record for record in output}

    assert list(records) == [
        "00001",
        "00002",
        "00003",
        "00004",
        "00005",
        "00006",
        "00011",
        "00012",
        "00013",
    ]
    assert records["00001"]["Centre nom"] == "IDI Lleida"
    assert records["00012"]["Centre nom"] == "ZZ"
    assert records["00001"]["Màquina de la prestació codi"] == ""
    assert records["00001"]["Mes"] == "4"
    assert records["00012"]["Mes"] == "0"
    assert records["00012"]["Acumulat"] == "12"
    assert records["00012"]["Mes prestació número"] == "1"
    assert records["00013"]["Mes prestació número"] == "2"
    assert records["00001"]["Període informe"] == "2026-10"
    assert records["00001"]["Data tall"] == "2026-11-09"
    report = json.loads(result.report_path.read_text())
    assert_controls(
        report,
        rows_read=13,
        distinct_keys=13,
        incomplete_key_rows=0,
        duplicate_rows=0,
        interventional_rows=9,
        rows_excluded=4,
        rows_included=9,
        rows_published=0,
        mes=10,
        acumulat=23,
        postgresql={"status": "skipped"},
    )


def test_transform_preserves_null_centre_and_accepts_missing_level_9(config):
    import pandas as pd

    frame = pd.DataFrame(
        [
            row("1", **{pending.CENTRE: None}),
            row("2", **{pending.DESCRIPTION: INTERVENTIONAL, pending.LEVEL_9: None}),
        ]
    )
    output = pending.transform(frame, config.data["transformations"][0], {})
    assert output[IDENTIFIER].tolist() == ["1"]
    assert output["Centre nom"].isna().all()


@pytest.mark.parametrize(
    ("rows", "metric", "value"),
    [
        ([row(), row(**{pending.DESCRIPTION: INTERVENTIONAL})], "duplicate_rows", 2),
        ([row(**{pending.CENTRE: " "})], "incomplete_key_rows", 1),
        ([row("")], "incomplete_key_rows", 1),
        ([], "rows_read", 0),
        ([row(**{pending.SERVICE_DATE: "31/09/26"})], "invalid_date_rows", 1),
        ([row(**{pending.SERVICE_DATE: "32/10/26"})], "invalid_date_rows", 1),
        ([row(**{pending.SERVICE_DATE: "2026-10-31"})], "invalid_date_rows", 1),
        ([row(**{pending.YEAR: "2027"})], "invalid_period_rows", 1),
        ([row(**{pending.PERIOD: "2026-00"})], "invalid_period_rows", 1),
        ([row(**{pending.COUNT: ""})], "invalid_count_rows", 1),
        ([row(**{pending.COUNT: "-1"})], "invalid_count_rows", 1),
        ([row(**{pending.COUNT: "2.5"})], "invalid_count_rows", 1),
        ([row(**{pending.COUNT: "2147483648"})], "invalid_count_rows", 1),
        ([row(**{pending.DESCRIPTION: INTERVENTIONAL})], "rows_included", 0),
    ],
)
def test_invalid_snapshot_never_reaches_loader(
    config, tmp_path, monkeypatch, rows, metric, value
):
    write_source(config, rows)

    def unexpected_load(*args, **kwargs):
        pytest.fail("Invalid snapshot reached the PostgreSQL load path.")

    monkeypatch.setattr(runner, "prepare_postgresql_load", unexpected_load)
    monkeypatch.setattr(runner, "load_to_postgresql", unexpected_load)
    published_report = tmp_path / f"{REPORT}.json"
    published_report.write_text("previous successful publication")
    with pytest.raises(runner.FlowRunError):
        runner.run_flow(config, load=True)

    error_path = tmp_path / f"{REPORT}_error.json"
    error_content = error_path.read_text()
    report = json.loads(error_content)
    assert_controls(report, **{metric: value}, rows_published=0, status="error")
    assert "000042" not in error_content
    assert len(report["source_hash"]) == 64
    assert published_report.read_text() == "previous successful publication"
    assert not (tmp_path / "prestacions_pendents_processed.csv").exists()


@pytest.mark.parametrize("malformed", ['""\n', '"one field"\n', '"unterminated\n'])
def test_malformed_source(config, tmp_path, malformed):
    source = write_source(config, [row()])
    with source.open("a", encoding="utf-8") as handle:
        handle.write(malformed)
    with pytest.raises(runner.FlowRunError):
        runner.run_flow(config)
    report = json.loads((tmp_path / f"{REPORT}_error.json").read_text())
    assert_controls(report, stage="read", rows_published=0, status="error")


def test_wrong_header(config, tmp_path):
    write_source(config, [row()], header=list(reversed(input_columns(config)[:25])))
    with pytest.raises(runner.FlowRunError, match="Unexpected CSV header"):
        runner.run_flow(config)
    assert (tmp_path / f"{REPORT}_error.json").is_file()


def test_publication_count_comes_from_loader_result(config, monkeypatch):
    write_source(config, [row()])
    monkeypatch.setattr(
        runner, "prepare_postgresql_load", lambda *args, **kw: "test-password"
    )

    def fake_load(cfg, frame, **kwargs):
        assert cfg.data["load"]["load_mode"] == "replace"
        assert frame["Mes"].tolist() == [1]
        return PostgresqlLoadResult(
            target_schema="test",
            target_table=pending.FLOW_NAME,
            load_mode="replace",
            source_filename=pending.SOURCE_FILENAME,
            source_hash="test-hash",
            rows_loaded=1,
            status="success",
        )

    monkeypatch.setattr(runner, "load_to_postgresql", fake_load)
    result = runner.run_flow(config, load=True)
    assert result.report_path.name == f"{REPORT}.json"
    assert_controls(json.loads(result.report_path.read_text()), rows_published=1)


@pytest.mark.skipif(not SOURCE.is_file(), reason="Requires the local IDI sample CSV.")
def test_sample_reconciliation(config):
    config.data["paths"]["input_base_dir"] = str(SOURCE.parent)
    result = runner.run_flow(config)
    assert_controls(
        json.loads(result.report_path.read_text()),
        rows_read=252901,
        distinct_keys=252901,
        duplicate_rows=0,
        incomplete_key_rows=0,
        interventional_rows=911,
        rows_excluded=108,
        rows_included=252793,
        rows_published=0,
        count_before_filter=252901,
        mes_before_filter=27918,
        excluded_mes=3,
        acumulat=252793,
        mes=27915,
    )
