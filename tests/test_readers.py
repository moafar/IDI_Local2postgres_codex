# tests/test_readers.py
"""Tests for configured tabular readers."""

from pathlib import Path
from typing import Any

import pytest

from up_to_postgresql.readers import (
    CsvReader,
    ReaderError,
    ReaderFactory,
    SourceFileNotFoundError,
    UnsupportedSourceTypeError,
    XlsxReader,
    read_source,
)


def config_for(source: dict[str, object], base_dir: Path) -> dict[str, object]:
    return {"paths": {"input_base_dir": str(base_dir)}, "source": source}


def test_factory_builds_reader_by_source_type(tmp_path: Path) -> None:
    assert isinstance(
        ReaderFactory.create(config_for({"type": "csv", "path": "data.csv"}, tmp_path)),
        CsvReader,
    )
    assert isinstance(
        ReaderFactory.create(config_for({"type": "xlsx", "path": "data.xlsx"}, tmp_path)),
        XlsxReader,
    )


def test_factory_rejects_unsupported_source_type(tmp_path: Path) -> None:
    with pytest.raises(UnsupportedSourceTypeError, match="Unsupported source.type"):
        ReaderFactory.create(config_for({"type": "json", "path": "data.json"}, tmp_path))


def test_reader_reports_missing_source_file(tmp_path: Path) -> None:
    with pytest.raises(SourceFileNotFoundError, match="Source file not found"):
        read_source(config_for({"type": "csv", "path": "missing.csv"}, tmp_path))


def test_reader_rejects_invalid_header_row(tmp_path: Path) -> None:
    source = tmp_path / "data.csv"
    source.write_text("id,name\n001,Ana\n", encoding="utf-8")

    with pytest.raises(ReaderError, match="header_row"):
        read_source(
            config_for({"type": "csv", "path": "data.csv", "header_row": 0}, tmp_path)
        )


def test_csv_reader_reads_relative_path_with_text_values(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    source = tmp_path / "data.csv"
    source.write_text("skip;skip\nid;name;empty\n001; Ana ;\n", encoding="utf-8")

    frame = read_source(
        config_for(
            {
                "type": "csv",
                "path": "data.csv",
                "encoding": "utf-8",
                "delimiter": ";",
                "header_row": 2,
            },
            tmp_path,
        )
    )

    assert list(frame.columns) == ["id", "name", "empty"]
    assert frame.to_dict(orient="records") == [
        {"id": "001", "name": " Ana ", "empty": ""}
    ]


def test_xlsx_reader_reads_first_sheet_by_default(tmp_path: Path) -> None:
    pd = pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    source = tmp_path / "book.xlsx"
    _write_xlsx(
        source,
        {
            "ventas": pd.DataFrame([["id", "value"], ["001", "10"]]),
            "other": pd.DataFrame([["id", "value"], ["999", "0"]]),
        },
    )

    frame = read_source(config_for({"type": "xlsx", "path": "book.xlsx"}, tmp_path))

    assert list(frame.columns) == ["id", "value"]
    assert frame.to_dict(orient="records") == [{"id": "001", "value": "10"}]


def test_xlsx_reader_reads_configured_sheet_with_text_values(tmp_path: Path) -> None:
    pd = pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    source = tmp_path / "book.xlsx"
    _write_xlsx(
        source,
        {
            "target": pd.DataFrame([["ignore", "ignore"], ["id", "value"], ["001", ""]]),
            "other": pd.DataFrame([["other"]]),
        },
    )

    frame = read_source(
        config_for(
            {
                "type": "xlsx",
                "path": "book.xlsx",
                "sheet": "target",
                "header_row": 2,
            },
            tmp_path,
        )
    )

    assert list(frame.columns) == ["id", "value"]
    assert frame.to_dict(orient="records") == [{"id": "001", "value": ""}]


def _write_xlsx(path: Path, sheets: dict[str, Any]) -> None:
    pd = pytest.importorskip("pandas")
    with pd.ExcelWriter(path) as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=sheet_name, header=False, index=False)
