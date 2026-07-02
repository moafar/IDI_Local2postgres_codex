# src/up_to_postgresql/readers/xlsx_reader.py
"""XLSX tabular reader."""

from __future__ import annotations

from typing import Any

from up_to_postgresql.readers.base import BaseTabularReader


class XlsxReader(BaseTabularReader):
    def read(self) -> Any:
        path = self.source_path()
        header = self.header_row()

        import pandas as pd

        return pd.read_excel(
            path,
            sheet_name=self.source.get("sheet", 0),
            header=header,
            dtype=str,
            keep_default_na=False,
        )
