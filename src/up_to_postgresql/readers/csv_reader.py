# src/up_to_postgresql/readers/csv_reader.py
"""CSV tabular reader."""

from __future__ import annotations

from typing import Any

from up_to_postgresql.readers.base import BaseTabularReader


class CsvReader(BaseTabularReader):
    def read(self) -> Any:
        path = self.source_path()
        header = self.header_row()

        import pandas as pd

        return pd.read_csv(
            path,
            encoding=self.source.get("encoding", "utf-8"),
            delimiter=self.source.get("delimiter", ","),
            header=header,
            dtype=str,
            keep_default_na=False,
        )
