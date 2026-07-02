# src/up_to_postgresql/readers/__init__.py
"""Read tabular source files."""

from up_to_postgresql.readers.base import (
    BaseTabularReader,
    ReaderError,
    SourceFileNotFoundError,
    UnsupportedSourceTypeError,
)
from up_to_postgresql.readers.csv_reader import CsvReader
from up_to_postgresql.readers.factory import ReaderFactory, read_source
from up_to_postgresql.readers.xlsx_reader import XlsxReader

TabularReader = BaseTabularReader

__all__ = [
    "BaseTabularReader",
    "CsvReader",
    "ReaderError",
    "ReaderFactory",
    "SourceFileNotFoundError",
    "TabularReader",
    "UnsupportedSourceTypeError",
    "XlsxReader",
    "read_source",
]
