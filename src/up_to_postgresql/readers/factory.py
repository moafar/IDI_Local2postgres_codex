# src/up_to_postgresql/readers/factory.py
"""Build readers from resolved source configuration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.readers.base import (
    BaseTabularReader,
    ReaderError,
    UnsupportedSourceTypeError,
)
from up_to_postgresql.readers.csv_reader import CsvReader
from up_to_postgresql.readers.xlsx_reader import XlsxReader


class ReaderFactory:
    _readers: dict[str, type[BaseTabularReader]] = {
        "csv": CsvReader,
        "xlsx": XlsxReader,
    }

    @classmethod
    def create(cls, config: FlowConfig | Mapping[str, Any]) -> BaseTabularReader:
        data = config.data if isinstance(config, FlowConfig) else config
        source = data.get("source")
        if not isinstance(source, Mapping):
            raise ReaderError("Reader configuration requires a 'source' mapping.")

        source_type = source.get("type")
        try:
            reader = cls._readers[source_type]
        except KeyError as error:
            supported = ", ".join(sorted(cls._readers))
            raise UnsupportedSourceTypeError(
                f"Unsupported source.type {source_type!r}; expected one of: {supported}."
            ) from error
        return reader(config)


def read_source(config: FlowConfig | Mapping[str, Any]) -> Any:
    return ReaderFactory.create(config).read()
