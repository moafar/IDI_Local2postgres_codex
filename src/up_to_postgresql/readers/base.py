# src/up_to_postgresql/readers/base.py
"""Common tabular reader interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from up_to_postgresql.config.schema import FlowConfig
from up_to_postgresql.source import SourcePathError, resolve_source_path


class ReaderError(ValueError):
    """Raised when tabular reader configuration is invalid."""


class SourceFileNotFoundError(FileNotFoundError):
    """Raised when the configured source file does not exist."""


class UnsupportedSourceTypeError(ReaderError):
    """Raised when no reader exists for a configured source type."""


class BaseTabularReader(ABC):
    def __init__(self, config: FlowConfig | Mapping[str, Any]) -> None:
        self.config = config
        self.data = config.data if isinstance(config, FlowConfig) else config

    @abstractmethod
    def read(self) -> Any:
        """Read configured source into a pandas DataFrame."""

    @property
    def source(self) -> Mapping[str, Any]:
        source = self.data.get("source")
        if not isinstance(source, Mapping):
            raise ReaderError("Reader configuration requires a 'source' mapping.")
        return source

    @property
    def paths(self) -> Mapping[str, Any]:
        paths = self.data.get("paths", {})
        if not isinstance(paths, Mapping):
            raise ReaderError("Reader configuration 'paths' must be a mapping.")
        return paths

    def source_path(self) -> Path:
        try:
            return resolve_source_path(self.data)
        except FileNotFoundError as error:
            raise SourceFileNotFoundError(str(error)) from error
        except SourcePathError as error:
            raise ReaderError(str(error)) from error

    def header_row(self) -> int | None:
        raw_header = self.source.get("header_row", 1)
        if raw_header is None:
            return None
        try:
            header = int(raw_header)
        except (TypeError, ValueError) as error:
            raise ReaderError("Reader source.header_row must be an integer.") from error
        if header < 1:
            raise ReaderError("Reader source.header_row must be greater than or equal to 1.")
        return header - 1
