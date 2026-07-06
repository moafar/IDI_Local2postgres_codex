"""Resolve configured flow source files."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import copy
from typing import Any

from up_to_postgresql.config.schema import FlowConfig


class SourcePathError(ValueError):
    """Raised when a flow source path is invalid."""


def with_source_path(config: FlowConfig, source_path: str) -> FlowConfig:
    """Return a copy of config with source.path overridden in memory."""
    data = copy.deepcopy(config.data)
    source = data.get("source")
    if not isinstance(source, dict):
        raise SourcePathError("Flow configuration requires a source mapping.")
    source["path"] = source_path
    return replace(config, data=data)


def resolve_source_path(config: FlowConfig | dict[str, Any]) -> Path:
    """Resolve source.path under paths.input_base_dir and ensure it exists."""
    data = config.data if isinstance(config, FlowConfig) else config
    source = data.get("source")
    if not isinstance(source, dict):
        raise SourcePathError("Flow configuration requires a source mapping.")

    raw_path = source.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        raise SourcePathError(
            "Flow source.path is required for execution; pass --source or configure "
            "source.path."
        )

    relative_path = Path(raw_path)
    if relative_path.is_absolute():
        raise SourcePathError("Flow source.path must be relative to paths.input_base_dir.")
    if ".." in relative_path.parts:
        raise SourcePathError("Flow source.path must not contain '..'.")

    base_dir = _input_base_dir(data)
    resolved_base = base_dir.resolve()
    resolved_path = (base_dir / relative_path).resolve()
    if resolved_path != resolved_base and resolved_base not in resolved_path.parents:
        raise SourcePathError("Flow source.path must stay inside paths.input_base_dir.")
    if not resolved_path.exists():
        raise FileNotFoundError(f"Source file not found: {resolved_path}")
    if not resolved_path.is_file():
        raise SourcePathError(f"Source path is not a file: {resolved_path}")
    return resolved_path


def _input_base_dir(data: dict[str, Any]) -> Path:
    paths = data.get("paths", {})
    if not isinstance(paths, dict):
        raise SourcePathError("Flow paths must be a mapping.")
    raw_base_dir = paths.get("input_base_dir", paths.get("input_dir", "."))
    if not isinstance(raw_base_dir, str) or not raw_base_dir:
        raise SourcePathError("Flow paths.input_base_dir must be a non-empty string.")
    return Path(raw_base_dir)
