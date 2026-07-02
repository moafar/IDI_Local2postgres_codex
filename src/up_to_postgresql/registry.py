# src/up_to_postgresql/registry.py
"""Discover configured flow definitions."""

from __future__ import annotations

from pathlib import Path

from up_to_postgresql.config.resolver import load_yaml
from up_to_postgresql.config.schema import ConfigError, validate_flow_name


class FlowRegistry:
    def __init__(self, flows_dir: Path | str = "config/flows") -> None:
        self.flows_dir = Path(flows_dir)

    def discover(self) -> dict[str, Path]:
        flows: dict[str, Path] = {}
        if not self.flows_dir.exists():
            return flows

        for path in sorted(self.flows_dir.glob("*.yml")):
            file_name = path.stem
            validate_flow_name(file_name)
            data = load_yaml(path)
            configured_name = data.get("name", file_name)
            if not isinstance(configured_name, str):
                raise ConfigError(f"{path}: flow name must be a string.")
            validate_flow_name(configured_name)
            if configured_name in flows:
                raise ConfigError(f"Duplicate flow name {configured_name!r}.")
            if configured_name != file_name:
                raise ConfigError(
                    f"{path}: configured flow name {configured_name!r} must match "
                    f"file name {file_name!r}."
                )
            flows[configured_name] = path
        return flows

    def require(self, name: str) -> Path:
        validate_flow_name(name)
        flows = self.discover()
        try:
            return flows[name]
        except KeyError as error:
            raise ConfigError(f"Unknown flow {name!r}.") from error
