# src/up_to_postgresql/config/schema.py
"""Validate declarative flow configuration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import re
from typing import Any


VALID_ENVS = ("test", "prd")
VALID_SOURCE_TYPES = ("csv", "xlsx")
FLOW_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class ConfigError(ValueError):
    """Raised when declarative configuration is invalid."""


@dataclass(frozen=True)
class FlowConfig:
    name: str
    env: str
    data: dict[str, Any]


def validate_flow_name(name: str) -> str:
    if not isinstance(name, str) or not FLOW_NAME_PATTERN.fullmatch(name):
        raise ConfigError(
            "Flow names must start with a lowercase letter and contain only "
            "lowercase letters, digits, and underscores."
        )
    return name


def validate_env(env: str) -> str:
    if env not in VALID_ENVS:
        raise ConfigError(f"Invalid environment {env!r}; expected one of {VALID_ENVS}.")
    return env


def validate_flow_config(
    config: Mapping[str, Any], *, expected_name: str | None = None, env: str | None = None
) -> FlowConfig:
    if not isinstance(config, Mapping):
        raise ConfigError("Flow configuration must be a mapping.")

    name = config.get("name")
    if not isinstance(name, str):
        raise ConfigError("Flow configuration requires a string 'name'.")
    validate_flow_name(name)

    if expected_name is not None and name != expected_name:
        raise ConfigError(
            f"Flow file name {expected_name!r} does not match configured name {name!r}."
        )

    resolved_env = config.get("env") if env is None else env
    if not isinstance(resolved_env, str):
        raise ConfigError("Flow configuration requires a string environment.")
    validate_env(resolved_env)

    source = config.get("source")
    if not isinstance(source, Mapping):
        raise ConfigError("Flow configuration requires a 'source' mapping.")

    source_type = source.get("type")
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"Invalid source.type {source_type!r}; expected one of {VALID_SOURCE_TYPES}."
        )

    source_path = source.get("path")
    if not isinstance(source_path, str) or not source_path:
        raise ConfigError("Flow source requires a non-empty string 'path'.")

    _validate_transformations(config.get("transformations", []))
    return FlowConfig(name=name, env=resolved_env, data=dict(config))


def _validate_transformations(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ConfigError("'transformations' must be a list when present.")
    for item in value:
        if not isinstance(item, Mapping):
            raise ConfigError("Each transformation must be a mapping.")
        name = item.get("name")
        if not isinstance(name, str) or not name:
            raise ConfigError("Each transformation requires a non-empty string 'name'.")
