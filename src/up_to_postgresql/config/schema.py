# src/up_to_postgresql/config/schema.py
"""Validate declarative flow configuration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any


VALID_ENVS = ("test", "prd")
VALID_SOURCE_TYPES = ("csv", "xlsx")
VALID_POLICIES = ("error", "warning", "report")
VALID_LOAD_MODES = ("fail", "replace", "append")
FLOW_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


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

    _validate_source(config.get("source"))
    _validate_processing(config.get("processing", {}))
    _validate_validation(config.get("validation", {}))
    _validate_output(config.get("output", {}))
    _validate_transformations(config.get("transformations", []))
    _validate_postgresql(config.get("postgresql", {}), resolved_env)
    _validate_load(config.get("load", {}))
    return FlowConfig(name=name, env=resolved_env, data=dict(config))


def _validate_source(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise ConfigError("Flow configuration requires a 'source' mapping.")

    source_type = value.get("type")
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"Invalid source.type {source_type!r}; expected one of {VALID_SOURCE_TYPES}."
        )

    source_path = value.get("path")
    if not isinstance(source_path, str) or not source_path:
        raise ConfigError("Flow source requires a non-empty string 'path'.")

    sheet = value.get("sheet")
    if source_type == "xlsx" and (not isinstance(sheet, str) or not sheet):
        raise ConfigError("XLSX sources require a non-empty string 'source.sheet'.")
    if source_type == "csv" and sheet is not None:
        raise ConfigError("CSV sources do not support 'source.sheet'.")

    _positive_integer(value.get("header_row", 1), "source.header_row")


def _validate_processing(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise ConfigError("'processing' must be a mapping when present.")
    _optional_bool(value, "drop_empty_rows", "processing.drop_empty_rows")
    _optional_bool(value, "drop_empty_columns", "processing.drop_empty_columns")
    trim_strings = value.get("trim_strings")
    if trim_strings is not None and not (
        isinstance(trim_strings, bool)
        or trim_strings == "all"
        or (
            isinstance(trim_strings, Sequence)
            and not isinstance(trim_strings, (str, bytes))
            and all(isinstance(column, str) and column for column in trim_strings)
        )
    ):
        raise ConfigError(
            "'processing.trim_strings' must be a boolean, 'all', or a list of column names."
        )


def _validate_validation(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise ConfigError("'validation' must be a mapping when present.")
    _optional_string_list(value, "required_columns", "validation.required_columns")
    _optional_string_list(value, "duplicate_key", "validation.duplicate_key")
    _optional_policy(value, "missing_columns_policy", "validation.missing_columns_policy")
    _optional_policy(value, "duplicate_policy", "validation.duplicate_policy")


def _validate_output(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise ConfigError("'output' must be a mapping when present.")
    for key in ("processed_filename", "report_filename"):
        item = value.get(key)
        if item is not None and (not isinstance(item, str) or not item):
            raise ConfigError(f"'output.{key}' must be a non-empty string.")
        if isinstance(item, str) and Path(item).is_absolute():
            raise ConfigError(f"'output.{key}' must be relative to output_base_dir.")


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


def _validate_postgresql(value: Any, env: str) -> None:
    if value is None or value == {}:
        return
    if not isinstance(value, Mapping):
        raise ConfigError("'postgresql' must be a mapping when present.")
    for key in ("host", "database", "user", "schema"):
        item = value.get(key)
        if not isinstance(item, str) or not item:
            raise ConfigError(f"'postgresql.{key}' must be a non-empty string.")
    if value.get("schema") != env:
        raise ConfigError("'postgresql.schema' must match the selected environment.")
    port = value.get("port")
    if not isinstance(port, int) or isinstance(port, bool) or port < 1:
        raise ConfigError("'postgresql.port' must be a positive integer.")
    if "password" in value:
        raise ConfigError("'postgresql.password' must not be stored in configuration.")


def _validate_load(value: Any) -> None:
    if value is None or value == {}:
        return
    if not isinstance(value, Mapping):
        raise ConfigError("'load' must be a mapping when present.")
    target_table = value.get("target_table")
    if not _is_identifier(target_table):
        raise ConfigError("'load.target_table' must be a PostgreSQL identifier.")
    load_mode = value.get("load_mode")
    if load_mode not in VALID_LOAD_MODES:
        raise ConfigError(f"'load.load_mode' must be one of {VALID_LOAD_MODES}.")
    reload_hash = value.get("reload_existing_hash")
    if reload_hash is not None and not isinstance(reload_hash, bool):
        raise ConfigError("'load.reload_existing_hash' must be a boolean.")
    mapping = value.get("column_mapping")
    if not isinstance(mapping, Sequence) or isinstance(mapping, (str, bytes)) or not mapping:
        raise ConfigError("'load.column_mapping' must be a non-empty list.")
    targets: list[str] = []
    for item in mapping:
        if not isinstance(item, Mapping):
            raise ConfigError("Each load.column_mapping item must be a mapping.")
        source = item.get("source")
        target = item.get("target")
        if not isinstance(source, str) or not source:
            raise ConfigError("Each load.column_mapping item requires a source.")
        if not _is_identifier(target):
            raise ConfigError("Each load.column_mapping item requires a target identifier.")
        targets.append(target)
    if len(targets) != len(set(targets)):
        raise ConfigError("'load.column_mapping' contains duplicate target columns.")


def _optional_bool(value: Mapping[str, Any], key: str, label: str) -> None:
    item = value.get(key)
    if item is not None and not isinstance(item, bool):
        raise ConfigError(f"'{label}' must be a boolean.")


def _optional_policy(value: Mapping[str, Any], key: str, label: str) -> None:
    item = value.get(key)
    if item is not None and item not in VALID_POLICIES:
        raise ConfigError(f"'{label}' must be one of {VALID_POLICIES}.")


def _optional_string_list(value: Mapping[str, Any], key: str, label: str) -> None:
    item = value.get(key)
    if item is None:
        return
    if not isinstance(item, Sequence) or isinstance(item, (str, bytes)):
        raise ConfigError(f"'{label}' must be a list of strings.")
    if not all(isinstance(column, str) and column for column in item):
        raise ConfigError(f"'{label}' must contain only non-empty strings.")


def _positive_integer(value: Any, label: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ConfigError(f"'{label}' must be a positive integer.")


def _is_identifier(value: Any) -> bool:
    return isinstance(value, str) and bool(IDENTIFIER_PATTERN.fullmatch(value))
