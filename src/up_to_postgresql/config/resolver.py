# src/up_to_postgresql/config/resolver.py
"""Resolve layered declarative configuration files."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import copy
import shlex
from typing import Any

from up_to_postgresql.config.schema import FlowConfig, validate_env, validate_flow_config


class YamlSubsetError(ValueError):
    """Raised when a YAML file uses unsupported syntax."""


def resolve_flow_config(
    flow: str, env: str, *, config_dir: Path | str = "config"
) -> FlowConfig:
    validate_env(env)
    root = Path(config_dir)
    common = load_yaml(root / "common.yml")
    env_config = load_yaml(root / "env" / f"{env}.yml")
    flow_config = load_yaml(root / "flows" / f"{flow}.yml")

    merged = merge_recursive(common, env_config)
    merged = merge_recursive(merged, flow_config)
    return validate_flow_config(merged, expected_name=flow, env=env)


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    parsed = _YamlSubsetParser(path.read_text(encoding="utf-8"), path).parse()
    if not isinstance(parsed, dict):
        raise YamlSubsetError(f"{path} must contain a top-level mapping.")
    return parsed


def merge_recursive(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(dict(base))
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = merge_recursive(existing, value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


class _YamlSubsetParser:
    def __init__(self, text: str, path: Path) -> None:
        self.path = path
        self.lines = self._prepare_lines(text)

    def parse(self) -> Any:
        if not self.lines:
            return {}
        value, index = self._parse_block(0, self.lines[0][0])
        if index != len(self.lines):
            _, line_number, _ = self.lines[index]
            raise YamlSubsetError(f"{self.path}:{line_number}: unexpected content.")
        return value

    def _prepare_lines(self, text: str) -> list[tuple[int, int, str]]:
        prepared: list[tuple[int, int, str]] = []
        for line_number, raw in enumerate(text.splitlines(), start=1):
            if "\t" in raw:
                raise YamlSubsetError(f"{self.path}:{line_number}: tabs are unsupported.")
            without_comment = self._strip_comment(raw).rstrip()
            if not without_comment.strip():
                continue
            indent = len(without_comment) - len(without_comment.lstrip(" "))
            prepared.append((indent, line_number, without_comment[indent:]))
        return prepared

    def _parse_block(self, index: int, indent: int) -> tuple[Any, int]:
        if self.lines[index][0] != indent:
            _, line_number, _ = self.lines[index]
            raise YamlSubsetError(f"{self.path}:{line_number}: invalid indentation.")
        content = self.lines[index][2]
        if content in ("{}", "[]"):
            return self._parse_scalar(content, self.lines[index][1]), index + 1
        if content.startswith("- "):
            return self._parse_list(index, indent)
        return self._parse_mapping(index, indent)

    def _parse_mapping(self, index: int, indent: int) -> tuple[dict[str, Any], int]:
        result: dict[str, Any] = {}
        while index < len(self.lines):
            current_indent, line_number, content = self.lines[index]
            if current_indent < indent:
                break
            if current_indent > indent:
                raise YamlSubsetError(f"{self.path}:{line_number}: invalid indentation.")
            if content.startswith("- "):
                break
            key, raw_value = self._split_key_value(content, line_number)
            if raw_value == "":
                if index + 1 >= len(self.lines) or self.lines[index + 1][0] <= indent:
                    result[key] = {}
                    index += 1
                else:
                    result[key], index = self._parse_block(index + 1, self.lines[index + 1][0])
            else:
                result[key] = self._parse_scalar(raw_value, line_number)
                index += 1
        return result, index

    def _parse_list(self, index: int, indent: int) -> tuple[list[Any], int]:
        result: list[Any] = []
        while index < len(self.lines):
            current_indent, line_number, content = self.lines[index]
            if current_indent < indent:
                break
            if current_indent > indent:
                raise YamlSubsetError(f"{self.path}:{line_number}: invalid indentation.")
            if not content.startswith("- "):
                break
            item = content[2:].strip()
            if item == "":
                if index + 1 >= len(self.lines) or self.lines[index + 1][0] <= indent:
                    result.append(None)
                    index += 1
                else:
                    value, index = self._parse_block(index + 1, self.lines[index + 1][0])
                    result.append(value)
            elif self._looks_like_mapping_item(item):
                key, raw_value = self._split_key_value(item, line_number)
                mapping: dict[str, Any] = {
                    key: self._parse_scalar(raw_value, line_number) if raw_value else {}
                }
                index += 1
                if index < len(self.lines) and self.lines[index][0] > indent:
                    nested, index = self._parse_mapping(index, self.lines[index][0])
                    mapping = merge_recursive(mapping, nested)
                result.append(mapping)
            else:
                result.append(self._parse_scalar(item, line_number))
                index += 1
        return result, index

    def _split_key_value(self, content: str, line_number: int) -> tuple[str, str]:
        if ":" not in content:
            raise YamlSubsetError(f"{self.path}:{line_number}: expected 'key: value'.")
        key, raw_value = content.split(":", 1)
        key = key.strip()
        if not key:
            raise YamlSubsetError(f"{self.path}:{line_number}: empty keys are unsupported.")
        return key, raw_value.strip()

    def _parse_scalar(self, raw_value: str, line_number: int) -> Any:
        if raw_value == "[]":
            return []
        if raw_value == "{}":
            return {}
        if raw_value in ("true", "false"):
            return raw_value == "true"
        if raw_value in ("null", "~"):
            return None
        if raw_value.startswith(('"', "'")):
            try:
                return shlex.split(raw_value)[0]
            except ValueError as error:
                raise YamlSubsetError(f"{self.path}:{line_number}: {error}") from error
        return raw_value

    def _looks_like_mapping_item(self, item: str) -> bool:
        return ":" in item and not item.startswith(('"', "'"))

    def _strip_comment(self, raw: str) -> str:
        quote: str | None = None
        for index, char in enumerate(raw):
            if char in ("'", '"'):
                quote = None if quote == char else char
            if char == "#" and quote is None:
                return raw[:index]
        return raw
