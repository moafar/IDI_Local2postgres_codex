# tests/test_config_resolver.py
"""Tests for declarative configuration resolution."""

from pathlib import Path

import pytest

from up_to_postgresql.config.resolver import merge_recursive, resolve_flow_config
from up_to_postgresql.config.schema import ConfigError


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_resolver_merges_common_env_and_flow(tmp_path: Path) -> None:
    write(
        tmp_path / "common.yml",
        """
paths:
  input_dir: data/input
source:
  encoding: utf-8
  delimiter: ","
transformations: []
""",
    )
    write(
        tmp_path / "env" / "test.yml",
        """
env: test
paths:
  input_dir: data/input/test
""",
    )
    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: csv
  path: clientes.csv
  delimiter: ";"
transformations:
  - name: trim_strings
    columns:
      - nombre
""",
    )

    resolved = resolve_flow_config("clientes", "test", config_dir=tmp_path)

    assert resolved.name == "clientes"
    assert resolved.env == "test"
    assert resolved.data["paths"]["input_dir"] == "data/input/test"
    assert resolved.data["source"]["encoding"] == "utf-8"
    assert resolved.data["source"]["delimiter"] == ";"
    assert resolved.data["transformations"] == [
        {"name": "trim_strings", "columns": ["nombre"]}
    ]


def test_merge_recursive_replaces_lists() -> None:
    merged = merge_recursive(
        {"items": ["a"], "nested": {"keep": "yes", "replace": ["old"]}},
        {"nested": {"replace": ["new"]}},
    )

    assert merged == {"items": ["a"], "nested": {"keep": "yes", "replace": ["new"]}}


def test_resolver_rejects_unsupported_source_type(tmp_path: Path) -> None:
    write(tmp_path / "common.yml", "source:\n  encoding: utf-8\n")
    write(tmp_path / "env" / "test.yml", "env: test\n")
    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: json
  path: clientes.json
""",
    )

    with pytest.raises(ConfigError, match="source.type"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)


def test_resolver_rejects_non_declarative_transformations(tmp_path: Path) -> None:
    write(tmp_path / "common.yml", "{}\n")
    write(tmp_path / "env" / "test.yml", "env: test\n")
    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: csv
  path: clientes.csv
transformations: trim_strings
""",
    )

    with pytest.raises(ConfigError, match="transformations"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)
