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


def test_resolver_validates_xlsx_sheet_and_header_row(tmp_path: Path) -> None:
    write(tmp_path / "common.yml", "{}\n")
    write(tmp_path / "env" / "test.yml", "env: test\n")
    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: xlsx
  path: clientes.xlsx
  header_row: 0
""",
    )

    with pytest.raises(ConfigError, match="source.sheet"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)

    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: xlsx
  path: clientes.xlsx
  sheet: datos
  header_row: 0
""",
    )

    with pytest.raises(ConfigError, match="source.header_row"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)


def test_resolver_validates_processing_validation_and_output(tmp_path: Path) -> None:
    write(tmp_path / "common.yml", "{}\n")
    write(tmp_path / "env" / "test.yml", "env: test\n")
    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: csv
  path: clientes.csv
processing:
  drop_empty_rows: yes
validation:
  required_columns:
    - id
  missing_columns_policy: stop
output:
  processed_filename: ""
""",
    )

    with pytest.raises(ConfigError, match="processing.drop_empty_rows"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)

    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: csv
  path: clientes.csv
processing:
  drop_empty_rows: true
validation:
  missing_columns_policy: stop
""",
    )

    with pytest.raises(ConfigError, match="missing_columns_policy"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)

    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: csv
  path: clientes.csv
processing:
  trim_strings: 1
""",
    )

    with pytest.raises(ConfigError, match="processing.trim_strings"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)

    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: csv
  path: clientes.csv
validation:
  duplicate_key: id
""",
    )

    with pytest.raises(ConfigError, match="validation.duplicate_key"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)

    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
source:
  type: csv
  path: clientes.csv
output:
  processed_filename: []
""",
    )

    with pytest.raises(ConfigError, match="output.processed_filename"):
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


def test_resolver_validates_postgresql_load_contract(tmp_path: Path) -> None:
    write(
        tmp_path / "common.yml",
        """
source:
  type: csv
  path: clientes.csv
postgresql:
  host: localhost
  port: 5433
  database: data_dbo_idi
  user: rom
  schema: test
""",
    )
    write(tmp_path / "env" / "test.yml", "env: test\n")
    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
load:
  target_table: clientes
  load_mode: merge
  column_mapping:
    - source: id
      target: id
""",
    )

    with pytest.raises(ConfigError, match="load.load_mode"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)

    write(
        tmp_path / "flows" / "clientes.yml",
        """
name: clientes
load:
  target_table: clientes
  load_mode: append
  column_mapping:
    - source: id
      target: id
    - source: code
      target: id
""",
    )

    with pytest.raises(ConfigError, match="duplicate target"):
        resolve_flow_config("clientes", "test", config_dir=tmp_path)


def test_resolver_accepts_replace_partition_load_mode(tmp_path: Path) -> None:
    write(
        tmp_path / "common.yml",
        """
source:
  type: csv
  path: activitat_actual.csv
postgresql:
  host: localhost
  port: 5433
  database: data_dbo_idi
  user: rom
  schema: test
""",
    )
    write(tmp_path / "env" / "test.yml", "env: test\n")
    write(
        tmp_path / "flows" / "activitat_actual.yml",
        """
name: activitat_actual
load:
  target_table: activitat_actual
  load_mode: replace_partition
  partition_column: any_prestacio
  column_mapping:
    - source: Any prestació (YYYY)
      target: any_prestacio
""",
    )

    config = resolve_flow_config("activitat_actual", "test", config_dir=tmp_path)

    assert config.data["load"]["load_mode"] == "replace_partition"
    assert config.data["load"]["partition_column"] == "any_prestacio"


def test_resolver_rejects_replace_partition_without_partition_column(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "common.yml",
        """
source:
  type: csv
  path: activitat_actual.csv
postgresql:
  host: localhost
  port: 5433
  database: data_dbo_idi
  user: rom
  schema: test
""",
    )
    write(tmp_path / "env" / "test.yml", "env: test\n")
    write(
        tmp_path / "flows" / "activitat_actual.yml",
        """
name: activitat_actual
load:
  target_table: activitat_actual
  load_mode: replace_partition
  column_mapping:
    - source: Any prestació (YYYY)
      target: any_prestacio
""",
    )

    with pytest.raises(ConfigError, match="load.partition_column"):
        resolve_flow_config("activitat_actual", "test", config_dir=tmp_path)
