# tests/test_registry.py
"""Tests for flow discovery."""

from pathlib import Path

import pytest

from up_to_postgresql.config.schema import ConfigError
from up_to_postgresql.registry import FlowRegistry


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_registry_discovers_yml_flows(tmp_path: Path) -> None:
    flows_dir = tmp_path / "flows"
    write(flows_dir / "clientes.yml", "name: clientes\n")
    write(flows_dir / "ventas.yml", "name: ventas\n")

    flows = FlowRegistry(flows_dir).discover()

    assert set(flows) == {"clientes", "ventas"}
    assert flows["clientes"] == flows_dir / "clientes.yml"


def test_registry_rejects_duplicate_declared_names(tmp_path: Path) -> None:
    flows_dir = tmp_path / "flows"
    write(flows_dir / "clientes.yml", "name: clientes\n")
    write(flows_dir / "clientes_copia.yml", "name: clientes\n")

    with pytest.raises(ConfigError, match="Duplicate flow name"):
        FlowRegistry(flows_dir).discover()


def test_registry_rejects_invalid_declared_names(tmp_path: Path) -> None:
    flows_dir = tmp_path / "flows"
    write(flows_dir / "clientes.yml", "name: Clientes\n")

    with pytest.raises(ConfigError, match="Flow names"):
        FlowRegistry(flows_dir).discover()


def test_registry_rejects_declared_name_that_differs_from_file_name(
    tmp_path: Path,
) -> None:
    flows_dir = tmp_path / "flows"
    write(flows_dir / "clientes_alias.yml", "name: clientes\n")

    with pytest.raises(ConfigError, match="must match file name"):
        FlowRegistry(flows_dir).discover()


def test_registry_requires_existing_flow(tmp_path: Path) -> None:
    flows_dir = tmp_path / "flows"
    write(flows_dir / "clientes.yml", "name: clientes\n")

    with pytest.raises(ConfigError, match="Unknown flow"):
        FlowRegistry(flows_dir).require("ventas")
