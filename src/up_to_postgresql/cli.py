# src/up_to_postgresql/cli.py
"""Command line interface for configured data flows."""

import argparse

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import ConfigError
from up_to_postgresql.flows.runner import FlowRunError, run_flow
from up_to_postgresql.registry import FlowRegistry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m up_to_postgresql",
        description="Run a configured data flow.",
    )
    parser.add_argument(
        "--flow",
        required=True,
        help="Unique flow name to execute.",
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=("test", "prd"),
        help="Target environment.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the configured file-processing flow.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        FlowRegistry().require(args.flow)
        config = resolve_flow_config(args.flow, args.env)
        result = run_flow(config) if args.execute else None
    except (ConfigError, FileNotFoundError, FlowRunError) as error:
        parser.error(str(error))
    print(f"flow={args.flow}")
    print(f"env={args.env}")
    if result is not None:
        print(f"processed_path={result.processed_path}")
        print(f"report_path={result.report_path}")
    return 0
