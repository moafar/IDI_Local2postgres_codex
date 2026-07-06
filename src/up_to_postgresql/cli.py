# src/up_to_postgresql/cli.py
"""Command line interface for configured data flows."""

import argparse

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import ConfigError
from up_to_postgresql.flows.runner import FlowRunError, run_flow
from up_to_postgresql.loading import PostgresqlLoadError
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
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load processed rows into PostgreSQL. Requires --execute.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.load and not args.execute:
        parser.error("--load requires --execute.")
    try:
        FlowRegistry().require(args.flow)
        config = resolve_flow_config(args.flow, args.env)
        result = run_flow(config, load=args.load) if args.execute else None
    except (ConfigError, FileNotFoundError, FlowRunError, PostgresqlLoadError) as error:
        parser.error(str(error))
    print(f"flow={args.flow}")
    print(f"env={args.env}")
    if result is not None:
        print(f"processed_path={result.processed_path}")
        print(f"report_path={result.report_path}")
        if result.postgresql is not None:
            print(f"rows_loaded={result.postgresql.rows_loaded}")
    return 0
