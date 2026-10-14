# src/up_to_postgresql/cli.py
"""Run configured flows and explicit demanda substitutions recalculations.

Depends on configuration resolution and the flow runner. Manual recalculation
requires a year and confirmation, reads no source file, and cannot combine with
loading or source overrides; all other flow commands retain their behavior.
"""

import argparse

from psycopg import Error as PostgresqlError

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.config.schema import ConfigError
from up_to_postgresql.flows.demanda_sustituciones import DemandaSustitucionesError
from up_to_postgresql.flows.runner import (
    FlowRunError,
    recalcular_demanda_sustituciones,
    run_flow,
)
from up_to_postgresql.loading import PostgresqlLoadError
from up_to_postgresql.registry import FlowRegistry
from up_to_postgresql.source import SourcePathError, with_source_path


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
    parser.add_argument(
        "--source",
        help=(
            "Source file for this execution, relative to paths.input_base_dir. "
            "Overrides source.path in memory only."
        ),
    )
    parser.add_argument(
        "--recalcular-sustituciones",
        action="store_true",
        help="Recalculate one year of demanda substitutions without loading a file.",
    )
    parser.add_argument(
        "--anio",
        type=int,
        help="Year to recalculate with --recalcular-sustituciones.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.load and not args.execute:
        parser.error("--load requires --execute.")
    if args.recalcular_sustituciones:
        if not args.execute:
            parser.error("--recalcular-sustituciones requires --execute.")
        if args.load or args.source is not None:
            parser.error("--recalcular-sustituciones cannot use --load or --source.")
        if args.flow != "demanda":
            parser.error("--recalcular-sustituciones is only available for demanda.")
        if args.anio is None:
            parser.error("--anio is required for --recalcular-sustituciones.")
    elif args.anio is not None:
        parser.error("--anio requires --recalcular-sustituciones.")
    try:
        FlowRegistry().require(args.flow)
        config = resolve_flow_config(args.flow, args.env)
        if args.recalcular_sustituciones:
            recalculation = recalcular_demanda_sustituciones(config, anio=args.anio)
            print(f"flow={args.flow}")
            print(f"env={args.env}")
            print(f"anio_procesado={recalculation.sustituciones.anio_procesado}")
            print(f"report_path={recalculation.report_path}")
            return 0
        if args.source is not None:
            config = with_source_path(config, args.source)
        result = run_flow(config, load=args.load) if args.execute else None
    except (
        ConfigError,
        DemandaSustitucionesError,
        FileNotFoundError,
        FlowRunError,
        PostgresqlLoadError,
        SourcePathError,
    ) as error:
        notes = getattr(error, "__notes__", ())
        parser.error("; ".join((str(error), *notes)))
    except PostgresqlError as error:
        if args.flow == "demanda" and (args.load or args.recalcular_sustituciones):
            notes = getattr(error, "__notes__", ())
            parser.error("; ".join((f"{type(error).__name__}: {error}", *notes)))
        raise
    print(f"flow={args.flow}")
    print(f"env={args.env}")
    if result is not None:
        print(f"processed_path={result.processed_path}")
        print(f"report_path={result.report_path}")
        if result.postgresql is not None:
            print(f"rows_loaded={result.postgresql.rows_loaded}")
    return 0
