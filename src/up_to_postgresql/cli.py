import argparse


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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    print(f"flow={args.flow}")
    print(f"env={args.env}")
    return 0

