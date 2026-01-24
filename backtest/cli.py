from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, Sequence

from backtest.config import BacktestConfig
from backtest.runner import run_backtest


def _maybe_load_dotenv() -> None:
    raw = os.environ.get("DISABLE_DOTENV")
    if raw is not None and raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}:
        return

    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv(override=False)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a backtest from a YAML config.")
    parser.add_argument("-c", "--config", required=True, help="Path to backtest YAML config.")
    parser.add_argument("--run-id", default=None, help="Optional run id override.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional base output directory override (defaults to output.local_dir from config).",
    )
    parser.add_argument(
        "--no-strict",
        dest="strict",
        action="store_false",
        help="Disable strict YAML key validation (typo detection).",
    )
    parser.set_defaults(strict=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    _maybe_load_dotenv()
    args = _build_parser().parse_args(list(argv) if argv is not None else None)

    try:
        cfg = BacktestConfig.from_yaml(args.config, strict=bool(args.strict))

        output_dir = Path(args.output_dir) if args.output_dir else None
        result = run_backtest(
            cfg,
            run_id=args.run_id,
            output_base_dir=output_dir,
        )

        print(f"run_id={result.run_id}")
        print(f"output_dir={result.output_dir}")
        return 0
    except Exception as exc:
        message = str(exc).strip() or exc.__class__.__name__
        print(f"Error: {message}", file=sys.stderr)
        if "AZURE_STORAGE_ACCOUNT_NAME must be set" in message:
            print(
                "Hint: set AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING "
                "(you can place them in a local .env file; set DISABLE_DOTENV=true to disable loading).",
                file=sys.stderr,
            )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
