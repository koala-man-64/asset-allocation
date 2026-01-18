from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.runner import run_backtest


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
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
