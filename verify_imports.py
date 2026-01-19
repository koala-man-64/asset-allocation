from __future__ import annotations

import importlib
from typing import Iterable


def _check_group(label: str, modules: Iterable[str]) -> bool:
    ok = True
    for name in modules:
        try:
            importlib.import_module(name)
        except Exception as exc:
            ok = False
            print(f"{label} import FAILED: {name} ({exc})")
    if ok:
        print(f"{label} imports: OK")
    return ok


def main() -> int:
    print("Verifying imports...")
    checks = [
        (
            "Market Data",
            [
                "scripts.market_data.bronze_market_data",
                "scripts.market_data.silver_market_data",
                "scripts.market_data.gold_market_data",
            ],
        ),
        (
            "Finance Data",
            [
                "scripts.finance_data.bronze_finance_data",
                "scripts.finance_data.silver_finance_data",
                "scripts.finance_data.gold_finance_data",
            ],
        ),
        (
            "Earnings Data",
            [
                "scripts.earnings_data.bronze_earnings_data",
                "scripts.earnings_data.silver_earnings_data",
                "scripts.earnings_data.gold_earnings_data",
            ],
        ),
        (
            "Price Target Data",
            [
                "scripts.price_target_data.bronze_price_target_data",
                "scripts.price_target_data.silver_price_target_data",
                "scripts.price_target_data.gold_price_target_data",
            ],
        ),
    ]

    ok = True
    for label, modules in checks:
        ok = _check_group(label, modules) and ok

    if not ok:
        print("One or more imports failed. Ensure the project is installed (e.g. `pip install -e .`).")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
