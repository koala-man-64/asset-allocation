from __future__ import annotations

PIOTROSKI_FINANCE_SUBDOMAINS: tuple[str, ...] = (
    "balance_sheet",
    "income_statement",
    "cash_flow",
)

SILVER_FINANCE_SUBDOMAINS: tuple[str, ...] = (
    *PIOTROSKI_FINANCE_SUBDOMAINS,
    "valuation",
)

SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN: dict[str, tuple[str, ...]] = {
    "balance_sheet": (
        "date",
        "symbol",
        "long_term_debt",
        "total_assets",
        "current_assets",
        "current_liabilities",
        "shares_outstanding",
        "timeframe",
    ),
    "income_statement": (
        "date",
        "symbol",
        "total_revenue",
        "gross_profit",
        "net_income",
        "shares_outstanding",
        "timeframe",
    ),
    "cash_flow": (
        "date",
        "symbol",
        "operating_cash_flow",
        "timeframe",
    ),
    "valuation": (
        "date",
        "symbol",
        "market_cap",
        "pe_ratio",
    ),
}

SILVER_FINANCE_SOURCE_ALIASES_BY_SUBDOMAIN: dict[str, dict[str, tuple[str, ...]]] = {
    "balance_sheet": {
        "long_term_debt": ("long_term_debt",),
        "total_assets": ("total_assets",),
        "current_assets": ("current_assets",),
        "current_liabilities": ("current_liabilities",),
        "shares_outstanding": ("shares_outstanding",),
        "timeframe": ("timeframe",),
    },
    "income_statement": {
        "total_revenue": ("total_revenue",),
        "gross_profit": ("gross_profit",),
        "net_income": ("net_income",),
        "shares_outstanding": ("shares_outstanding",),
        "timeframe": ("timeframe",),
    },
    "cash_flow": {
        "operating_cash_flow": ("operating_cash_flow",),
        "timeframe": ("timeframe",),
    },
    "valuation": {
        "market_cap": ("market_cap",),
        "pe_ratio": ("pe_ratio",),
    },
}

PIOTROSKI_ALPHA26_REPORT_LAYOUTS: dict[str, tuple[str, str]] = {
    "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
    "income_statement": ("Income Statement", "quarterly_financials"),
    "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
}

SILVER_FINANCE_ALPHA26_REPORT_LAYOUTS: dict[str, tuple[str, str]] = {
    **PIOTROSKI_ALPHA26_REPORT_LAYOUTS,
    "valuation": ("Valuation", "quarterly_valuation_measures"),
}

SILVER_FINANCE_REPORT_TYPE_TO_LAYOUT: dict[str, tuple[str, str]] = {
    "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
    "income_statement": ("Income Statement", "quarterly_financials"),
    "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
    "valuation": ("Valuation", "quarterly_valuation_measures"),
}
