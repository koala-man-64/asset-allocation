from __future__ import annotations

PIOTROSKI_FINANCE_SUBDOMAINS: tuple[str, ...] = (
    "balance_sheet",
    "income_statement",
    "cash_flow",
)

VALUATION_FINANCE_COLUMNS: tuple[str, ...] = (
    "market_cap",
    "pe_ratio",
    "price_to_book",
    "price_to_sales",
    "price_to_cash_flow",
    "price_to_free_cash_flow",
    "dividend_yield",
    "return_on_assets",
    "return_on_equity",
    "debt_to_equity",
    "current_ratio",
    "quick_ratio",
    "cash_ratio",
    "ev_to_sales",
    "ev_to_ebitda",
    "enterprise_value",
    "earnings_per_share",
    "free_cash_flow",
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
        *VALUATION_FINANCE_COLUMNS,
    ),
}

SILVER_FINANCE_SOURCE_ALIASES_BY_SUBDOMAIN: dict[str, dict[str, tuple[str, ...]]] = {
    "balance_sheet": {
        "long_term_debt": (
            "long_term_debt",
            "long_term_debt_and_capital_lease_obligations",
            "long_term_debt_noncurrent",
        ),
        "total_assets": ("total_assets",),
        "current_assets": ("current_assets", "total_current_assets"),
        "current_liabilities": ("current_liabilities", "total_current_liabilities"),
        "shares_outstanding": (
            "shares_outstanding",
            "common_stock_shares_outstanding",
            "common_shares_outstanding",
            "ordinary_shares_number",
            "share_issued",
        ),
        "timeframe": ("timeframe",),
    },
    "income_statement": {
        "total_revenue": ("total_revenue", "revenue", "revenues"),
        "gross_profit": ("gross_profit",),
        "net_income": (
            "net_income",
            "net_income_loss",
            "consolidated_net_income_loss",
            "net_income_loss_attributable_to_parent",
        ),
        "shares_outstanding": (
            "shares_outstanding",
            "diluted_shares_outstanding",
            "basic_shares_outstanding",
            "weighted_average_shares",
        ),
        "timeframe": ("timeframe",),
    },
    "cash_flow": {
        "operating_cash_flow": (
            "operating_cash_flow",
            "net_cash_flow_from_operating_activities",
            "net_cash_from_operating_activities",
            "net_cash_provided_by_operating_activities",
        ),
        "timeframe": ("timeframe",),
    },
    "valuation": {
        "market_cap": ("market_cap",),
        "pe_ratio": ("pe_ratio", "price_to_earnings"),
        "price_to_book": ("price_to_book",),
        "price_to_sales": ("price_to_sales",),
        "price_to_cash_flow": ("price_to_cash_flow",),
        "price_to_free_cash_flow": ("price_to_free_cash_flow",),
        "dividend_yield": ("dividend_yield",),
        "return_on_assets": ("return_on_assets",),
        "return_on_equity": ("return_on_equity",),
        "debt_to_equity": ("debt_to_equity",),
        "current_ratio": ("current_ratio", "current"),
        "quick_ratio": ("quick_ratio", "quick"),
        "cash_ratio": ("cash_ratio", "cash"),
        "ev_to_sales": ("ev_to_sales",),
        "ev_to_ebitda": ("ev_to_ebitda",),
        "enterprise_value": ("enterprise_value",),
        "earnings_per_share": ("earnings_per_share",),
        "free_cash_flow": ("free_cash_flow",),
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
