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
    ),
    "income_statement": (
        "date",
        "symbol",
        "total_revenue",
        "gross_profit",
        "net_income",
    ),
    "cash_flow": (
        "date",
        "symbol",
        "operating_cash_flow",
    ),
    "valuation": (
        "date",
        "symbol",
        "market_cap",
        "pe_ratio",
        "forward_pe",
    ),
}

SILVER_FINANCE_SOURCE_ALIASES_BY_SUBDOMAIN: dict[str, dict[str, tuple[str, ...]]] = {
    "balance_sheet": {
        "long_term_debt": (
            "long_term_debt",
            "longTermDebt",
            "Long Term Debt",
            "long_term_debt_and_capital_lease_obligation",
            "longTermDebtAndCapitalLeaseObligation",
            "Long Term Debt And Capital Lease Obligation",
            "long_term_debt_capital_lease_obligation",
            "Long Term Debt & Capital Lease Obligation",
            "long_term_debt_noncurrent",
            "longTermDebtNoncurrent",
        ),
        "total_assets": (
            "total_assets",
            "totalAssets",
            "Total Assets",
        ),
        "current_assets": (
            "current_assets",
            "currentAssets",
            "Current Assets",
            "total_current_assets",
            "totalCurrentAssets",
            "Total Current Assets",
        ),
        "current_liabilities": (
            "current_liabilities",
            "currentLiabilities",
            "Current Liabilities",
            "total_current_liabilities",
            "totalCurrentLiabilities",
            "Total Current Liabilities",
        ),
        "shares_outstanding": (
            "shares_outstanding",
            "sharesOutstanding",
            "Shares Outstanding",
            "common_stock_shares_outstanding",
            "commonStockSharesOutstanding",
            "Common Stock Shares Outstanding",
            "common_shares_outstanding",
            "Common Shares Outstanding",
            "ordinary_shares_number",
            "ordinarySharesNumber",
            "Ordinary Shares Number",
            "share_issued",
            "shareIssued",
            "Share Issued",
        ),
    },
    "income_statement": {
        "total_revenue": (
            "total_revenue",
            "totalRevenue",
            "Total Revenue",
            "revenue",
            "Revenue",
        ),
        "gross_profit": (
            "gross_profit",
            "grossProfit",
            "Gross Profit",
        ),
        "net_income": (
            "net_income",
            "netIncome",
            "Net Income",
            "net_income_common_stockholders",
            "netIncomeCommonStockholders",
            "Net Income Common Stockholders",
        ),
    },
    "cash_flow": {
        "operating_cash_flow": (
            "operating_cash_flow",
            "operatingCashFlow",
            "operatingCashflow",
            "Operating Cash Flow",
            "total_cash_from_operating_activities",
            "totalCashFromOperatingActivities",
            "Total Cash From Operating Activities",
            "cash_flow_from_continuing_operating_activities",
            "cashFlowFromContinuingOperatingActivities",
            "Cash Flow From Continuing Operating Activities",
            "net_cash_provided_by_operating_activities",
            "netCashProvidedByOperatingActivities",
            "Net Cash Provided by Operating Activities",
        ),
    },
    "valuation": {
        "market_cap": (
            "market_cap",
            "MarketCapitalization",
            "Market Cap",
            "MarketCap",
        ),
        "pe_ratio": (
            "pe_ratio",
            "PERatio",
            "P/E",
            "PE Ratio",
        ),
        "forward_pe": (
            "forward_pe",
            "ForwardPE",
            "Forward P/E",
            "Forward PE",
        ),
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
    "overview": ("Valuation", "quarterly_valuation_measures"),
    "valuation": ("Valuation", "quarterly_valuation_measures"),
}
