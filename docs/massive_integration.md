# Massive Provider Integration

## Scope

This integration adds Massive as an API-hosted provider and replaces Alpha Vantage at the Bronze layer for:

- `tasks.market_data.bronze_market_data`
- `tasks.finance_data.bronze_finance_data`

Option A migration is active: Bronze earnings ingestion remains on Alpha Vantage.

## Added Components

- Provider package: `massive_provider/`
- API gateway service: `api/service/massive_gateway.py`
- API routes: `api/endpoints/massive.py`
- ETL gateway client: `core/massive_gateway_client.py`

## API Endpoints

All routes are mounted under `/api/providers/massive`.

- `GET /time-series/daily?symbol=AAPL&from=2025-01-01&to=2025-01-10&adjusted=true`
- `GET /fundamentals/short-interest?symbol=AAPL`
- `GET /fundamentals/short-volume?symbol=AAPL`
- `GET /fundamentals/float?symbol=AAPL&as_of=2025-01-10`
- `GET /financials/{report}?symbol=AAPL` where `report` is:
  - `balance_sheet`
  - `cash_flow`
  - `income_statement`
  - `overview` (mapped to ratios for legacy continuity)
  - `ratios`

Backwards-compatible alias:

- `GET /finance/{report}?symbol=AAPL`

## Environment Variables

### Required for API-hosted Massive integration

- `MASSIVE_API_KEY`

### Optional tuning

- `MASSIVE_BASE_URL` (default: `https://api.massive.com`)
- `MASSIVE_TIMEOUT_SECONDS` (default: `30`)
- `MASSIVE_PREFER_OFFICIAL_SDK` (default: `true`)
- `MASSIVE_MAX_WORKERS` (default: `32`)
- `MASSIVE_FINANCE_FRESH_DAYS` (default: `28`)

### Optional flat files

- `MASSIVE_FLATFILES_ENDPOINT_URL`
- `MASSIVE_FLATFILES_BUCKET`
- `MASSIVE_FLATFILES_ACCESS_KEY_ID`
- `MASSIVE_FLATFILES_SECRET_ACCESS_KEY`
- `MASSIVE_FLATFILES_SESSION_TOKEN`

### Optional websocket defaults

- `MASSIVE_WS_SUBSCRIPTIONS`

## Bronze Layer Behavior

### Market

- Bronze market job calls API gateway via `MassiveGatewayClient`.
- Daily bars are normalized to canonical CSV:
  - `Date,Open,High,Low,Close,Volume`

### Finance

- Bronze finance job calls API gateway via `MassiveGatewayClient`.
- Existing output paths are preserved for downstream compatibility.
- Legacy `overview` report contract is mapped to Massive ratios.

### Earnings (Option A)

- Bronze earnings job remains Alpha Vantage-backed for now.

## Validation Commands

```bash
python3 -m compileall massive_provider api core tasks
pytest -q tests/api/test_massive_endpoints.py tests/core/test_massive_gateway_client.py tests/market_data/test_bronze_market_data.py tests/finance_data/test_bronze_finance_data.py
ruff check api core tasks tests
```

## Smoke Checks

```bash
curl -s "http://localhost:8000/api/providers/massive/time-series/daily?symbol=AAPL&from=2025-01-01&to=2025-01-10&adjusted=true" | head -n 5
curl -s "http://localhost:8000/api/providers/massive/fundamentals/short-interest?symbol=AAPL" | head -c 400 && echo
curl -s "http://localhost:8000/api/providers/massive/financials/balance_sheet?symbol=AAPL" | head -c 400 && echo
```
