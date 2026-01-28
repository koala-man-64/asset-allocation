# AssetAllocation

Python project for market/finance data pipelines, ranking signals, and a backtest framework.

## Quickstart

### Prerequisites
- Python 3.10 (matches Docker/CI) and `pip`

### Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
python3 -m pip install -e .
```

### Run tests
```bash
python3 -m pytest -q
```



## Backend API (UI)

The UI calls the FastAPI service under `/api/*` (see `api/API_ENDPOINTS.md`). Common endpoints:

- `GET /api/data/{layer}/market?ticker={ticker}` (layer: `silver|gold`)
- `GET /api/data/{layer}/finance/{sub_domain}?ticker={ticker}` (layer: `silver|gold`)
- `GET /api/ranking/strategies`
- `GET /api/system/health`
- `WS /api/ws/updates`

## Deployment

Azure deployment is driven by `.github/workflows/deploy.yml` and manifests under `deploy/`.

## Dependency lockfiles
- `requirements.lock.txt` is used by Docker builds for reproducible images.
- `requirements-dev.lock.txt` is used by CI for reproducible test installs.

## Docs
- `api/API_ENDPOINTS.md`

