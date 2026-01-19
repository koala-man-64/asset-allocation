# Contributing

## Development setup
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt -r requirements-dev.txt
```

## Running tests
```bash
pytest -q
```

## Updating dependency lockfiles
Docker images install from `requirements.lock.txt`, and CI installs from `requirements-dev.lock.txt`.

If you change `requirements.txt` or `requirements-dev.txt`, regenerate lockfiles:
```bash
virtualenv -p python3 /tmp/assetallocation-lock
/tmp/assetallocation-lock/bin/python -m pip install -r requirements.txt
/tmp/assetallocation-lock/bin/python -m pip freeze --exclude-editable > requirements.lock.txt
/tmp/assetallocation-lock/bin/python -m pip install -r requirements-dev.txt
/tmp/assetallocation-lock/bin/python -m pip freeze --exclude-editable > requirements-dev.lock.txt
rm -rf /tmp/assetallocation-lock
```

## Backtest development notes
- The backtest engine generates targets at close(T) and executes at open(T+1).
- Backtest strategies should consume precomputed signals (no recomputation inside the backtest loop) unless explicitly scoped.

## Pull requests
- Keep changes scoped and add tests for any new behavior.
- Update config examples/docs when changing config schema or artifacts.
