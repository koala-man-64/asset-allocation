# Alpha Vantage Fair-Share Rate Limiting

## Goal

Prevent one long-running Bronze job from monopolizing Alpha Vantage access while still honoring provider rate limits.

## Design

1. Bronze jobs no longer take a shared `JobLock("alpha_vantage")`.
2. ETL gateway client sends caller identity headers:
   - `X-Caller-Job`
   - `X-Caller-Execution`
3. API provider routes set caller context per request.
4. API-side `RateLimiter` enforces:
   - global per-minute rate budget
   - fair caller rotation (round-robin across active callers)
   - optional bounded queue wait timeout

## Runtime Configuration

The following non-secret keys can be set from env or DB runtime-config:

1. `ALPHA_VANTAGE_RATE_LIMIT_PER_MIN` (default `300`)
2. `ALPHA_VANTAGE_TIMEOUT_SECONDS` (default `15`)
3. `ALPHA_VANTAGE_RATE_WAIT_TIMEOUT_SECONDS` (default `120`, set `<=0` to disable timeout)
4. `ALPHA_VANTAGE_MAX_WORKERS` (default `32`)

## Verification

Run targeted checks:

```bash
python3 -m ruff check alpha_vantage api core tasks tests
python3 -m pytest -q \
  tests/alpha_vantage/test_rate_limiter.py \
  tests/alpha_vantage/test_alpha_vantage_client.py \
  tests/core/test_alpha_vantage_gateway_client.py \
  tests/api/test_alpha_vantage_endpoints.py
```

Expected:

1. No shared `alpha_vantage` lock in Bronze market/finance/earnings entrypoints.
2. API logs include caller context for provider requests.
3. Rate limiter tests show both callers progress without starvation.
