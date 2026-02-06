# Strategy Pipeline: Layer/Domain-Aware Data Bindings (Design Reference)

**Status:** Design reference (not yet implemented)  
**Purpose:** Keep strategy modules “field-first” (logical fields) while data sourcing is configurable across medallion layers and domains.

---

## Why this matters

If universe + signals can pull from `silver/gold/platinum` (and different domains), you don’t want strategy modules hardcoding:

- table names
- schemas
- which layer a column comes from

Instead, strategy modules should ask for:

- “give me `close` and `adv20_usd` for date `dt`”
- “give me `return_60d` for the universe on `dt`”

…and a context builder resolves those fields from configured sources.

---

## Strategy config additions (minimal but powerful)

### 1) Data sources dictionary

Define named sources with `layer`, `domain`, and a dataset identifier (`table/view/path`).

```json
{
  "data_sources": {
    "prices":   { "layer": "gold",     "domain": "market", "dataset": "daily_prices" },
    "liq":      { "layer": "gold",     "domain": "market", "dataset": "liquidity_metrics" },
    "features": { "layer": "platinum", "domain": "equity", "dataset": "xs_features" }
  }
}
```

### 2) Bindings map logical fields → (source, column)

```json
{
  "bindings": {
    "close":         { "source": "prices",   "column": "close" },
    "is_tradable":   { "source": "prices",   "column": "is_tradable" },
    "adv20_usd":     { "source": "liq",      "column": "adv20_usd" },
    "return_60d":    { "source": "features", "column": "ret_60d" },
    "quality_score": { "source": "features", "column": "quality" },
    "sector":        { "source": "features", "column": "gics_sector" }
  }
}
```

### 3) Universe/signals reference bindings, not raw columns

Universe resolver config references binding keys:

```json
{
  "universe": {
    "enabled": true,
    "type": "basic_filters",
    "filters": {
      "min_price": { "field": "close", "gte": 5.0 },
      "min_adv":   { "field": "adv20_usd", "gte": 1000000 },
      "tradable":  { "field": "is_tradable", "eq": true }
    }
  }
}
```

Signal components reference bindings too:

```json
{
  "signals": {
    "enabled": true,
    "components": [
      {
        "name": "mom60",
        "field": "return_60d",
        "weight": 0.6,
        "missing": { "policy": "drop_symbol" },
        "transform": { "winsorize": { "p": 0.01 }, "zscore": {} }
      },
      {
        "name": "quality",
        "field": "quality_score",
        "weight": 0.4,
        "missing": { "policy": "impute", "value": 0.0 },
        "transform": { "rank_pct": {} }
      }
    ],
    "composer": { "type": "weighted_sum" }
  }
}
```

**Net effect:** If you later decide `return_60d` should come from gold instead of platinum, it’s a config-only change.

---

## Cross-cutting implementation pieces (used in all phases)

### A) `MedallionDataProvider` interface

Add one abstraction the strategy engine uses to fetch data from any `(layer, domain, dataset)`.

- `fetch_frame(source_ref, dt, columns, symbols=None) -> DataFrame`
- Standardized output indexed by `symbol` (and `dt` applied/filtered consistently)

Responsibilities:

- translate `(layer, domain, dataset)` to the project’s storage mechanism
- enforce consistent keys (`symbol`, `date`)
- optionally cache per `(source, dt)` to avoid repeated I/O

### B) `StrategyContextBuilder`

Given `StrategyConfig.bindings`, it:

- determines which sources/columns are needed for the date
- fetches them once
- aligns/joins into a single features frame keyed by `symbol`
- optionally exposes “raw frames” (useful for diagnostics)

### C) Diagnostics include lineage

Every output should be able to say:

- field `return_60d` came from `platinum/equity/xs_features.ret_60d`
- universe filter `min_adv` came from `gold/market/liquidity_metrics.adv20_usd`

This is especially valuable when configs span layers.

---

## Phase plan

### Phase 1 — Foundation + Core Strategy Pipeline (layer/domain aware)

**Goal:** Produce target weights per decision date using:

`Config → Universe → Signals → Transforms → Composite → Rank → Select → Allocate`

Layer/domain selection works via bindings.

**Deliverables**

1) Config schema + defaults

- `StrategyConfig` includes `data_sources`, `bindings`, and module sections (`universe`, `signals`, `transforms`, etc.).
- Validation:
  - every referenced binding key exists
  - every binding references a valid data source
  - type checks for module params (e.g., winsorize `p` in `(0, 0.5)`)

2) Data access + context build

- `MedallionDataProvider` implementation that can fetch by `(layer, domain, dataset)`
- `StrategyContextBuilder` that:
  - pulls required binding columns for `dt`
  - merges into a canonical per-symbol dataframe
  - returns `StrategyContext(dt, universe_frame, feature_frame, classifications_frame, ...)`

3) Core modules implemented (first pass)

- Universe Resolver (basic filters using binding fields)
- Signal Component Resolver (binding → series extraction + missing policy)
- Transform chain (winsorize, zscore, rank_pct, clipping)
- Alpha composer (weighted sum)
- Ranker (deterministic tie-breaker = symbol)
- Selector (top-N + long-only)
- Weight allocator (equal weight + score-proportional)

4) Minimal diagnostics

- universe counts + exclusion reasons (by filter)
- missing counts per component
- top/bottom of composite score
- selection membership

**Integration points**

- Identify the existing “per date strategy computation” function in the backtest runner.
- Replace internals with:
  - `cfg = config_store.get(strategy_id)`
  - `ctx = context_builder.build(dt, cfg)`
  - `targets = strategy_engine.run(ctx, state)`

**Acceptance criteria**

- Changing `layer/domain/dataset` for a binding changes outputs without code changes.
- Pipeline output is deterministic across runs.
- Unit tests cover:
  - binding resolution
  - universe filtering correctness
  - missing policy behavior
  - ordering tie-break determinism

### Phase 2 — Constraints + Rebalance/Turnover + Full Diagnostics

**Goal:** Move from “raw targets” to “portfolio-legal targets” and “only trade when appropriate,” while producing rich explainability.

Add: `Constrain → Rebalance/Turnover Policy`

**Deliverables**

1) Constraint Engine

- max name weight
- group caps (sector/industry) using a binding like `sector`
- gross/net enforcement
- deterministic redistribution policy (start with clip+renormalize)

2) Rebalance / Turnover policy

- schedule: daily/weekly/monthly
- drift threshold: don’t trade small deltas
- min trade weight
- optional turnover cap (simple proportional scaling of deltas)

3) Diagnostics Collector v1 (structured + stable schema)

- universe exclusions
- component raw/normalized summaries
- composite and rank snapshots
- selection changes (adds/drops)
- constraints hit + offenders + redistribution summary
- rebalance decision (triggered? why?) + turnover metrics
- data lineage: per field → `(layer/domain/dataset/column)`

4) Golden test fixtures

- small synthetic dataset spanning multiple layers/domains
- expected outputs for a few dates pinned in tests

**Acceptance criteria**

- Constraints always produce valid targets with predictable redistribution.
- Rebalance policy prevents churn as configured.
- Diagnostics answer “why” for:
  - why a symbol excluded
  - why selected
  - why weight clipped
  - why trade skipped (drift/min size)

### Phase 3 — Lifecycle Engine (Stops/Holding Rules) + Trade Intents + Hardening

**Goal:** Add rules that can override strategy desires:

`Lifecycle (stops/holding rules) → trade intents`

**Deliverables**

1) `StrategyState` + lifecycle bookkeeping  
Persist per symbol:

- entry date/price (or entry reference)
- high watermark / low watermark (for trailing)
- holding days counter
- cooldown end date
- last exit reason

2) Lifecycle Engine (configurable, deterministic)

- stop loss
- trailing stop
- take profit
- max holding period
- cooldown / re-entry block
- same-bar conflict resolution (lifecycle overrides selection/weights)

Output:

- lifecycle-adjusted targets (force 0 where needed)
- lifecycle event log for diagnostics

3) Target → Trade Intent Translator

- compare current holdings vs final targets
- net deltas by symbol
- produce “order intents” (not broker orders):
  - `symbol`, `side`, `delta_weight`, optional urgency ordering

4) Hardening

- determinism regression test (same inputs twice → same outputs)
- performance pass:
  - data fetch caching per `(source, dt)`
  - vectorized transforms
- config versioning support (if not already in Phase 1)

**Acceptance criteria**

- Stops reliably override target weights and are visible in diagnostics.
- Cooldowns block re-entry deterministically.
- Trade intent list matches delta between holdings and lifecycle-adjusted constrained targets.

---

## Implementation notes specific to multi-layer + domain sourcing

1) Field-first strategy modules  
Every module should ask for field names, not tables/columns.

- Universe: filters operate on field keys like `close`, `adv20_usd`
- Signals: components reference fields like `return_60d`, `quality_score`
- Constraints: reference `sector`, `industry`, etc.

2) Context builder resolves and aligns  
The context builder must handle:

- different symbol coverage per source (outer join then module-level missing policy)
- different column names (handled by binding map)
- optional symbol allow/deny lists (apply late or early, but consistently)

3) Missing data policy belongs at the module/component level  
Even if the source is “platinum features,” you’ll still have NaNs. Keep missing policy logic in:

- component resolver (drop vs impute)
- universe resolver (exclude if required field missing)

---

## Upfront improvement recommendation

If you only do one “extra” thing now: add bindings + lineage in diagnostics.

Without that, as soon as you mix `silver/gold/platinum`, you’ll lose time debugging “why did this change?” because the same logical feature might come from different sources between strategy versions or environments.

---

## Source

This doc is a cleaned reference of a design note supplied in chat (includes examples and a phased delivery plan).
