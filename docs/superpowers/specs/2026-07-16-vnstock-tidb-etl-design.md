# Vnstock TiDB ETL Design

## Goal

Build a Python ETL system that fetches daily listed-equity OHLCV data from `vnstock_data`, maps the DataFrame fields to SQLAlchemy ORM models, and upserts rows into TiDB Cloud using connection config from `.env`.

V1 scope is deliberately narrow:

- Asset universe: listed equities from `Reference().equity.list_by_exchange()`.
- Data API: `Market().equity(symbol).ohlcv(start=date, end=date, interval="1D")`.
- Run modes: one day or date-range backfill through CLI.
- Idempotency: upsert by `(symbol, trade_date)`.

## Current project context

The repository is mostly Vnstock documentation and examples. `test.py` currently probes `vnstock_data` with:

```python
from vnstock_data import Market

market = Market()
print(market.equity("FPT").ohlcv(start="2026-07-14", end="2026-07-14", interval='1M'))
```

User auth state shows tier `silver`, so `vnstock_data` is available. The project has a `.env` file for environment config.

## Architecture

Create a package-style ETL module:

```text
etl/
  config.py        # .env loading, TiDB URL, batch size, rate-limit settings
  db.py            # SQLAlchemy engine/session helpers
  models.py        # EquitySymbol, EquityDailyOhlcv ORM models
  extractors.py    # vnstock_data Reference/Market calls
  transforms.py    # DataFrame -> validated row dictionaries
  loaders.py       # TiDB/MySQL bulk upsert
  retry.py         # retry queue and request throttling
  logging.py       # console + file logging setup
  cli.py           # daily/backfill commands
```

Main flow:

1. CLI receives `--date` or `--start/--end`.
2. Load `.env` and create SQLAlchemy engine for TiDB.
3. Extract symbol universe from `Reference().equity.list_by_exchange()`.
4. Upsert symbol metadata.
5. For each requested trading date and symbol, call OHLCV API.
6. Validate and transform output schema.
7. Bulk upsert OHLCV rows into TiDB.
8. Print and log run summary.

## CLI

Commands:

```bash
python -m etl.cli daily --date 2026-07-14
python -m etl.cli backfill --start 2026-01-01 --end 2026-07-14
```

CLI behavior:

- Validate date format before any API/DB work.
- `daily` processes one date.
- `backfill` processes inclusive date range.
- Each run gets a `batch_id` for metadata and logs.
- Final summary includes total symbols, success count, no-data count, failed count, retry count, and rows upserted.

## Source schemas and mapping

Mapping must follow repo docs, not guessed column names.

Reference schema source:

- `docs/vnstock-data/schema/01-reference.md`
- `Reference().equity.list_by_exchange()` columns:
  - `symbol` (`object`)
  - `exchange` (`object`)
  - `organ_name` (`object`)
  - `organ_short_name` (`object`)
  - `icb_code_lv2` (`object`)

Market schema source:

- `docs/vnstock-data/schema/02-market.md`
- `Market().equity('TCB').ohlcv(length='1Y')` columns:
  - `time` (`datetime64[ns]`)
  - `open` (`float64`)
  - `high` (`float64`)
  - `low` (`float64`)
  - `close` (`float64`)
  - `volume` (`int64`)

## Database schema

Use SQLAlchemy 2.x + Alembic. TiDB is MySQL-compatible, so use MySQL dialect types and upsert syntax.

### `equity_symbols`

```text
symbol VARCHAR PRIMARY KEY        # from symbol
exchange VARCHAR                  # from exchange
organ_name VARCHAR                # from organ_name
organ_short_name VARCHAR          # from organ_short_name
icb_code_lv2 VARCHAR              # from icb_code_lv2
source VARCHAR                    # e.g. vnstock_data
created_at DATETIME               # first insert time
updated_at DATETIME               # changed on upsert
fetched_at DATETIME               # API fetch time
batch_id VARCHAR                  # CLI run id
```

### `equity_daily_ohlcv`

```text
id BIGINT PRIMARY KEY AUTO_INCREMENT
symbol VARCHAR NOT NULL           # FK to equity_symbols.symbol
trade_date DATE NOT NULL          # from time.date()
open DECIMAL(18,4)                # from open
high DECIMAL(18,4)                # from high
low DECIMAL(18,4)                 # from low
close DECIMAL(18,4)               # from close
volume BIGINT                     # from volume
source VARCHAR                    # e.g. vnstock_data
created_at DATETIME               # first insert time
updated_at DATETIME               # changed on upsert
fetched_at DATETIME               # API fetch time
batch_id VARCHAR                  # CLI run id
UNIQUE(symbol, trade_date)
```

Upsert behavior for OHLCV:

- Insert sets `created_at`, `updated_at`, `fetched_at`, `batch_id`.
- Duplicate `(symbol, trade_date)` updates OHLCV fields, `updated_at`, `fetched_at`, and `batch_id`.
- Duplicate update does not overwrite `created_at`.

## Extraction

Use `vnstock_data` Unified UI. Before finalizing API calls during implementation, inspect the installed package with `show_api()` and `show_doc()` as required by project instructions.

Planned calls:

```python
from vnstock_data import Reference, Market

ref = Reference()
symbols_df = ref.equity.list_by_exchange()

mkt = Market()
ohlcv_df = mkt.equity(symbol).ohlcv(start=date, end=date, interval="1D")
```

Use `list_by_exchange()` as the V1 source of truth because it includes exchange and company metadata needed for `equity_symbols`.

## Transform rules

Symbol transform:

- Require `symbol`.
- Preserve `exchange`, `organ_name`, `organ_short_name`, `icb_code_lv2` when present.
- Attach metadata: `source`, `created_at`, `updated_at`, `fetched_at`, `batch_id`.

OHLCV transform:

- Require columns: `time`, `open`, `high`, `low`, `close`, `volume`.
- Convert `time` to `trade_date` using date component.
- Convert price fields to decimal-compatible values.
- Convert `volume` to integer-compatible value.
- Attach `symbol`, `source`, `created_at`, `updated_at`, `fetched_at`, `batch_id`.
- Empty DataFrame is `no_data`, not error.

## Loading

Use SQLAlchemy Core insert statements with MySQL/TiDB `ON DUPLICATE KEY UPDATE`.

Loading order:

1. Upsert `equity_symbols`.
2. Upsert `equity_daily_ohlcv` batches.

DB failure behavior:

- Roll back failed DB transaction.
- Log failed batch.
- Exit with non-zero code because persisted state is uncertain.

## Rate limiting and retry

Known limit: 300 requests/minute.

V1 must include a request throttle so normal symbol iteration does not exceed 300 requests/minute. Throttle may run after each request or after small batches, but effective request rate must stay below the limit.

Retry behavior:

- API network/dead-connect failures go into a retry queue.
- The job does not sleep-block all symbols after one failure.
- Failed item continues later when `next_retry_at = now + 5 minutes`.
- Continue processing other symbols/date items while retry items wait.
- Each item gets up to 3 attempts.
- End of job drains retry queue until each item succeeds or reaches max attempts.
- Final failed items are logged and counted.

Retry log statuses:

- `queued_retry`
- `retry_success`
- `retry_failed`

## Logging

Log to both console and file.

File path pattern:

```text
logs/etl_YYYYMMDD.log
```

Log fields should include:

- `batch_id`
- `run_mode`
- `date`
- `symbol`
- `status`
- `attempt`
- `rows`
- `error` when present

Status values:

- `success`
- `no_data`
- `failed`
- `queued_retry`
- `retrying`
- `retry_success`
- `retry_failed`

## Testing

Use pytest.

V1 tests:

- Transform test: valid OHLCV DataFrame with docs schema maps to row dict.
- Transform test: missing required OHLCV column raises clear error.
- Transform test: `time` becomes `trade_date`.
- Symbol transform test: `Reference().equity.list_by_exchange()` docs schema maps to symbol rows.
- Upsert test: statement uses unique `(symbol, trade_date)` and updates metadata fields except `created_at`.
- CLI test: invalid date fails before API/DB calls.
- CLI test: `daily --date` builds one-date work list.
- CLI test: `backfill --start --end` builds inclusive date work list.
- Retry queue test: failed item schedules `next_retry_at` without blocking other work.

Optional integration smoke test:

- Use real `.env` and vnstock auth.
- Run one date for 1-2 symbols such as `FPT`, `VCB`.
- Do not run whole-market integration by default.

## Out of scope for V1

- Index, ETF, futures, bond, macro, news, and fundamentals ETL.
- Raw staging tables.
- Scheduler/cron/Airflow/Prefect.
- Intraday data.
- Multi-provider source comparison.
- Dashboarding or analytics.

## Recommended implementation phases

1. Package skeleton, config loading, logging, SQLAlchemy engine.
2. ORM models and Alembic migration for two tables.
3. Symbol extraction/transform/upsert.
4. OHLCV extraction/transform/upsert for one date.
5. CLI `daily` and `backfill`.
6. Rate-limit throttle and non-blocking retry queue.
7. Unit tests and optional live smoke test.
