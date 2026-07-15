# Vnstock TiDB ETL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python CLI ETL that loads listed-equity daily OHLCV data from `vnstock_data` into TiDB Cloud with idempotent upserts.

**Architecture:** Create a focused `etl/` package with config, DB, models, extractors, transforms, loaders, retry/rate-limit helpers, logging, and CLI orchestration. Keep v1 limited to `Reference().equity.list_by_exchange()` and `Market().equity(symbol).ohlcv(start=date, end=date, interval="1D")`. Persist only docs-defined fields plus ETL metadata.

**Tech Stack:** Python 3.10+, pandas, vnstock_data, SQLAlchemy 2.x, PyMySQL, python-dotenv, Alembic, pytest, argparse, standard logging.

## Global Constraints

- Use `vnstock_data` Unified UI because user tier is `silver`.
- Before finalizing API calls, inspect installed `vnstock_data` with `show_api()` and `show_doc()`.
- Reference symbol schema must follow `docs/vnstock-data/schema/01-reference.md`.
- OHLCV schema must follow `docs/vnstock-data/schema/02-market.md`.
- V1 asset universe is listed equities from `Reference().equity.list_by_exchange()`.
- V1 market data API is `Market().equity(symbol).ohlcv(start=date, end=date, interval="1D")`.
- Idempotency is `UNIQUE(symbol, trade_date)` with TiDB/MySQL `ON DUPLICATE KEY UPDATE`.
- OHLCV metadata columns: `source`, `created_at`, `updated_at`, `fetched_at`, `batch_id`.
- Log to console and `logs/etl_YYYYMMDD.log`.
- Respect rate limit of 300 requests/minute.
- Retry API network/dead-connect failures max 3 attempts.
- Retry delay is 5 minutes, and retry queue must not block processing of other symbols.
- Do not add raw staging, scheduler, intraday, index, ETF, futures, bond, macro, news, or fundamentals in V1.
- Existing repo is mostly docs; prefer creating focused new files instead of modifying docs/examples.
- Run GitNexus impact before editing existing functions/classes/methods. New files do not need symbol impact.

---

## File Structure

Create these files:

```text
pyproject.toml                         # Package metadata, dependencies, pytest config
etl/__init__.py                        # Package marker
etl/config.py                          # .env loading and typed ETL settings
etl/logging.py                         # Console + file logging setup
etl/db.py                              # SQLAlchemy engine/session helpers
etl/models.py                          # ORM models: EquitySymbol, EquityDailyOhlcv
etl/transforms.py                      # DataFrame schema validation and row mapping
etl/loaders.py                         # TiDB/MySQL upsert functions
etl/extractors.py                      # vnstock_data wrappers
etl/retry.py                           # Rate limiter and non-blocking retry queue
etl/cli.py                             # argparse CLI and orchestration
alembic.ini                            # Alembic config
alembic/env.py                         # Alembic migration runtime
alembic/versions/20260716_0001_create_etl_tables.py
logs/.gitkeep                          # Keep log directory in git
tests/test_config_logging.py
tests/test_models_loaders.py
tests/test_transforms.py
tests/test_retry.py
tests/test_cli.py
```

Do not modify `test.py` in this plan.

---

### T

ask 1: Project scaffold, settings, and logging

**Files:**
- Create: `pyproject.toml`
- Create: `etl/__init__.py`
- Create: `etl/config.py`
- Create: `etl/logging.py`
- Create: `logs/.gitkeep`
- Test: `tests/test_config_logging.py`

**Interfaces:**
- Produces: `etl.config.EtlSettings`, `etl.config.load_settings(env_file: str | Path | None = None) -> EtlSettings`
- Produces: `etl.logging.configure_logging(log_dir: Path = Path("logs"), run_date: date | None = None) -> logging.Logger`
- Later tasks consume settings fields: `tidb_url`, `source`, `requests_per_minute`, `retry_delay_seconds`, `max_attempts`, `batch_size`

- [ ] **Step 1: Write failing tests for settings and logging**

Create `tests/test_config_logging.py`:

```python
from __future__ import annotations

from datetime import date
from pathlib import Path

from etl.config import load_settings
from etl.logging import configure_logging


def test_load_settings_from_env_file(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TIDB_URL=mysql+pymysql://user:pass@example.com:4000/db\n"
        "ETL_BATCH_SIZE=25\n"
        "ETL_REQUESTS_PER_MINUTE=240\n"
        "ETL_RETRY_DELAY_SECONDS=300\n"
        "ETL_MAX_ATTEMPTS=3\n"
        "ETL_SOURCE=vnstock_data\n",
        encoding="utf-8",
    )

    settings = load_settings(env_file)

    assert settings.tidb_url == "mysql+pymysql://user:pass@example.com:4000/db"
    assert settings.batch_size == 25
    assert settings.requests_per_minute == 240
    assert settings.retry_delay_seconds == 300
    assert settings.max_attempts == 3
    assert settings.source == "vnstock_data"


def test_load_settings_requires_tidb_url(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("ETL_BATCH_SIZE=25\n", encoding="utf-8")

    try:
        load_settings(env_file)
    except ValueError as exc:
        assert "TIDB_URL" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_configure_logging_creates_dated_log_file(tmp_path: Path):
    logger = configure_logging(log_dir=tmp_path, run_date=date(2026, 7, 16))

    logger.info("hello")

    log_file = tmp_path / "etl_20260716.log"
    assert log_file.exists()
    assert "hello" in log_file.read_text(encoding="utf-8")
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_config_logging.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'etl'`.

- [ ] **Step 3: Add project metadata and package files**

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "vnstock-tidb-etl"
version = "0.1.0"
description = "ETL for vnstock_data listed-equity OHLCV into TiDB Cloud"
requires-python = ">=3.10"
dependencies = [
  "alembic>=1.13",
  "pandas>=2.0",
  "PyMySQL>=1.1",
  "python-dotenv>=1.0",
  "SQLAlchemy>=2.0",
]

[project.optional-dependencies]
dev = [
  "pytest>=8.0",
]

[tool.setuptools.packages.find]
include = ["etl*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

Create `etl/__init__.py`:

```python
"""Vnstock TiDB ETL package."""
```

Create `logs/.gitkeep` as an empty file.

- [ ] **Step 4: Implement settings loader**

Create `etl/config.py`:

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class EtlSettings:
    tidb_url: str
    source: str = "vnstock_data"
    requests_per_minute: int = 300
    retry_delay_seconds: int = 300
    max_attempts: int = 3
    batch_size: int = 100


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def load_settings(env_file: str | Path | None = None) -> EtlSettings:
    if env_file is not None:
        load_dotenv(Path(env_file), override=True)
    else:
        load_dotenv(override=False)

    tidb_url = os.getenv("TIDB_URL")
    if not tidb_url:
        raise ValueError("TIDB_URL is required")

    return EtlSettings(
        tidb_url=tidb_url,
        source=os.getenv("ETL_SOURCE", "vnstock_data"),
        requests_per_minute=_int_env("ETL_REQUESTS_PER_MINUTE", 300),
        retry_delay_seconds=_int_env("ETL_RETRY_DELAY_SECONDS", 300),
        max_attempts=_int_env("ETL_MAX_ATTEMPTS", 3),
        batch_size=_int_env("ETL_BATCH_SIZE", 100),
    )
```

- [ ] **Step 5: Implement logging setup**

Create `etl/logging.py`:

```python
from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path


def configure_logging(log_dir: Path = Path("logs"), run_date: date | None = None) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    active_date = run_date or date.today()
    log_file = log_dir / f"etl_{active_date:%Y%m%d}.log"

    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
```

- [ ] **Step 6: Run tests to verify pass**

Run:

```bash
pytest tests/test_config_logging.py -v
```

Expected: PASS all 3 tests.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml etl/__init__.py etl/config.py etl/logging.py logs/.gitkeep tests/test_config_logging.py
git commit -m "feat: add ETL config and logging scaffold"
```

---

### Task 2: ORM models, database helpers, and Alembic migration

**Files:**
- Create: `etl/db.py`
- Create: `etl/models.py`
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/versions/20260716_0001_create_etl_tables.py`
- Test: `tests/test_models_loaders.py`

**Interfaces:**
- Consumes: `EtlSettings.tidb_url`
- Produces: `etl.db.create_engine_from_url(url: str) -> Engine`
- Produces: `etl.db.session_factory(engine: Engine) -> sessionmaker[Session]`
- Produces: `etl.models.Base`, `EquitySymbol`, `EquityDailyOhlcv`
- Later tasks consume model table names: `equity_symbols`, `equity_daily_ohlcv`

- [ ] **Step 1: Write failing tests for model schema**

Create `tests/test_models_loaders.py` with initial schema tests:

```python
from __future__ import annotations

from etl.models import EquityDailyOhlcv, EquitySymbol


def test_equity_symbol_columns_match_reference_schema_plus_metadata():
    columns = EquitySymbol.__table__.columns

    assert set(columns.keys()) == {
        "symbol",
        "exchange",
        "organ_name",
        "organ_short_name",
        "icb_code_lv2",
        "source",
        "created_at",
        "updated_at",
        "fetched_at",
        "batch_id",
    }
    assert columns["symbol"].primary_key


def test_equity_daily_ohlcv_unique_symbol_trade_date():
    table = EquityDailyOhlcv.__table__
    unique_sets = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }

    assert ("symbol", "trade_date") in unique_sets


def test_equity_daily_ohlcv_columns_match_market_schema_plus_metadata():
    columns = EquityDailyOhlcv.__table__.columns

    assert set(columns.keys()) == {
        "id",
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "created_at",
        "updated_at",
        "fetched_at",
        "batch_id",
    }
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_models_loaders.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'etl.models'`.

- [ ] **Step 3: Implement ORM models**

Create `etl/models.py`:

```python
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import BigInteger


class Base(DeclarativeBase):
    pass


class EquitySymbol(Base):
    __tablename__ = "equity_symbols"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    exchange: Mapped[str | None] = mapped_column(String(32))
    organ_name: Mapped[str | None] = mapped_column(String(255))
    organ_short_name: Mapped[str | None] = mapped_column(String(255))
    icb_code_lv2: Mapped[str | None] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    batch_id: Mapped[str] = mapped_column(String(64), nullable=False)


class EquityDailyOhlcv(Base):
    __tablename__ = "equity_daily_ohlcv"
    __table_args__ = (UniqueConstraint("symbol", "trade_date", name="uq_equity_daily_ohlcv_symbol_trade_date"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), ForeignKey("equity_symbols.symbol"), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    open: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    high: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    low: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    close: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    volume: Mapped[int | None] = mapped_column(BigInteger)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    batch_id: Mapped[str] = mapped_column(String(64), nullable=False)
```

- [ ] **Step 4: Implement DB helpers**

Create `etl/db.py`:

```python
from __future__ import annotations

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


def create_engine_from_url(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, future=True)


def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, future=True)
```

- [ ] **Step 5: Add Alembic files**

Create `alembic.ini`:

```ini
[alembic]
script_location = alembic
prepend_sys_path = .

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

Create `alembic/env.py`:

```python
from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from etl.config import load_settings
from etl.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    return load_settings().tidb_url


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(configuration, prefix="sqlalchemy.", poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

Create `alembic/versions/20260716_0001_create_etl_tables.py`:

```python
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260716_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "equity_symbols",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=True),
        sa.Column("organ_name", sa.String(length=255), nullable=True),
        sa.Column("organ_short_name", sa.String(length=255), nullable=True),
        sa.Column("icb_code_lv2", sa.String(length=32), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(), nullable=False),
        sa.Column("batch_id", sa.String(length=64), nullable=False),
        sa.PrimaryKeyConstraint("symbol"),
    )
    op.create_table(
        "equity_daily_ohlcv",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("open", sa.Numeric(18, 4), nullable=True),
        sa.Column("high", sa.Numeric(18, 4), nullable=True),
        sa.Column("low", sa.Numeric(18, 4), nullable=True),
        sa.Column("close", sa.Numeric(18, 4), nullable=True),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(), nullable=False),
        sa.Column("batch_id", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(["symbol"], ["equity_symbols.symbol"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "trade_date", name="uq_equity_daily_ohlcv_symbol_trade_date"),
    )


def downgrade() -> None:
    op.drop_table("equity_daily_ohlcv")
    op.drop_table("equity_symbols")
```

- [ ] **Step 6: Run tests to verify pass**

Run:

```bash
pytest tests/test_models_loaders.py -v
```

Expected: PASS all 3 tests.

- [ ] **Step 7: Commit**

```bash
git add etl/db.py etl/models.py alembic.ini alembic/env.py alembic/versions/20260716_0001_create_etl_tables.py tests/test_models_loaders.py
git commit -m "feat: add ETL database models"
```

---

### Task 3: DataFrame transforms for reference and OHLCV schemas

**Files:**
- Create: `etl/transforms.py`
- Modify: `tests/test_transforms.py`

**Interfaces:**
- Consumes docs schema columns from Task 2 models.
- Produces: `SchemaError(ValueError)`
- Produces: `transform_symbols(df: pandas.DataFrame, *, source: str, fetched_at: datetime, batch_id: str) -> list[dict[str, object]]`
- Produces: `transform_ohlcv(symbol: str, df: pandas.DataFrame, *, source: str, fetched_at: datetime, batch_id: str) -> list[dict[str, object]]`
- Later tasks pass these row dicts to loader functions.

- [ ] **Step 1: Write failing transform tests**

Create `tests/test_transforms.py`:

```python
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pandas as pd

from etl.transforms import SchemaError, transform_ohlcv, transform_symbols


def test_transform_symbols_maps_reference_docs_schema():
    fetched_at = datetime(2026, 7, 16, 9, 0, 0)
    df = pd.DataFrame(
        [
            {
                "symbol": "FPT",
                "exchange": "HOSE",
                "organ_name": "Công ty Cổ phần FPT",
                "organ_short_name": "FPT Corp",
                "icb_code_lv2": "9500",
            }
        ]
    )

    rows = transform_symbols(df, source="vnstock_data", fetched_at=fetched_at, batch_id="batch-1")

    assert rows == [
        {
            "symbol": "FPT",
            "exchange": "HOSE",
            "organ_name": "Công ty Cổ phần FPT",
            "organ_short_name": "FPT Corp",
            "icb_code_lv2": "9500",
            "source": "vnstock_data",
            "created_at": fetched_at,
            "updated_at": fetched_at,
            "fetched_at": fetched_at,
            "batch_id": "batch-1",
        }
    ]


def test_transform_ohlcv_maps_market_docs_schema():
    fetched_at = datetime(2026, 7, 16, 9, 0, 0)
    df = pd.DataFrame(
        [
            {
                "time": pd.Timestamp("2026-07-14T07:00:00"),
                "open": 29.58,
                "high": 30.87,
                "low": 29.53,
                "close": 30.82,
                "volume": 23148300,
            }
        ]
    )

    rows = transform_ohlcv("FPT", df, source="vnstock_data", fetched_at=fetched_at, batch_id="batch-1")

    assert rows[0]["symbol"] == "FPT"
    assert rows[0]["trade_date"].isoformat() == "2026-07-14"
    assert rows[0]["open"] == Decimal("29.5800")
    assert rows[0]["high"] == Decimal("30.8700")
    assert rows[0]["low"] == Decimal("29.5300")
    assert rows[0]["close"] == Decimal("30.8200")
    assert rows[0]["volume"] == 23148300
    assert rows[0]["created_at"] == fetched_at
    assert rows[0]["updated_at"] == fetched_at
    assert rows[0]["fetched_at"] == fetched_at
    assert rows[0]["batch_id"] == "batch-1"


def test_transform_ohlcv_empty_dataframe_returns_empty_list():
    fetched_at = datetime(2026, 7, 16, 9, 0, 0)
    df = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    rows = transform_ohlcv("FPT", df, source="vnstock_data", fetched_at=fetched_at, batch_id="batch-1")

    assert rows == []


def test_transform_ohlcv_missing_column_raises_schema_error():
    fetched_at = datetime(2026, 7, 16, 9, 0, 0)
    df = pd.DataFrame([{"time": pd.Timestamp("2026-07-14"), "open": 1}])

    try:
        transform_ohlcv("FPT", df, source="vnstock_data", fetched_at=fetched_at, batch_id="batch-1")
    except SchemaError as exc:
        assert "high" in str(exc)
        assert "volume" in str(exc)
    else:
        raise AssertionError("expected SchemaError")
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_transforms.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'etl.transforms'`.

- [ ] **Step 3: Implement transforms**

Create `etl/transforms.py`:

```python
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable

import pandas as pd


class SchemaError(ValueError):
    pass


SYMBOL_COLUMNS = {"symbol", "exchange", "organ_name", "organ_short_name", "icb_code_lv2"}
OHLCV_COLUMNS = {"time", "open", "high", "low", "close", "volume"}


def _require_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise SchemaError(f"missing required columns: {', '.join(missing)}")


def _none_if_nan(value: object) -> object | None:
    if pd.isna(value):
        return None
    return value


def _str_or_none(value: object) -> str | None:
    value = _none_if_nan(value)
    if value is None:
        return None
    return str(value)


def _decimal_or_none(value: object) -> Decimal | None:
    value = _none_if_nan(value)
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _int_or_none(value: object) -> int | None:
    value = _none_if_nan(value)
    if value is None:
        return None
    return int(value)


def transform_symbols(df: pd.DataFrame, *, source: str, fetched_at: datetime, batch_id: str) -> list[dict[str, object]]:
    _require_columns(df, {"symbol"})
    rows: list[dict[str, object]] = []

    for record in df.to_dict(orient="records"):
        symbol = _str_or_none(record.get("symbol"))
        if not symbol:
            raise SchemaError("symbol is required")
        rows.append(
            {
                "symbol": symbol,
                "exchange": _str_or_none(record.get("exchange")),
                "organ_name": _str_or_none(record.get("organ_name")),
                "organ_short_name": _str_or_none(record.get("organ_short_name")),
                "icb_code_lv2": _str_or_none(record.get("icb_code_lv2")),
                "source": source,
                "created_at": fetched_at,
                "updated_at": fetched_at,
                "fetched_at": fetched_at,
                "batch_id": batch_id,
            }
        )
    return rows


def transform_ohlcv(symbol: str, df: pd.DataFrame, *, source: str, fetched_at: datetime, batch_id: str) -> list[dict[str, object]]:
    _require_columns(df, OHLCV_COLUMNS)
    if df.empty:
        return []

    rows: list[dict[str, object]] = []
    for record in df.to_dict(orient="records"):
        trade_time = pd.Timestamp(record["time"])
        rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_time.date(),
                "open": _decimal_or_none(record.get("open")),
                "high": _decimal_or_none(record.get("high")),
                "low": _decimal_or_none(record.get("low")),
                "close": _decimal_or_none(record.get("close")),
                "volume": _int_or_none(record.get("volume")),
                "source": source,
                "created_at": fetched_at,
                "updated_at": fetched_at,
                "fetched_at": fetched_at,
                "batch_id": batch_id,
            }
        )
    return rows
```

- [ ] **Step 4: Run transform tests**

Run:

```bash
pytest tests/test_transforms.py -v
```

Expected: PASS all 4 tests.

- [ ] **Step 5: Commit**

```bash
git add etl/transforms.py tests/test_transforms.py
git commit -m "feat: add ETL dataframe transforms"
```

---

### Task 4: TiDB upsert loaders

**Files:**
- Create: `etl/loaders.py`
- Modify: `tests/test_models_loaders.py`

**Interfaces:**
- Consumes row dicts from `transform_symbols` and `transform_ohlcv`.
- Produces: `upsert_symbols(session: Session, rows: Sequence[dict[str, object]]) -> int`
- Produces: `upsert_ohlcv(session: Session, rows: Sequence[dict[str, object]]) -> int`

- [ ] **Step 1: Add failing loader tests**

Append to `tests/test_models_loaders.py`:

```python
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from etl.loaders import upsert_ohlcv, upsert_symbols
from etl.models import Base, EquityDailyOhlcv, EquitySymbol


def test_upsert_symbols_inserts_and_updates_without_changing_created_at():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    created_at = datetime(2026, 7, 16, 9, 0, 0)
    updated_at = datetime(2026, 7, 16, 10, 0, 0)

    with Session.begin() as session:
        assert upsert_symbols(
            session,
            [
                {
                    "symbol": "FPT",
                    "exchange": "HOSE",
                    "organ_name": "old",
                    "organ_short_name": "old short",
                    "icb_code_lv2": "9500",
                    "source": "vnstock_data",
                    "created_at": created_at,
                    "updated_at": created_at,
                    "fetched_at": created_at,
                    "batch_id": "batch-1",
                }
            ],
        ) == 1

    with Session.begin() as session:
        assert upsert_symbols(
            session,
            [
                {
                    "symbol": "FPT",
                    "exchange": "HOSE",
                    "organ_name": "new",
                    "organ_short_name": "new short",
                    "icb_code_lv2": "9500",
                    "source": "vnstock_data",
                    "created_at": updated_at,
                    "updated_at": updated_at,
                    "fetched_at": updated_at,
                    "batch_id": "batch-2",
                }
            ],
        ) == 1
        row = session.execute(select(EquitySymbol).where(EquitySymbol.symbol == "FPT")).scalar_one()

    assert row.organ_name == "new"
    assert row.created_at == created_at
    assert row.updated_at == updated_at
    assert row.batch_id == "batch-2"


def test_upsert_ohlcv_inserts_and_updates_without_changing_created_at():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    created_at = datetime(2026, 7, 16, 9, 0, 0)
    updated_at = datetime(2026, 7, 16, 10, 0, 0)

    with Session.begin() as session:
        upsert_symbols(
            session,
            [
                {
                    "symbol": "FPT",
                    "exchange": "HOSE",
                    "organ_name": "FPT",
                    "organ_short_name": "FPT",
                    "icb_code_lv2": "9500",
                    "source": "vnstock_data",
                    "created_at": created_at,
                    "updated_at": created_at,
                    "fetched_at": created_at,
                    "batch_id": "batch-1",
                }
            ],
        )
        assert upsert_ohlcv(
            session,
            [
                {
                    "symbol": "FPT",
                    "trade_date": date(2026, 7, 14),
                    "open": Decimal("1.0000"),
                    "high": Decimal("2.0000"),
                    "low": Decimal("0.5000"),
                    "close": Decimal("1.5000"),
                    "volume": 100,
                    "source": "vnstock_data",
                    "created_at": created_at,
                    "updated_at": created_at,
                    "fetched_at": created_at,
                    "batch_id": "batch-1",
                }
            ],
        ) == 1

    with Session.begin() as session:
        assert upsert_ohlcv(
            session,
            [
                {
                    "symbol": "FPT",
                    "trade_date": date(2026, 7, 14),
                    "open": Decimal("1.1000"),
                    "high": Decimal("2.1000"),
                    "low": Decimal("0.6000"),
                    "close": Decimal("1.6000"),
                    "volume": 200,
                    "source": "vnstock_data",
                    "created_at": updated_at,
                    "updated_at": updated_at,
                    "fetched_at": updated_at,
                    "batch_id": "batch-2",
                }
            ],
        ) == 1
        row = session.execute(select(EquityDailyOhlcv).where(EquityDailyOhlcv.symbol == "FPT")).scalar_one()

    assert row.close == Decimal("1.6000")
    assert row.volume == 200
    assert row.created_at == created_at
    assert row.updated_at == updated_at
    assert row.batch_id == "batch-2"
```

- [ ] **Step 2: Run loader tests to verify failure**

Run:

```bash
pytest tests/test_models_loaders.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'etl.loaders'`.

- [ ] **Step 3: Implement cross-dialect upsert loaders**

Create `etl/loaders.py`:

```python
from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from etl.models import EquityDailyOhlcv, EquitySymbol


def _insert_for_session(session: Session, table):
    dialect_name = session.get_bind().dialect.name
    if dialect_name == "sqlite":
        return sqlite_insert(table)
    return mysql_insert(table)


def _incoming_values(stmt, dialect_name: str, column_names: Sequence[str]) -> dict[str, object]:
    source = stmt.excluded if dialect_name == "sqlite" else stmt.inserted
    return {name: getattr(source, name) for name in column_names}


def upsert_symbols(session: Session, rows: Sequence[dict[str, object]]) -> int:
    if not rows:
        return 0

    dialect_name = session.get_bind().dialect.name
    table = EquitySymbol.__table__
    stmt = _insert_for_session(session, table).values(list(rows))
    update_columns = _incoming_values(
        stmt,
        dialect_name,
        ["exchange", "organ_name", "organ_short_name", "icb_code_lv2", "source", "updated_at", "fetched_at", "batch_id"],
    )
    if dialect_name == "sqlite":
        stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_columns)
    else:
        stmt = stmt.on_duplicate_key_update(**update_columns)

    session.execute(stmt)
    return len(rows)


def upsert_ohlcv(session: Session, rows: Sequence[dict[str, object]]) -> int:
    if not rows:
        return 0

    dialect_name = session.get_bind().dialect.name
    table = EquityDailyOhlcv.__table__
    stmt = _insert_for_session(session, table).values(list(rows))
    update_columns = _incoming_values(
        stmt,
        dialect_name,
        ["open", "high", "low", "close", "volume", "source", "updated_at", "fetched_at", "batch_id"],
    )
    if dialect_name == "sqlite":
        stmt = stmt.on_conflict_do_update(index_elements=["symbol", "trade_date"], set_=update_columns)
    else:
        stmt = stmt.on_duplicate_key_update(**update_columns)

    session.execute(stmt)
    return len(rows)
```

- [ ] **Step 4: Run model/loader tests**

Run:

```bash
pytest tests/test_models_loaders.py -v
```

Expected: PASS all tests.

- [ ] **Step 5: Commit**

```bash
git add etl/loaders.py tests/test_models_loaders.py
git commit -m "feat: add TiDB upsert loaders"
```

---

### Task 5: vnstock_data extractors with API discovery hook

**Files:**
- Create: `etl/extractors.py`
- Test: `tests/test_extractors.py`

**Interfaces:**
- Produces: `VnstockExtractor.inspect_api() -> None`
- Produces: `VnstockExtractor.fetch_symbols() -> pandas.DataFrame`
- Produces: `VnstockExtractor.fetch_ohlcv(symbol: str, trade_date: date) -> pandas.DataFrame`
- Later CLI consumes these methods.

- [ ] **Step 1: Write failing extractor tests with fakes**

Create `tests/test_extractors.py`:

```python
from __future__ import annotations

from datetime import date

import pandas as pd

from etl.extractors import VnstockExtractor


class FakeEquityReference:
    def list_by_exchange(self):
        return pd.DataFrame([{"symbol": "FPT", "exchange": "HOSE"}])


class FakeReference:
    def __init__(self):
        self.equity = FakeEquityReference()


class FakeEquityMarket:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.calls = []

    def ohlcv(self, *, start: str, end: str, interval: str):
        self.calls.append((start, end, interval))
        return pd.DataFrame([{"time": pd.Timestamp(start), "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100}])


class FakeMarket:
    def __init__(self):
        self.last_equity = None

    def equity(self, symbol: str):
        self.last_equity = FakeEquityMarket(symbol)
        return self.last_equity


def test_fetch_symbols_uses_list_by_exchange():
    extractor = VnstockExtractor(reference=FakeReference(), market=FakeMarket())

    df = extractor.fetch_symbols()

    assert list(df["symbol"]) == ["FPT"]


def test_fetch_ohlcv_uses_one_day_interval_1d():
    fake_market = FakeMarket()
    extractor = VnstockExtractor(reference=FakeReference(), market=fake_market)

    df = extractor.fetch_ohlcv("FPT", date(2026, 7, 14))

    assert len(df) == 1
    assert fake_market.last_equity.calls == [("2026-07-14", "2026-07-14", "1D")]
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_extractors.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'etl.extractors'`.

- [ ] **Step 3: Implement extractors**

Create `etl/extractors.py`:

```python
from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd


class VnstockExtractor:
    def __init__(self, reference: Any | None = None, market: Any | None = None):
        if reference is None or market is None:
            from vnstock_data import Market, Reference

            reference = reference or Reference()
            market = market or Market()
        self.reference = reference
        self.market = market

    def inspect_api(self) -> None:
        from vnstock_data import show_api, show_doc

        show_api()
        show_doc("Reference.equity")
        show_doc("Market.equity")

    def fetch_symbols(self) -> pd.DataFrame:
        return self.reference.equity.list_by_exchange()

    def fetch_ohlcv(self, symbol: str, trade_date: date) -> pd.DataFrame:
        day = trade_date.isoformat()
        return self.market.equity(symbol).ohlcv(start=day, end=day, interval="1D")
```

- [ ] **Step 4: Run extractor tests**

Run:

```bash
pytest tests/test_extractors.py -v
```

Expected: PASS all tests.

- [ ] **Step 5: Commit**

```bash
git add etl/extractors.py tests/test_extractors.py
git commit -m "feat: add vnstock_data extractors"
```

---

### Task 6: Rate limiter and non-blocking retry queue

**Files:**
- Create: `etl/retry.py`
- Test: `tests/test_retry.py`

**Interfaces:**
- Produces: `RateLimiter(requests_per_minute: int, monotonic: Callable[[], float] = time.monotonic, sleeper: Callable[[float], None] = time.sleep)`
- Produces: `RateLimiter.wait_for_slot() -> None`
- Produces: `RetryItem(symbol: str, trade_date: date, attempts: int, next_retry_at: datetime)`
- Produces: `RetryQueue(delay_seconds: int, max_attempts: int, now: Callable[[], datetime] | None = None)`
- Produces: `RetryQueue.schedule(symbol: str, trade_date: date, attempts: int) -> None`
- Produces: `RetryQueue.ready() -> list[RetryItem]`
- Produces: `RetryQueue.has_pending() -> bool`
- CLI uses queue to avoid blocking healthy symbols.

- [ ] **Step 1: Write failing retry tests**

Create `tests/test_retry.py`:

```python
from __future__ import annotations

from datetime import date, datetime, timedelta

from etl.retry import RateLimiter, RetryQueue


def test_rate_limiter_sleeps_when_rate_exceeded():
    times = iter([0.0, 0.0, 0.1])
    sleeps = []
    limiter = RateLimiter(requests_per_minute=60, monotonic=lambda: next(times), sleeper=sleeps.append)

    limiter.wait_for_slot()
    limiter.wait_for_slot()

    assert sleeps == [0.9]


def test_retry_queue_returns_only_ready_items_without_blocking_others():
    current = datetime(2026, 7, 16, 9, 0, 0)

    def now():
        return current

    queue = RetryQueue(delay_seconds=300, max_attempts=3, now=now)
    queue.schedule("FPT", date(2026, 7, 14), attempts=1)

    assert queue.ready() == []

    current = current + timedelta(minutes=5)
    ready = queue.ready()

    assert len(ready) == 1
    assert ready[0].symbol == "FPT"
    assert ready[0].attempts == 1
    assert not queue.has_pending()


def test_retry_queue_drops_items_at_max_attempts():
    queue = RetryQueue(delay_seconds=300, max_attempts=3, now=lambda: datetime(2026, 7, 16, 9, 0, 0))

    queue.schedule("FPT", date(2026, 7, 14), attempts=3)

    assert queue.ready() == []
    assert not queue.has_pending()
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_retry.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'etl.retry'`.

- [ ] **Step 3: Implement retry helpers**

Create `etl/retry.py`:

```python
from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta


class RateLimiter:
    def __init__(
        self,
        requests_per_minute: int,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        self.interval_seconds = 60.0 / requests_per_minute
        self.monotonic = monotonic
        self.sleeper = sleeper
        self._last_request_at: float | None = None

    def wait_for_slot(self) -> None:
        now = self.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            wait_seconds = self.interval_seconds - elapsed
            if wait_seconds > 0:
                self.sleeper(round(wait_seconds, 10))
                now = self.monotonic()
        self._last_request_at = now


@dataclass(frozen=True)
class RetryItem:
    symbol: str
    trade_date: date
    attempts: int
    next_retry_at: datetime


class RetryQueue:
    def __init__(self, delay_seconds: int, max_attempts: int, now: Callable[[], datetime] | None = None):
        if delay_seconds <= 0:
            raise ValueError("delay_seconds must be positive")
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        self.delay_seconds = delay_seconds
        self.max_attempts = max_attempts
        self.now = now or datetime.now
        self._items: deque[RetryItem] = deque()

    def schedule(self, symbol: str, trade_date: date, attempts: int) -> None:
        if attempts >= self.max_attempts:
            return
        self._items.append(
            RetryItem(
                symbol=symbol,
                trade_date=trade_date,
                attempts=attempts,
                next_retry_at=self.now() + timedelta(seconds=self.delay_seconds),
            )
        )

    def ready(self) -> list[RetryItem]:
        current = self.now()
        ready_items: list[RetryItem] = []
        waiting_items: deque[RetryItem] = deque()
        while self._items:
            item = self._items.popleft()
            if item.next_retry_at <= current:
                ready_items.append(item)
            else:
                waiting_items.append(item)
        self._items = waiting_items
        return ready_items

    def has_pending(self) -> bool:
        return bool(self._items)

    def seconds_until_next_ready(self) -> float:
        if not self._items:
            return 0.0
        current = self.now()
        return max(0.0, min((item.next_retry_at - current).total_seconds() for item in self._items))
```

- [ ] **Step 4: Run retry tests**

Run:

```bash
pytest tests/test_retry.py -v
```

Expected: PASS all tests.

- [ ] **Step 5: Commit**

```bash
git add etl/retry.py tests/test_retry.py
git commit -m "feat: add ETL retry and rate limiting"
```

---

### Task 7: CLI orchestration for daily and backfill runs

**Files:**
- Create: `etl/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes all previous task interfaces.
- Produces: `parse_date(value: str) -> date`
- Produces: `date_range(start: date, end: date) -> list[date]`
- Produces: `RunStats` dataclass
- Produces: `run_etl(...dependencies...) -> RunStats`
- Produces CLI entrypoint `main(argv: Sequence[str] | None = None) -> int`

- [ ] **Step 1: Write failing CLI tests**

Create `tests/test_cli.py`:

```python
from __future__ import annotations

from datetime import date

import pandas as pd

from etl.cli import date_range, main, parse_date, run_etl
from etl.config import EtlSettings


class FakeExtractor:
    def __init__(self):
        self.calls = []

    def fetch_symbols(self):
        return pd.DataFrame(
            [
                {"symbol": "FPT", "exchange": "HOSE", "organ_name": "FPT", "organ_short_name": "FPT", "icb_code_lv2": "9500"},
                {"symbol": "VCB", "exchange": "HOSE", "organ_name": "VCB", "organ_short_name": "VCB", "icb_code_lv2": "8300"},
            ]
        )

    def fetch_ohlcv(self, symbol, trade_date):
        self.calls.append((symbol, trade_date))
        if symbol == "VCB":
            return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
        return pd.DataFrame(
            [{"time": pd.Timestamp(trade_date), "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100}]
        )


class FakeSessionContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeSessionFactory:
    def begin(self):
        return FakeSessionContext()


class FakeLimiter:
    def __init__(self):
        self.calls = 0

    def wait_for_slot(self):
        self.calls += 1


def test_parse_date_accepts_iso_date():
    assert parse_date("2026-07-14") == date(2026, 7, 14)


def test_parse_date_rejects_invalid_date():
    try:
        parse_date("2026/07/14")
    except ValueError as exc:
        assert "YYYY-MM-DD" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_date_range_is_inclusive():
    assert date_range(date(2026, 7, 14), date(2026, 7, 16)) == [
        date(2026, 7, 14),
        date(2026, 7, 15),
        date(2026, 7, 16),
    ]


def test_run_etl_processes_symbols_and_counts_no_data():
    extractor = FakeExtractor()
    limiter = FakeLimiter()
    symbol_rows = []
    ohlcv_rows = []

    stats = run_etl(
        dates=[date(2026, 7, 14)],
        settings=EtlSettings(tidb_url="sqlite://"),
        extractor=extractor,
        session_factory=FakeSessionFactory(),
        rate_limiter=limiter,
        upsert_symbols_fn=lambda session, rows: symbol_rows.extend(rows) or len(rows),
        upsert_ohlcv_fn=lambda session, rows: ohlcv_rows.extend(rows) or len(rows),
        logger=None,
        batch_id="batch-1",
    )

    assert stats.symbols_total == 2
    assert stats.success == 1
    assert stats.no_data == 1
    assert stats.failed == 0
    assert stats.rows_upserted == 1
    assert extractor.calls == [("FPT", date(2026, 7, 14)), ("VCB", date(2026, 7, 14))]
    assert limiter.calls == 2
    assert len(symbol_rows) == 2
    assert len(ohlcv_rows) == 1


def test_main_rejects_invalid_daily_date():
    assert main(["daily", "--date", "2026/07/14"]) == 2
```

- [ ] **Step 2: Run CLI tests to verify failure**

Run:

```bash
pytest tests/test_cli.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'etl.cli'`.

- [ ] **Step 3: Implement CLI orchestration**

Create `etl/cli.py`:

```python
from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from sqlalchemy.orm import Session, sessionmaker

from etl.config import EtlSettings, load_settings
from etl.db import create_engine_from_url, session_factory as build_session_factory
from etl.extractors import VnstockExtractor
from etl.loaders import upsert_ohlcv, upsert_symbols
from etl.logging import configure_logging
from etl.retry import RateLimiter, RetryQueue
from etl.transforms import SchemaError, transform_ohlcv, transform_symbols


@dataclass
class RunStats:
    symbols_total: int = 0
    success: int = 0
    no_data: int = 0
    failed: int = 0
    retried: int = 0
    rows_upserted: int = 0


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"date must use YYYY-MM-DD format: {value}") from exc


def date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end date must be on or after start date")
    days = (end - start).days
    return [start + timedelta(days=offset) for offset in range(days + 1)]


def _log(logger: logging.Logger | None, message: str) -> None:
    if logger is not None:
        logger.info(message)


def _process_symbol_date(
    *,
    symbol: str,
    trade_date: date,
    settings: EtlSettings,
    extractor: VnstockExtractor,
    session_factory: sessionmaker[Session],
    rate_limiter: RateLimiter,
    upsert_ohlcv_fn: Callable[[Session, Sequence[dict[str, object]]], int],
    logger: logging.Logger | None,
    batch_id: str,
    attempt: int,
    stats: RunStats,
) -> bool:
    fetched_at = datetime.now()
    try:
        rate_limiter.wait_for_slot()
        df = extractor.fetch_ohlcv(symbol, trade_date)
        rows = transform_ohlcv(symbol, df, source=settings.source, fetched_at=fetched_at, batch_id=batch_id)
        if not rows:
            stats.no_data += 1
            _log(logger, f"batch_id={batch_id} date={trade_date} symbol={symbol} status=no_data attempt={attempt} rows=0")
            return True
        with session_factory.begin() as session:
            count = upsert_ohlcv_fn(session, rows)
        stats.success += 1
        stats.rows_upserted += count
        _log(logger, f"batch_id={batch_id} date={trade_date} symbol={symbol} status=success attempt={attempt} rows={count}")
        return True
    except SchemaError as exc:
        stats.failed += 1
        _log(logger, f"batch_id={batch_id} date={trade_date} symbol={symbol} status=failed attempt={attempt} error={exc}")
        return True
    except Exception as exc:
        _log(logger, f"batch_id={batch_id} date={trade_date} symbol={symbol} status=queued_retry attempt={attempt} error={exc}")
        return False


def run_etl(
    *,
    dates: Sequence[date],
    settings: EtlSettings,
    extractor: VnstockExtractor,
    session_factory: sessionmaker[Session],
    rate_limiter: RateLimiter,
    upsert_symbols_fn: Callable[[Session, Sequence[dict[str, object]]], int] = upsert_symbols,
    upsert_ohlcv_fn: Callable[[Session, Sequence[dict[str, object]]], int] = upsert_ohlcv,
    logger: logging.Logger | None,
    batch_id: str,
) -> RunStats:
    stats = RunStats()
    fetched_at = datetime.now()
    symbols_df = extractor.fetch_symbols()
    symbol_rows = transform_symbols(symbols_df, source=settings.source, fetched_at=fetched_at, batch_id=batch_id)
    stats.symbols_total = len(symbol_rows)
    symbols = [str(row["symbol"]) for row in symbol_rows]

    with session_factory.begin() as session:
        upsert_symbols_fn(session, symbol_rows)

    retry_queue = RetryQueue(delay_seconds=settings.retry_delay_seconds, max_attempts=settings.max_attempts)

    for trade_date in dates:
        for symbol in symbols:
            completed = _process_symbol_date(
                symbol=symbol,
                trade_date=trade_date,
                settings=settings,
                extractor=extractor,
                session_factory=session_factory,
                rate_limiter=rate_limiter,
                upsert_ohlcv_fn=upsert_ohlcv_fn,
                logger=logger,
                batch_id=batch_id,
                attempt=1,
                stats=stats,
            )
            if not completed:
                retry_queue.schedule(symbol, trade_date, attempts=1)

    while retry_queue.has_pending():
        ready_items = retry_queue.ready()
        if not ready_items:
            time.sleep(retry_queue.seconds_until_next_ready())
            continue
        for item in ready_items:
            stats.retried += 1
            completed = _process_symbol_date(
                symbol=item.symbol,
                trade_date=item.trade_date,
                settings=settings,
                extractor=extractor,
                session_factory=session_factory,
                rate_limiter=rate_limiter,
                upsert_ohlcv_fn=upsert_ohlcv_fn,
                logger=logger,
                batch_id=batch_id,
                attempt=item.attempts + 1,
                stats=stats,
            )
            if not completed:
                if item.attempts + 1 >= settings.max_attempts:
                    stats.failed += 1
                    _log(logger, f"batch_id={batch_id} date={item.trade_date} symbol={item.symbol} status=retry_failed attempt={item.attempts + 1}")
                else:
                    retry_queue.schedule(item.symbol, item.trade_date, attempts=item.attempts + 1)

    return stats


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl")
    subparsers = parser.add_subparsers(dest="command", required=True)

    daily = subparsers.add_parser("daily")
    daily.add_argument("--date", required=True)

    backfill = subparsers.add_parser("backfill")
    backfill.add_argument("--start", required=True)
    backfill.add_argument("--end", required=True)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "daily":
            dates = [parse_date(args.date)]
        else:
            dates = date_range(parse_date(args.start), parse_date(args.end))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    settings = load_settings()
    logger = configure_logging(run_date=dates[0])
    engine = create_engine_from_url(settings.tidb_url)
    sessions = build_session_factory(engine)
    extractor = VnstockExtractor()
    extractor.inspect_api()
    limiter = RateLimiter(settings.requests_per_minute)
    batch_id = uuid.uuid4().hex

    stats = run_etl(
        dates=dates,
        settings=settings,
        extractor=extractor,
        session_factory=sessions,
        rate_limiter=limiter,
        logger=logger,
        batch_id=batch_id,
    )
    logger.info(
        "batch_id=%s status=summary symbols_total=%s success=%s no_data=%s failed=%s retried=%s rows_upserted=%s",
        batch_id,
        stats.symbols_total,
        stats.success,
        stats.no_data,
        stats.failed,
        stats.retried,
        stats.rows_upserted,
    )
    return 1 if stats.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run CLI tests**

Run:

```bash
pytest tests/test_cli.py -v
```

Expected: PASS all tests.

- [ ] **Step 5: Run full unit suite**

Run:

```bash
pytest -v
```

Expected: PASS all tests.

- [ ] **Step 6: Commit**

```bash
git add etl/cli.py tests/test_cli.py
git commit -m "feat: add ETL CLI orchestration"
```

---

### Task 8: Verification, docs alignment, and live smoke command

**Files:**
- Modify: `docs/superpowers/specs/2026-07-16-vnstock-tidb-etl-design.md` only if implementation forces a spec correction.
- No required code files.

**Interfaces:**
- Consumes completed CLI and tests.
- Produces verified local status and optional live smoke result.

- [ ] **Step 1: Run static schema search against docs references**

Run:

```bash
grep -n "Reference().equity.list_by_exchange\|Market().equity('TCB').ohlcv" docs/vnstock-data/schema/01-reference.md docs/vnstock-data/schema/02-market.md
```

Expected: output includes:

```text
docs/vnstock-data/schema/01-reference.md:128:### `Reference().equity.list_by_exchange()`
docs/vnstock-data/schema/02-market.md:71:### `Market().equity('TCB').ohlcv(length='1Y')`
```

- [ ] **Step 2: Run full unit tests**

Run:

```bash
pytest -v
```

Expected: PASS all tests.

- [ ] **Step 3: Run Alembic offline SQL render**

Run:

```bash
alembic upgrade head --sql
```

Expected: SQL includes `CREATE TABLE equity_symbols` and `CREATE TABLE equity_daily_ohlcv`.

If this fails because `TIDB_URL` is not set in the environment, run with a non-secret local value:

```bash
TIDB_URL=mysql+pymysql://user:pass@localhost:4000/test alembic upgrade head --sql
```

Expected: SQL render succeeds without connecting to TiDB.

- [ ] **Step 4: Optional live migration against TiDB Cloud**

Only run if user confirms `.env` contains valid TiDB Cloud config and wants DB schema created.

Run:

```bash
alembic upgrade head
```

Expected: migration completes without errors.

- [ ] **Step 5: Optional live ETL smoke for one day and real symbols**

Only run after live migration succeeds.

Run:

```bash
python -m etl.cli daily --date 2026-07-14
```

Expected:

```text
status=summary ... failed=0 ...
```

Also verify `logs/etl_20260714.log` exists and contains per-symbol rows.

- [ ] **Step 6: Run GitNexus detect changes before final commit/PR**

Run MCP tool equivalent:

```text
detect_changes(scope="all", repo="trading-bot")
```

Expected: changed symbols limited to new ETL package, tests, Alembic files, and docs/spec if adjusted.

- [ ] **Step 7: Commit any verification-only fixes**

If Step 1-6 required fixes, inspect changed files:

```bash
git status --short
```

Stage only files changed by verification fixes. For example, if only CLI and CLI tests changed:

```bash
git add etl/cli.py tests/test_cli.py
git commit -m "test: verify vnstock TiDB ETL"
```

If no files changed, skip commit.

---

## Self-Review

Spec coverage:

- Listed equities via `Reference().equity.list_by_exchange()` covered in Tasks 3, 5, 7.
- Daily OHLCV via `Market().equity(symbol).ohlcv(start=date, end=date, interval="1D")` covered in Tasks 3, 5, 7.
- SQLAlchemy ORM + TiDB/MySQL upsert covered in Tasks 2 and 4.
- `.env` config covered in Task 1.
- CLI daily/backfill covered in Task 7.
- Metadata columns covered in Tasks 2, 3, 4.
- Console/file logging covered in Tasks 1 and 7.
- 300 requests/minute throttle covered in Tasks 6 and 7.
- Non-blocking retry queue with 5-minute delay and 3 attempts covered in Tasks 6 and 7.
- Tests and optional smoke covered across Tasks 1-8.

Placeholder scan:

- No placeholder red flags remain.
- Optional live DB steps require user confirmation because they affect shared TiDB Cloud state.

Type consistency:

- `EtlSettings`, `VnstockExtractor`, `RateLimiter`, `RetryQueue`, transform functions, and loader signatures match across tasks.
- Row dictionaries consistently use `symbol`, `trade_date`, OHLCV fields, `source`, `created_at`, `updated_at`, `fetched_at`, `batch_id`.
