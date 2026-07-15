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


def test_load_settings_caps_requests_per_minute_at_global_limit(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TIDB_URL=mysql+pymysql://user:pass@example.com:4000/db\n"
        "ETL_REQUESTS_PER_MINUTE=301\n",
        encoding="utf-8",
    )

    try:
        load_settings(env_file)
    except ValueError as exc:
        assert "ETL_REQUESTS_PER_MINUTE" in str(exc)
        assert "300" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_configure_logging_creates_dated_log_file(tmp_path: Path):
    logger = configure_logging(log_dir=tmp_path, run_date=date(2026, 7, 16))

    logger.info("hello")

    log_file = tmp_path / "etl_20260716.log"
    assert log_file.exists()
    assert "hello" in log_file.read_text(encoding="utf-8")
