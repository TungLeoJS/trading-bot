from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values, load_dotenv


@dataclass(frozen=True)
class EtlSettings:
    tidb_url: str
    source: str = "vnstock_data"
    requests_per_minute: int = 300
    retry_delay_seconds: int = 300
    max_attempts: int = 3
    batch_size: int = 100


def _int_env(
    name: str,
    default: int,
    values: dict[str, str | None] | None = None,
    maximum: int | None = None,
) -> int:
    raw = values.get(name) if values is not None else os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be less than or equal to {maximum}")
    return value


def load_settings(env_file: str | Path | None = None) -> EtlSettings:
    if env_file is not None:
        values = dotenv_values(Path(env_file))
    else:
        load_dotenv(override=False)
        values = None

    tidb_url = values.get("TIDB_URL") if values is not None else os.getenv("TIDB_URL")
    if not tidb_url:
        raise ValueError("TIDB_URL is required")

    return EtlSettings(
        tidb_url=tidb_url,
        source=(values.get("ETL_SOURCE") if values is not None else os.getenv("ETL_SOURCE")) or "vnstock_data",
        requests_per_minute=_int_env("ETL_REQUESTS_PER_MINUTE", 300, values, maximum=300),
        retry_delay_seconds=_int_env("ETL_RETRY_DELAY_SECONDS", 300, values),
        max_attempts=_int_env("ETL_MAX_ATTEMPTS", 3, values),
        batch_size=_int_env("ETL_BATCH_SIZE", 100, values),
    )
