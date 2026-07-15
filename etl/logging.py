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
