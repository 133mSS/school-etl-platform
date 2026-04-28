# src/utils/logger.py
"""
Logger dùng chung — fix bug duplicate log lines.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

_FORMATTER = logging.Formatter(
    fmt="[%(asctime)s] %(levelname)-7s %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Cờ chặn add handler nhiều lần khi import lại module trong Airflow
_CONFIGURED = False


def _configure_root_once() -> None:
    """Cấu hình root logger 1 lần duy nhất cho cả process."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Xoá handler cũ (Airflow đôi khi đã add handler vào root)
    for h in list(root.handlers):
        root.removeHandler(h)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(_FORMATTER)
    root.addHandler(console)

    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(_FORMATTER)
    root.addHandler(file_handler)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """
    Trả về logger con — không add handler riêng, dùng handler của root.
    Tắt propagate=False sẽ làm log không hiện → để mặc định True và
    chỉ cấu hình root 1 lần.
    """
    _configure_root_once()
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # KHÔNG add handler ở đây — tránh duplicate
    return logger