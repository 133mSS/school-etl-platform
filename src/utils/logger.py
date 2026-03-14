# src/utils/logger.py
"""
logger.py - Logger dùng chung cho toàn bộ dự án.
Cách dùng ở file khác:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Extracted 1000 rows")
    logger.warning("Null values found in column email")
    logger.error("Connection failed!")
"""

import logging
import os
from datetime import datetime
from pathlib import Path

# Thư mục lưu file log — tạo tự động nếu chưa có
LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    """
    Tạo logger với tên cho trước.
    - In ra màn hình (console)
    - Đồng thời ghi vào file logs/etl_YYYY-MM-DD.log

    Tham số:
        name: thường truyền __name__ để biết log từ file nào
    """
    logger = logging.getLogger(name)

    # Tránh thêm handler trùng nếu gọi get_logger nhiều lần
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # Format: [2024-01-15 10:30:00] INFO  src.etl.extract : Extracted 1000 rows
    formatter = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)-7s %(name)s : %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler 1 — in ra màn hình
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler 2 — ghi vào file log theo ngày
    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger