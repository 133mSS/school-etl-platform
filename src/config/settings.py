# src/config/settings.py
"""
settings.py - Quản lý tất cả cấu hình từ file .env
Các file khác import từ đây, KHÔNG hardcode connection string trực tiếp.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Tìm file .env từ thư mục gốc dự án
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

# ── SOURCE DATABASE ───────────────────────────────
SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST", "localhost")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT", "5434")
SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME", "school_source")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER", "school_user")
SOURCE_DB_PASS = os.getenv("SOURCE_DB_PASS", "school_pass")

SOURCE_DB_URL = (
    f"postgresql+psycopg2://{SOURCE_DB_USER}:{SOURCE_DB_PASS}"
    f"@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
)

# ── WAREHOUSE DATABASE ────────────────────────────
WH_DB_HOST = os.getenv("WH_DB_HOST", "localhost")
WH_DB_PORT = os.getenv("WH_DB_PORT", "5435")
WH_DB_NAME = os.getenv("WH_DB_NAME", "school_warehouse")
WH_DB_USER = os.getenv("WH_DB_USER", "warehouse_user")
WH_DB_PASS = os.getenv("WH_DB_PASS", "warehouse_pass")

WAREHOUSE_DB_URL = (
    f"postgresql+psycopg2://{WH_DB_USER}:{WH_DB_PASS}"
    f"@{WH_DB_HOST}:{WH_DB_PORT}/{WH_DB_NAME}"
)

# ── MINIO ─────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_RAW     = os.getenv("MINIO_BUCKET_RAW",     "raw-data")
MINIO_BUCKET_STAGING = os.getenv("MINIO_BUCKET_STAGING", "staging-data")

# ── AIRFLOW ───────────────────────────────────────
AIRFLOW_ADMIN_USER = os.getenv("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_ADMIN_PASS = os.getenv("AIRFLOW_ADMIN_PASS", "admin")

# ── ALERT ─────────────────────────────────────────
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")