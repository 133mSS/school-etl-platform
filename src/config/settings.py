# src/config/settings.py
"""
settings.py - Quản lý tất cả cấu hình từ file .env
"""
import os
from pathlib import Path
from dotenv import load_dotenv

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

# ── CSV (Nguồn 2) ──
CSV_DATA_DIR = os.getenv("CSV_DATA_DIR", str(BASE_DIR / "data" / "csv"))

# ── API JSON fallback (Nguồn 3) ──
API_BASE_URL    = os.getenv("API_BASE_URL", "http://localhost:5055")
API_JSON_DIR    = os.getenv("API_JSON_DIR", str(BASE_DIR / "data" / "api_json"))
API_TIMEOUT     = int(os.getenv("API_TIMEOUT", "30"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))

# ── ETL Settings ──────────────────────────────────
ETL_BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "500"))
ETL_LOG_LEVEL  = os.getenv("ETL_LOG_LEVEL", "INFO")

# ── MINIO ─────────────────────────────────────────
# FIX: Default phải có "http://" prefix để boto3 kết nối đúng.
# Khi chạy local (ngoài Docker): http://localhost:9000
# Khi chạy trong Docker Compose: http://minio:9000  (set qua .env)
MINIO_ENDPOINT       = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY     = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY     = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_RAW     = os.getenv("MINIO_BUCKET_RAW",     "raw-data")
MINIO_BUCKET_STAGING = os.getenv("MINIO_BUCKET_STAGING", "staging-data")

# ── AIRFLOW ───────────────────────────────────────
AIRFLOW_ADMIN_USER = os.getenv("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_ADMIN_PASS = os.getenv("AIRFLOW_ADMIN_PASS", "admin")

# ── ALERT ─────────────────────────────────────────
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")