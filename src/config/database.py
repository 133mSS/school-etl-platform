# src/config/database.py


from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from src.config.settings import SOURCE_DB_URL, WAREHOUSE_DB_URL


# ── ENGINE ────────────────────────────────────────────────────────
source_engine    = create_engine(SOURCE_DB_URL,    pool_pre_ping=True)
warehouse_engine = create_engine(WAREHOUSE_DB_URL, pool_pre_ping=True)


# ── SESSION ───────────────────────────────────────────────────────

SourceSession    = sessionmaker(bind=source_engine,    autocommit=False, autoflush=False)
WarehouseSession = sessionmaker(bind=warehouse_engine, autocommit=False, autoflush=False)


# ── BASE CLASS ────────────────────────────────────────────────────
SourceBase = declarative_base()

WarehouseBase = declarative_base()

# ── HÀM TIỆN ÍCH ─────────────────────────────────────────────────
def test_connections():
    """Kiểm tra kết nối cả 2 DB — chạy để verify sau khi setup."""
    results = {}

    # Test Source DB
    try:
        with source_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        results["source"] = "✅ OK"
    except Exception as e:
        results["source"] = f"❌ FAILED: {e}"

    # Test Warehouse DB
    try:
        with warehouse_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        results["warehouse"] = "✅ OK"
    except Exception as e:
        results["warehouse"] = f"❌ FAILED: {e}"

    return results


if __name__ == "__main__":
    print("Kiểm tra kết nối database...")
    r = test_connections()
    print(f"  Source DB    : {r['source']}")
    print(f"  Warehouse DB : {r['warehouse']}")