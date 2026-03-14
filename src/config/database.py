# src/config/database.py
"""
database.py - Tạo engine và session cho Source DB và Warehouse DB.
Các file ETL import SourceSession / WarehouseSession từ đây để truy vấn.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from src.config.settings import SOURCE_DB_URL, WAREHOUSE_DB_URL


# ── ENGINE ────────────────────────────────────────────────────────
# engine là đối tượng quản lý pool kết nối đến DB
# pool_pre_ping=True: tự động kiểm tra kết nối còn sống không trước khi dùng
source_engine    = create_engine(SOURCE_DB_URL,    pool_pre_ping=True)
warehouse_engine = create_engine(WAREHOUSE_DB_URL, pool_pre_ping=True)


# ── SESSION ───────────────────────────────────────────────────────
# Session là phiên làm việc: dùng để chạy query, commit, rollback
# autocommit=False: phải gọi .commit() thủ công (an toàn hơn)
# autoflush=False : không tự flush trước mỗi query
SourceSession    = sessionmaker(bind=source_engine,    autocommit=False, autoflush=False)
WarehouseSession = sessionmaker(bind=warehouse_engine, autocommit=False, autoflush=False)


# ── BASE CLASS ────────────────────────────────────────────────────
# source_models.py và warehouse_models.py kế thừa từ đây
class SourceBase(DeclarativeBase):
    pass

class WarehouseBase(DeclarativeBase):
    pass


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