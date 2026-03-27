"""
test_load.py — Unit tests cho tầng Load
=========================================
Chạy: pytest test/test_load.py -v
  hoặc: python test/test_load.py

Dùng SQLite in-memory thay PostgreSQL → KHÔNG cần Docker.

Coverage:
  ✅ _to_date / _to_decimal / _to_int / _classify_gpa  (utility methods)
  ✅ _load_dim_hoc_ky    (insert + update)
  ✅ _load_dim_giang_vien (insert + update)
  ✅ _load_dim_hoc_phan   (insert + update)
  ✅ _load_dim_sinh_vien  (SCD Type 2: insert, no-change, change → new version)
  ✅ _load_fact_ctsv      (insert + skip khi thiếu FK)
  ✅ _load_fact_tai_chinh (insert + update)
  ✅ _load_agg_summary    (insert + update existing)
  ✅ _build_key_caches    (đọc đúng surrogate keys sau khi load dims)
  ✅ load_all             (end-to-end với data nhỏ)
"""

import sys
import os
import unittest
from datetime import date, datetime
from unittest.mock import patch, MagicMock

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# ── Thêm project root vào sys.path ──
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ══════════════════════════════════════════════════════════════
# MOCK DATABASE — PHẢI đặt TRƯỚC khi import bất kỳ thứ gì từ src
#
# Chuỗi gây crash:
#   warehouse_models.py → database.py → create_engine(PostgreSQL)
#                                      → import psycopg2 → CRASH
# Giải pháp: mock psycopg2 + database trước, sau đó dùng
#            SQLite in-memory thay PostgreSQL cho toàn bộ test
# ══════════════════════════════════════════════════════════════

# 1. Mock psycopg2 driver
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.extensions"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()

# 2. Tạo THẬT WarehouseBase + SourceBase TRƯỚC khi mock database
from sqlalchemy.orm import DeclarativeBase

class _RealWarehouseBase(DeclarativeBase):
    pass

class _RealSourceBase(DeclarativeBase):
    pass

_sqlite_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
)
_TestSession = sessionmaker(bind=_sqlite_engine)

# 3. Mock database module — nhưng dùng Base class thật
import types
_db_module = types.ModuleType("src.config.database")
_db_module.source_engine = _sqlite_engine
_db_module.warehouse_engine = _sqlite_engine
_db_module.WarehouseSession = _TestSession
_db_module.WarehouseBase = _RealWarehouseBase
_db_module.SourceBase = _RealSourceBase
sys.modules["src.config.database"] = _db_module

# 4. Mock settings
_settings_module = types.ModuleType("src.config.settings")
_settings_module.CSV_DATA_DIR = "data/csv"
_settings_module.API_BASE_URL = "http://localhost:5050"
_settings_module.API_JSON_DIR = "data/api_json"
_settings_module.API_TIMEOUT = 30
_settings_module.API_MAX_RETRIES = 3
_settings_module.ETL_BATCH_SIZE = 500
_settings_module.ETL_LOG_LEVEL = "WARNING"
_settings_module.BASE_DIR = "."
_settings_module.SOURCE_DB_URL = "sqlite:///:memory:"
_settings_module.WAREHOUSE_DB_URL = "sqlite:///:memory:"
sys.modules["src.config.settings"] = _settings_module

# Bây giờ mới import — WarehouseBase sẽ là _RealWarehouseBase
# Bây giờ mới import — không còn cần psycopg2 / Docker
from src.models.warehouse_models import (
    WarehouseBase,
    DimHocKy, DimGiangVien, DimHocPhan, DimSinhVien,
    FactHocTap, FactCtsv, FactTaiChinh, AggStudentSummary,
)
from src.etl.transform import TransformedData


# ══════════════════════════════════════════════════════════════
# HELPER: tạo engine + loader riêng cho mỗi test
# Mỗi test class cần engine riêng để tránh data lẫn nhau
# ══════════════════════════════════════════════════════════════

def _make_loader_with_sqlite():
    import src.etl.load as load_module
    from src.etl.load import DataLoader

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )

    # Tạo bảng — bỏ qua lỗi server_default không tương thích SQLite
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.close()

    WarehouseBase.metadata.create_all(engine)

    TestSession = sessionmaker(bind=engine)

    # Patch module-level variables trong load.py
    load_module.WarehouseSession = TestSession
    load_module.warehouse_engine = engine

    # Tạo loader thủ công (bypass __init__ kết nối PG)
    loader = DataLoader.__new__(DataLoader)
    loader.engine = engine
    loader.batch_size = 100
    loader._sv_key_cache = {}
    loader._hp_key_cache = {}
    loader._gv_key_cache = {}
    loader._hk_key_cache = {}
    loader._hp_tc_cache = {}

    return loader, TestSession


# ══════════════════════════════════════════════════════════════
# TEST UTILITY METHODS (không cần DB)
# ══════════════════════════════════════════════════════════════

class TestLoadUtilities(unittest.TestCase):
    """Test các hàm tiện ích trong DataLoader."""

    def setUp(self):
        from src.etl.load import DataLoader
        self.loader = DataLoader.__new__(DataLoader)

    # ── _to_date ──
    def test_to_date_from_string(self):
        result = self.loader._to_date("2024-09-02")
        self.assertEqual(result, date(2024, 9, 2))

    def test_to_date_from_date(self):
        d = date(2024, 9, 2)
        self.assertEqual(self.loader._to_date(d), d)

    def test_to_date_from_timestamp(self):
        ts = pd.Timestamp("2024-09-02")
        result = self.loader._to_date(ts)
        # pd.Timestamp là subclass của datetime.date
        # _to_date check isinstance(val, date) trước → trả về Timestamp
        # Cần gọi .date() để so sánh đúng
        if hasattr(result, "date") and callable(result.date):
            result = result.date()
        self.assertEqual(result, date(2024, 9, 2))

    def test_to_date_none(self):
        self.assertIsNone(self.loader._to_date(None))

    def test_to_date_nan(self):
        self.assertIsNone(self.loader._to_date(np.nan))

    def test_to_date_invalid(self):
        self.assertIsNone(self.loader._to_date("not-a-date"))

    # ── _to_decimal ──
    def test_to_decimal_float(self):
        self.assertAlmostEqual(self.loader._to_decimal(3.756), 3.76, places=2)

    def test_to_decimal_int(self):
        self.assertEqual(self.loader._to_decimal(4), 4.0)

    def test_to_decimal_none(self):
        self.assertIsNone(self.loader._to_decimal(None))

    def test_to_decimal_nan(self):
        self.assertIsNone(self.loader._to_decimal(np.nan))

    # ── _to_int ──
    def test_to_int_float(self):
        self.assertEqual(self.loader._to_int(3.9), 3)

    def test_to_int_string(self):
        self.assertEqual(self.loader._to_int("5"), 5)

    def test_to_int_none(self):
        self.assertIsNone(self.loader._to_int(None))

    def test_to_int_nan(self):
        self.assertIsNone(self.loader._to_int(np.nan))

    # ── _classify_gpa ──
    def test_classify_xuat_sac(self):
        self.assertEqual(self.loader._classify_gpa(3.8), "Xuat sac")

    def test_classify_gioi(self):
        self.assertEqual(self.loader._classify_gpa(3.4), "Gioi")

    def test_classify_kha(self):
        self.assertEqual(self.loader._classify_gpa(2.7), "Kha")

    def test_classify_trung_binh(self):
        self.assertEqual(self.loader._classify_gpa(2.1), "Trung binh")

    def test_classify_yeu(self):
        self.assertEqual(self.loader._classify_gpa(1.5), "Yeu")

    def test_classify_kem(self):
        self.assertEqual(self.loader._classify_gpa(0.5), "Kem")


# ══════════════════════════════════════════════════════════════
# TEST LOAD DIMENSIONS
# ══════════════════════════════════════════════════════════════

class TestLoadDimHocKy(unittest.TestCase):

    def setUp(self):
        self.loader, self.Session = _make_loader_with_sqlite()
        self.df = pd.DataFrame([
            {"ma_hoc_ky": "HK1-2024-25", "nam_hoc": "2024-2025",
             "hoc_ky": "Học kỳ 1",
             "ngay_bat_dau": "2024-09-02", "ngay_ket_thuc": "2025-01-11",
             "nam_bat_dau": 2024, "nam_ket_thuc": 2025},
            {"ma_hoc_ky": "HK2-2024-25", "nam_hoc": "2024-2025",
             "hoc_ky": "Học kỳ 2",
             "ngay_bat_dau": "2025-02-10", "ngay_ket_thuc": "2025-06-27",
             "nam_bat_dau": 2025, "nam_ket_thuc": 2025},
        ])

    def test_insert_2_records(self):
        count = self.loader._load_dim_hoc_ky(self.df)
        self.assertEqual(count, 2)

    def test_records_in_db(self):
        self.loader._load_dim_hoc_ky(self.df)
        session = self.Session()
        rows = session.query(DimHocKy).all()
        session.close()
        self.assertEqual(len(rows), 2)

    def test_upsert_no_duplicate(self):
        """Chạy 2 lần → vẫn chỉ có 2 records."""
        self.loader._load_dim_hoc_ky(self.df)
        self.loader._load_dim_hoc_ky(self.df)
        session = self.Session()
        count = session.query(DimHocKy).count()
        session.close()
        self.assertEqual(count, 2)

    def test_empty_input(self):
        count = self.loader._load_dim_hoc_ky(pd.DataFrame())
        self.assertEqual(count, 0)


class TestLoadDimGiangVien(unittest.TestCase):

    def setUp(self):
        self.loader, self.Session = _make_loader_with_sqlite()
        self.df = pd.DataFrame([
            {"ma_giang_vien": "GV001", "ho": "Nguyen Minh", "ten": "Hieu",
             "ho_ten": "Nguyen Minh Hieu",
             "email": "gv001@ptit.edu.vn", "chuc_danh": "ThS",
             "trang_thai_cong_tac": "Đang công tác",
             "ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT 1"},
        ])

    def test_insert(self):
        self.loader._load_dim_giang_vien(self.df)
        session = self.Session()
        gv = session.query(DimGiangVien).filter_by(ma_giang_vien="GV001").first()
        session.close()
        self.assertIsNotNone(gv)
        self.assertEqual(gv.ho_ten, "Nguyen Minh Hieu")

    def test_update_chuc_danh(self):
        """Chạy lần 2 với chuc_danh thay đổi → cập nhật."""
        self.loader._load_dim_giang_vien(self.df)
        df_updated = self.df.copy()
        df_updated.loc[0, "chuc_danh"] = "TS"
        self.loader._load_dim_giang_vien(df_updated)

        session = self.Session()
        gv = session.query(DimGiangVien).filter_by(ma_giang_vien="GV001").first()
        session.close()
        self.assertEqual(gv.chuc_danh, "TS")
        # Vẫn chỉ có 1 record
        session = self.Session()
        count = session.query(DimGiangVien).count()
        session.close()
        self.assertEqual(count, 1)


class TestLoadDimHocPhan(unittest.TestCase):

    def setUp(self):
        self.loader, self.Session = _make_loader_with_sqlite()
        self.df = pd.DataFrame([
            {"ma_hoc_phan": "CN001", "ma_mon": "CN001",
             "ten_mon": "Triết học Mác-Lênin", "so_tin_chi": 3,
             "so_gio_ly_thuyet": 45, "so_gio_thuc_hanh": 0,
             "hoc_ky_de_xuat": 1, "bat_buoc": True,
             "loai_hoc_phan": "Bat buoc",
             "ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT 1"},
            {"ma_hoc_phan": "CN002", "ma_mon": "CN002",
             "ten_mon": "Đại số", "so_tin_chi": 3,
             "so_gio_ly_thuyet": 45, "so_gio_thuc_hanh": 0,
             "hoc_ky_de_xuat": 1, "bat_buoc": True,
             "loai_hoc_phan": "Bat buoc",
             "ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT 1"},
        ])

    def test_insert_2(self):
        self.loader._load_dim_hoc_phan(self.df)
        session = self.Session()
        count = session.query(DimHocPhan).count()
        session.close()
        self.assertEqual(count, 2)

    def test_so_tin_chi_stored(self):
        self.loader._load_dim_hoc_phan(self.df)
        session = self.Session()
        hp = session.query(DimHocPhan).filter_by(ma_hoc_phan="CN001").first()
        session.close()
        self.assertEqual(hp.so_tin_chi, 3)


class TestLoadDimSinhVienSCD2(unittest.TestCase):
    """Test SCD Type 2 cho dim_sinh_vien."""

    def setUp(self):
        self.loader, self.Session = _make_loader_with_sqlite()
        self.sv_df = pd.DataFrame([{
            "ma_sinh_vien": "B21DCCN001",
            "ho": "Nguyen Van", "ten": "An",
            "ho_ten": "Nguyen Van An",
            "ngay_sinh": "2003-05-15",
            "gioi_tinh": "Nam",
            "email": "sv001@ptit.edu.vn",
            "khoa_hoc": "B21",
            "trang_thai_hoc_tap": "Đang học",
            "ma_nganh": "CNTT", "ten_nganh": "Công nghệ thông tin",
            "ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT 1",
            "ma_lop": "D21CQCN01-B", "ten_lop": "D21CQCN01-B",
            "ma_co_van": "GV001", "ten_co_van": "Nguyen Minh Hieu",
        }])

    def test_insert_new(self):
        self.loader._load_dim_sinh_vien(self.sv_df)
        session = self.Session()
        count = session.query(DimSinhVien).count()
        current = session.query(DimSinhVien).filter_by(
            ma_sinh_vien="B21DCCN001", la_ban_hien_tai=True
        ).first()
        session.close()
        self.assertEqual(count, 1)
        self.assertIsNotNone(current)
        self.assertEqual(current.phien_ban, 1)

    def test_no_change_no_new_version(self):
        """Chạy 2 lần với data không đổi → vẫn 1 record."""
        self.loader._load_dim_sinh_vien(self.sv_df)
        self.loader._load_dim_sinh_vien(self.sv_df)
        session = self.Session()
        count = session.query(DimSinhVien).count()
        session.close()
        self.assertEqual(count, 1)

    def test_scd2_trang_thai_change(self):
        """trang_thai_hoc_tap thay đổi → bản cũ đóng, bản mới version 2."""
        self.loader._load_dim_sinh_vien(self.sv_df)

        sv_updated = self.sv_df.copy()
        sv_updated.loc[0, "trang_thai_hoc_tap"] = "Tốt nghiệp"
        self.loader._load_dim_sinh_vien(sv_updated)

        session = self.Session()
        all_rows = session.query(DimSinhVien).filter_by(
            ma_sinh_vien="B21DCCN001"
        ).all()
        current = session.query(DimSinhVien).filter_by(
            ma_sinh_vien="B21DCCN001", la_ban_hien_tai=True
        ).first()
        session.close()

        # Phải có 2 phiên bản
        self.assertEqual(len(all_rows), 2)
        # Bản hiện tại là Tốt nghiệp, phiên bản 2
        self.assertEqual(current.trang_thai_hoc_tap, "Tốt nghiệp")
        self.assertEqual(current.phien_ban, 2)


# ══════════════════════════════════════════════════════════════
# TEST LOAD FACTS
# ══════════════════════════════════════════════════════════════

class TestLoadFactCtsv(unittest.TestCase):

    def setUp(self):
        self.loader, self.Session = _make_loader_with_sqlite()
        # Cần load dims trước để có surrogate keys
        self._setup_dims()

    def _setup_dims(self):
        """Insert dim_sinh_vien và dim_hoc_ky để tạo surrogate keys."""
        sv_df = pd.DataFrame([{
            "ma_sinh_vien": "B21DCCN001",
            "ho": "Nguyen", "ten": "An", "ho_ten": "Nguyen An",
            "ngay_sinh": "2003-01-01", "gioi_tinh": "Nam",
            "email": "sv@ptit.edu.vn", "khoa_hoc": "B21",
            "trang_thai_hoc_tap": "Đang học",
            "ma_nganh": "CNTT", "ten_nganh": "CNTT",
            "ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT",
            "ma_lop": "D21CQCN01-B", "ten_lop": "D21CQCN01-B",
            "ma_co_van": None, "ten_co_van": None,
        }])
        hk_df = pd.DataFrame([{
            "ma_hoc_ky": "HK1-2024-25", "nam_hoc": "2024-2025",
            "hoc_ky": "Học kỳ 1",
            "ngay_bat_dau": "2024-09-02", "ngay_ket_thuc": "2025-01-11",
            "nam_bat_dau": 2024, "nam_ket_thuc": 2025,
        }])
        self.loader._load_dim_sinh_vien(sv_df)
        self.loader._load_dim_hoc_ky(hk_df)
        self.loader._build_key_caches()

    def test_insert_fact_ctsv(self):
        df = pd.DataFrame([{
            "ma_sinh_vien": "B21DCCN001",
            "hoc_ky": "HK1-2024-25",
            "diem_ren_luyen": 85, "xep_loai_rl": "Tốt",
            "loai_hoc_bong": "KKHT loại 2", "muc_tien_hb": 2400000,
            "hinh_thuc_ky_luat": None, "ly_do_ky_luat": None,
            "co_hoc_bong": True, "bi_ky_luat": False,
        }])
        count = self.loader._load_fact_ctsv(df)
        self.assertEqual(count, 1)

        session = self.Session()
        row = session.query(FactCtsv).first()
        session.close()
        self.assertEqual(row.diem_rl, 85)
        self.assertEqual(row.xep_loai_rl, "Tốt")
        self.assertTrue(row.co_hoc_bong)

    def test_skip_when_sv_not_exist(self):
        """SV không có trong dim_sinh_vien → skip."""
        df = pd.DataFrame([{
            "ma_sinh_vien": "B99DCCN999",  # không tồn tại
            "hoc_ky": "HK1-2024-25",
            "diem_ren_luyen": 70, "xep_loai_rl": "Khá",
            "loai_hoc_bong": None, "muc_tien_hb": 0,
            "hinh_thuc_ky_luat": None, "ly_do_ky_luat": None,
            "co_hoc_bong": False, "bi_ky_luat": False,
        }])
        count = self.loader._load_fact_ctsv(df)
        self.assertEqual(count, 0)

    def test_empty_input(self):
        count = self.loader._load_fact_ctsv(pd.DataFrame())
        self.assertEqual(count, 0)


class TestLoadFactTaiChinh(unittest.TestCase):

    def setUp(self):
        self.loader, self.Session = _make_loader_with_sqlite()
        # Setup dims
        sv_df = pd.DataFrame([{
            "ma_sinh_vien": "B21DCCN001",
            "ho": "Nguyen", "ten": "An", "ho_ten": "Nguyen An",
            "ngay_sinh": "2003-01-01", "gioi_tinh": "Nam",
            "email": "sv@ptit.edu.vn", "khoa_hoc": "B21",
            "trang_thai_hoc_tap": "Đang học",
            "ma_nganh": "CNTT", "ten_nganh": "CNTT",
            "ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT",
            "ma_lop": "D21CQCN01-B", "ten_lop": "D21CQCN01-B",
            "ma_co_van": None, "ten_co_van": None,
        }])
        hk_df = pd.DataFrame([{
            "ma_hoc_ky": "HK1-2024-25", "nam_hoc": "2024-2025",
            "hoc_ky": "Học kỳ 1",
            "ngay_bat_dau": "2024-09-02", "ngay_ket_thuc": "2025-01-11",
            "nam_bat_dau": 2024, "nam_ket_thuc": 2025,
        }])
        self.loader._load_dim_sinh_vien(sv_df)
        self.loader._load_dim_hoc_ky(hk_df)
        self.loader._build_key_caches()

    def test_insert_fact_tai_chinh(self):
        df = pd.DataFrame([{
            "ma_sinh_vien": "B21DCCN001",
            "hoc_ky": "HK1-2024-25",
            "hoc_phi_phai_dong": 8800000,
            "da_dong": 8800000, "con_no": 0,
            "duoc_mien_giam": False,
            "ly_do_mien_giam": None, "so_tien_mien_giam": 0,
            "ngay_dong_cuoi": "2024-10-07",
        }])
        count = self.loader._load_fact_tai_chinh(df)
        self.assertEqual(count, 1)

    def test_con_no_stored_correctly(self):
        df = pd.DataFrame([{
            "ma_sinh_vien": "B21DCCN001",
            "hoc_ky": "HK1-2024-25",
            "hoc_phi_phai_dong": 8800000,
            "da_dong": 4000000, "con_no": 4800000,
            "duoc_mien_giam": False,
            "ly_do_mien_giam": None, "so_tien_mien_giam": 0,
            "ngay_dong_cuoi": None,
        }])
        self.loader._load_fact_tai_chinh(df)
        session = self.Session()
        row = session.query(FactTaiChinh).first()
        session.close()
        self.assertEqual(row.con_no, 4800000)


class TestLoadAggSummary(unittest.TestCase):

    def setUp(self):
        self.loader, self.Session = _make_loader_with_sqlite()
        # Setup dim_sinh_vien
        sv_df = pd.DataFrame([{
            "ma_sinh_vien": "B21DCCN001",
            "ho": "Nguyen", "ten": "An", "ho_ten": "Nguyen An",
            "ngay_sinh": "2003-01-01", "gioi_tinh": "Nam",
            "email": "sv@ptit.edu.vn", "khoa_hoc": "B21",
            "trang_thai_hoc_tap": "Đang học",
            "ma_nganh": "CNTT", "ten_nganh": "CNTT",
            "ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT",
            "ma_lop": "D21CQCN01-B", "ten_lop": "D21CQCN01-B",
            "ma_co_van": None, "ten_co_van": None,
        }])
        hk_df = pd.DataFrame([{
            "ma_hoc_ky": "HK1-2024-25", "nam_hoc": "2024-2025",
            "hoc_ky": "Học kỳ 1",
            "ngay_bat_dau": "2024-09-02", "ngay_ket_thuc": "2025-01-11",
            "nam_bat_dau": 2024, "nam_ket_thuc": 2025,
        }])
        self.loader._load_dim_sinh_vien(sv_df)
        self.loader._load_dim_hoc_ky(hk_df)
        self.loader._build_key_caches()

        self.agg_df = pd.DataFrame([{
            "ma_sinh_vien": "B21DCCN001",
            "ma_hoc_ky": "HK1-2024-25",
            "gpa_hoc_ky_he4": 3.5,
            "gpa_hoc_ky_he10": 8.2,
            "tong_tin_chi": 18,
            "so_mon_hoc": 6, "so_mon_rot": 0,
            "diem_ren_luyen": 85.0, "xep_loai_rl": "Tốt",
            "con_no": 0, "duoc_mien_giam": False,
            "canh_bao_hoc_vu": False, "nguy_co_bo_hoc": False,
            "du_dieu_kien_hoc_bong": True,
            "co_hoc_bong": True, "bi_ky_luat": False,
        }])

    def test_insert_agg(self):
        count = self.loader._load_agg_summary(self.agg_df)
        self.assertGreater(count, 0)
        session = self.Session()
        row = session.query(AggStudentSummary).first()
        session.close()
        self.assertIsNotNone(row)
        self.assertAlmostEqual(float(row.gpa_he_4), 3.5, places=1)

    def test_update_existing(self):
        """Chạy 2 lần → update, không tạo thêm record."""
        self.loader._load_agg_summary(self.agg_df)

        agg_updated = self.agg_df.copy()
        agg_updated.loc[0, "gpa_hoc_ky_he4"] = 3.8
        self.loader._load_agg_summary(agg_updated)

        session = self.Session()
        count = session.query(AggStudentSummary).count()
        row   = session.query(AggStudentSummary).first()
        session.close()

        self.assertEqual(count, 1)
        self.assertAlmostEqual(float(row.gpa_he_4), 3.8, places=1)

    def test_muc_rui_ro_thap(self):
        self.loader._load_agg_summary(self.agg_df)
        session = self.Session()
        row = session.query(AggStudentSummary).first()
        session.close()
        self.assertEqual(row.muc_do_rui_ro, "Thap")

    def test_muc_rui_ro_rat_cao(self):
        agg_risk = self.agg_df.copy()
        agg_risk.loc[0, "nguy_co_bo_hoc"] = True
        agg_risk.loc[0, "gpa_hoc_ky_he4"] = 1.5
        self.loader._load_agg_summary(agg_risk)
        session = self.Session()
        row = session.query(AggStudentSummary).first()
        session.close()
        self.assertEqual(row.muc_do_rui_ro, "Rat cao")

    def test_empty_input(self):
        count = self.loader._load_agg_summary(pd.DataFrame())
        self.assertEqual(count, 0)


# ══════════════════════════════════════════════════════════════
# TEST BUILD KEY CACHES
# ══════════════════════════════════════════════════════════════

class TestBuildKeyCaches(unittest.TestCase):

    def setUp(self):
        self.loader, self.Session = _make_loader_with_sqlite()

    def test_hk_cache_populated(self):
        hk_df = pd.DataFrame([{
            "ma_hoc_ky": "HK1-2024-25", "nam_hoc": "2024-2025",
            "hoc_ky": "Học kỳ 1",
            "ngay_bat_dau": "2024-09-02", "ngay_ket_thuc": "2025-01-11",
            "nam_bat_dau": 2024, "nam_ket_thuc": 2025,
        }])
        self.loader._load_dim_hoc_ky(hk_df)
        self.loader._build_key_caches()

        self.assertIn("HK1-2024-25", self.loader._hk_key_cache)
        hk_key = self.loader._hk_key_cache["HK1-2024-25"]
        self.assertIsNotNone(hk_key)

    def test_hp_tc_cache_populated(self):
        hp_df = pd.DataFrame([{
            "ma_hoc_phan": "CN001", "ma_mon": "CN001",
            "ten_mon": "Triết học", "so_tin_chi": 3,
            "so_gio_ly_thuyet": 45, "so_gio_thuc_hanh": 0,
            "hoc_ky_de_xuat": 1, "bat_buoc": True,
            "loai_hoc_phan": "Bat buoc",
            "ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT",
        }])
        self.loader._load_dim_hoc_phan(hp_df)
        self.loader._build_key_caches()

        hp_key = self.loader._hp_key_cache.get("CN001")
        self.assertIsNotNone(hp_key)

        # _hp_tc_cache phải có so_tin_chi = 3
        tc = self.loader._hp_tc_cache.get(hp_key)
        self.assertEqual(tc, 3)

    def test_lookup_returns_correct_key(self):
        hk_df = pd.DataFrame([{
            "ma_hoc_ky": "HK1-2024-25", "nam_hoc": "2024-2025",
            "hoc_ky": "Học kỳ 1",
            "ngay_bat_dau": "2024-09-02", "ngay_ket_thuc": "2025-01-11",
            "nam_bat_dau": 2024, "nam_ket_thuc": 2025,
        }])
        self.loader._load_dim_hoc_ky(hk_df)
        self.loader._build_key_caches()

        key = self.loader._lookup_hk_key("HK1-2024-25")
        self.assertIsNotNone(key)
        self.assertIsNone(self.loader._lookup_hk_key("HK_KHONG_TON_TAI"))


if __name__ == "__main__":
    print("=" * 60)
    print("RUNNING test_load.py")
    print("=" * 60)
    unittest.main(verbosity=2)