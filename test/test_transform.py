"""
test_transform.py — Unit tests cho tầng Transform
==================================================
Chạy: pytest test/test_transform.py -v
  hoặc: python test/test_transform.py

KHÔNG cần kết nối DB — test hoàn toàn với dữ liệu giả (mock DataFrames).

Coverage:
  ✅ _transform_dim_hoc_ky
  ✅ _transform_dim_giang_vien
  ✅ _transform_dim_hoc_phan
  ✅ _transform_dim_sinh_vien
  ✅ _transform_fact_diem        (trọng số, điểm chữ, dat_mon)
  ✅ _transform_fact_ren_luyen   (flags co_hoc_bong, bi_ky_luat, xep_loai_rl)
  ✅ _transform_fact_tai_chinh   (ép kiểu số, ngày)
  ✅ _build_agg_student_summary  (nguy_co_bo_hoc, du_dieu_kien_hoc_bong)
  ✅ _to_letter_grade / _to_gpa_4 (tất cả ngưỡng)
  ✅ _classify_rl                (tất cả ngưỡng)
  ✅ Edge cases: empty DataFrame, NULL values
"""

import sys
import os
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np

# ── Thêm project root vào sys.path để import src ──
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ══════════════════════════════════════════════════════════════
# MOCK DATABASE — chặn import chain trước khi nó cố kết nối DB
#
# Vấn đề: transform.py → extract.py → database.py → create_engine(PostgreSQL)
#          → crash vì không có psycopg2 / Docker chưa chạy
#
# Giải pháp: mock toàn bộ src.config.database và psycopg2
#            TRƯỚC khi import bất kỳ module nào của src
# ══════════════════════════════════════════════════════════════
_mock_engine = MagicMock()
_mock_session = MagicMock()

# Dùng gán trực tiếp (không dùng setdefault) để đảm bảo override
sys.modules["psycopg2"]            = MagicMock()
sys.modules["psycopg2.extras"]     = MagicMock()
sys.modules["psycopg2.extensions"] = MagicMock()

# Mock toàn bộ module database để create_engine không chạy
_db_mock = MagicMock()
_db_mock.source_engine    = _mock_engine
_db_mock.warehouse_engine = _mock_engine
_db_mock.WarehouseSession = MagicMock(return_value=_mock_session)
sys.modules["src.config.database"] = _db_mock

# Mock settings để tránh lỗi import thiếu biến môi trường
_settings_mock = MagicMock()
_settings_mock.CSV_DATA_DIR  = "data/csv"
_settings_mock.API_BASE_URL  = "http://localhost:5050"
_settings_mock.API_TIMEOUT   = 30
_settings_mock.API_MAX_RETRIES = 3
_settings_mock.ETL_BATCH_SIZE  = 500
sys.modules["src.config.settings"] = _settings_mock

# Bây giờ mới import — không còn cố kết nối DB nữa
from src.etl.transform import DataTransformer, GRADE_SCALE, RL_THRESHOLDS


class TestLetterGrade(unittest.TestCase):
    """Test hàm _to_letter_grade và _to_gpa_4."""

    def setUp(self):
        self.tf = DataTransformer()

    def test_A_plus(self):
        self.assertEqual(self.tf._to_letter_grade(9.5),  "A+")
        self.assertEqual(self.tf._to_gpa_4(9.5),          4.0)

    def test_A(self):
        self.assertEqual(self.tf._to_letter_grade(8.7),  "A")
        self.assertEqual(self.tf._to_gpa_4(8.7),          3.7)

    def test_B_plus(self):
        self.assertEqual(self.tf._to_letter_grade(8.2),  "B+")
        self.assertEqual(self.tf._to_gpa_4(8.2),          3.5)

    def test_B(self):
        self.assertEqual(self.tf._to_letter_grade(7.5),  "B")
        self.assertEqual(self.tf._to_gpa_4(7.5),          3.0)

    def test_C_plus(self):
        self.assertEqual(self.tf._to_letter_grade(6.8),  "C+")
        self.assertEqual(self.tf._to_gpa_4(6.8),          2.5)

    def test_C(self):
        self.assertEqual(self.tf._to_letter_grade(6.0),  "C")
        self.assertEqual(self.tf._to_gpa_4(6.0),          2.0)

    def test_D_plus(self):
        self.assertEqual(self.tf._to_letter_grade(5.2),  "D+")
        self.assertEqual(self.tf._to_gpa_4(5.2),          1.5)

    def test_D(self):
        self.assertEqual(self.tf._to_letter_grade(4.5),  "D")
        self.assertEqual(self.tf._to_gpa_4(4.5),          1.0)

    def test_F(self):
        self.assertEqual(self.tf._to_letter_grade(3.9),  "F")
        self.assertEqual(self.tf._to_gpa_4(3.9),          0.0)
        self.assertEqual(self.tf._to_letter_grade(0.0),  "F")

    def test_boundary_exactly_8_5(self):
        """Điểm đúng 8.5 → A (không phải B+)."""
        self.assertEqual(self.tf._to_letter_grade(8.5), "A")

    def test_boundary_exactly_9_0(self):
        """Điểm đúng 9.0 → A+."""
        self.assertEqual(self.tf._to_letter_grade(9.0), "A+")

    def test_boundary_exactly_4_0(self):
        """Điểm đúng 4.0 → D (ngưỡng đạt môn)."""
        self.assertEqual(self.tf._to_letter_grade(4.0), "D")

    def test_null_returns_none(self):
        self.assertIsNone(self.tf._to_letter_grade(np.nan))
        self.assertIsNone(self.tf._to_gpa_4(np.nan))


class TestClassifyRL(unittest.TestCase):
    """Test hàm _classify_rl."""

    def setUp(self):
        self.tf = DataTransformer()

    def test_xuat_sac(self):
        self.assertEqual(self.tf._classify_rl(95),  "Xuất sắc")
        self.assertEqual(self.tf._classify_rl(90),  "Xuất sắc")

    def test_tot(self):
        self.assertEqual(self.tf._classify_rl(85),  "Tốt")
        self.assertEqual(self.tf._classify_rl(80),  "Tốt")

    def test_kha(self):
        self.assertEqual(self.tf._classify_rl(70),  "Khá")
        self.assertEqual(self.tf._classify_rl(65),  "Khá")

    def test_trung_binh(self):
        self.assertEqual(self.tf._classify_rl(55),  "Trung bình")
        self.assertEqual(self.tf._classify_rl(50),  "Trung bình")

    def test_yeu(self):
        self.assertEqual(self.tf._classify_rl(40),  "Yếu")
        self.assertEqual(self.tf._classify_rl(35),  "Yếu")

    def test_kem(self):
        self.assertEqual(self.tf._classify_rl(20),  "Kém")
        self.assertEqual(self.tf._classify_rl(0),   "Kém")

    def test_null(self):
        self.assertIsNone(self.tf._classify_rl(np.nan))


class TestTransformDimHocKy(unittest.TestCase):
    """Test _transform_dim_hoc_ky."""

    def setUp(self):
        self.tf = DataTransformer()
        self.df = pd.DataFrame([
            {"ma_hoc_ky": "HK1-2024-25", "nam_hoc": "2024-2025",
             "hoc_ky": "Học kỳ 1",
             "ngay_bat_dau": "2024-09-02", "ngay_ket_thuc": "2025-01-11"},
            {"ma_hoc_ky": "HK2-2024-25", "nam_hoc": "2024-2025",
             "hoc_ky": "Học kỳ 2",
             "ngay_bat_dau": "2025-02-10", "ngay_ket_thuc": "2025-06-27"},
        ])

    def test_output_columns(self):
        result = self.tf._transform_dim_hoc_ky(self.df)
        for col in ["ma_hoc_ky", "nam_hoc", "hoc_ky",
                    "ngay_bat_dau", "ngay_ket_thuc",
                    "nam_bat_dau", "nam_ket_thuc"]:
            self.assertIn(col, result.columns, f"Thiếu cột: {col}")

    def test_row_count(self):
        result = self.tf._transform_dim_hoc_ky(self.df)
        self.assertEqual(len(result), 2)

    def test_nam_bat_dau_extracted(self):
        result = self.tf._transform_dim_hoc_ky(self.df)
        self.assertEqual(result.iloc[0]["nam_bat_dau"], 2024)
        self.assertEqual(result.iloc[0]["nam_ket_thuc"], 2025)

    def test_empty_input(self):
        result = self.tf._transform_dim_hoc_ky(pd.DataFrame())
        self.assertTrue(result.empty)

    def test_dedup(self):
        df_dup = pd.concat([self.df, self.df])
        result = self.tf._transform_dim_hoc_ky(df_dup)
        self.assertEqual(len(result), 2)  # deduplicated


class TestTransformDimSinhVien(unittest.TestCase):
    """Test _transform_dim_sinh_vien."""

    def setUp(self):
        self.tf = DataTransformer()
        self.sv_df = pd.DataFrame([
            {"ma_sinh_vien": "B21DCCN001", "ho": "Nguyen Van",
             "ten": "An", "ngay_sinh": "2003-05-15",
             "gioi_tinh": "Nam", "email": "b21dccn001@ptit.edu.vn",
             "ma_nganh": "CNTT", "ma_lop": "D21CQCN01-B",
             "khoa_hoc": "B21", "trang_thai_hoc_tap": "Đang học"},
            {"ma_sinh_vien": "B21DCKT001", "ho": "Tran Thi",
             "ten": "Binh", "ngay_sinh": "2003-08-20",
             "gioi_tinh": "Nữ", "email": "b21dckt001@ptit.edu.vn",
             "ma_nganh": "KE_TOAN", "ma_lop": "D21CQKT01-B",
             "khoa_hoc": "B21", "trang_thai_hoc_tap": "Tốt nghiệp"},
        ])
        self.nganh_df = pd.DataFrame([
            {"ma_nganh": "CNTT",     "ten_nganh": "Công nghệ thông tin", "ma_khoa": "CNTT1"},
            {"ma_nganh": "KE_TOAN",  "ten_nganh": "Kế toán",             "ma_khoa": "KKT"},
        ])
        self.khoa_df = pd.DataFrame([
            {"ma_khoa": "CNTT1", "ten_khoa": "Khoa CNTT 1"},
            {"ma_khoa": "KKT",   "ten_khoa": "Khoa Kế toán"},
        ])
        self.lop_df = pd.DataFrame([
            {"ma_lop": "D21CQCN01-B", "ten_lop": "D21CQCN01-B", "ma_co_van": "GV001"},
            {"ma_lop": "D21CQKT01-B", "ten_lop": "D21CQKT01-B", "ma_co_van": "GV011"},
        ])
        self.gv_df = pd.DataFrame([
            {"ma_giang_vien": "GV001", "ho": "Nguyen Minh", "ten": "Hieu"},
            {"ma_giang_vien": "GV011", "ho": "Nguyen Van",  "ten": "Thanh"},
        ])

    def test_ho_ten_concatenated(self):
        result = self.tf._transform_dim_sinh_vien(
            self.sv_df, self.nganh_df, self.lop_df,
            self.khoa_df, self.gv_df
        )
        self.assertIn("ho_ten", result.columns)
        self.assertEqual(result.iloc[0]["ho_ten"], "Nguyen Van An")

    def test_enrich_ten_nganh(self):
        result = self.tf._transform_dim_sinh_vien(
            self.sv_df, self.nganh_df, self.lop_df,
            self.khoa_df, self.gv_df
        )
        self.assertIn("ten_nganh", result.columns)
        cn_row = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        self.assertEqual(cn_row["ten_nganh"], "Công nghệ thông tin")

    def test_enrich_ten_co_van(self):
        result = self.tf._transform_dim_sinh_vien(
            self.sv_df, self.nganh_df, self.lop_df,
            self.khoa_df, self.gv_df
        )
        self.assertIn("ten_co_van", result.columns)
        cn_row = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        self.assertEqual(cn_row["ten_co_van"], "Nguyen Minh Hieu")

    def test_row_count(self):
        result = self.tf._transform_dim_sinh_vien(
            self.sv_df, self.nganh_df, self.lop_df,
            self.khoa_df, self.gv_df
        )
        self.assertEqual(len(result), 2)

    def test_empty_input(self):
        result = self.tf._transform_dim_sinh_vien(
            pd.DataFrame(), self.nganh_df, self.lop_df,
            self.khoa_df, self.gv_df
        )
        self.assertTrue(result.empty)


class TestTransformFactDiem(unittest.TestCase):
    """Test _transform_fact_diem — trọng số, điểm chữ, dat_mon."""

    def setUp(self):
        self.tf = DataTransformer()
        # dk: ma_dang_ky → ma_sinh_vien, ma_hoc_phan, ma_hoc_ky, ma_giang_vien
        self.dk_df = pd.DataFrame([
            {"ma_dang_ky": 1, "ma_sinh_vien": "B21DCCN001",
             "ma_hoc_phan": "CN001", "ma_hoc_ky": "HK1-2024-25",
             "ma_giang_vien": "GV001"},
            {"ma_dang_ky": 2, "ma_sinh_vien": "B21DCCN002",
             "ma_hoc_phan": "CN001", "ma_hoc_ky": "HK1-2024-25",
             "ma_giang_vien": "GV001"},
        ])
        # diem: có đủ 4 thành phần
        self.diem_df = pd.DataFrame([
            {"ma_dang_ky": 1, "diem_chuyen_can": 8.0, "diem_bai_tap": 7.0,
             "diem_giua_ky": 7.0, "diem_cuoi_ky": 9.0,
             "diem_tong_ket": None, "diem_chu": None,
             "diem_he_4": None, "dat_mon": None, "hoc_lai": False},
            # SV2: điểm tổng kết đã có sẵn (không cần tính lại)
            {"ma_dang_ky": 2, "diem_chuyen_can": 5.0, "diem_bai_tap": 3.0,
             "diem_giua_ky": 3.0, "diem_cuoi_ky": 3.0,
             "diem_tong_ket": 3.2, "diem_chu": "F",
             "diem_he_4": 0.0, "dat_mon": False, "hoc_lai": False},
        ])

    def test_output_has_key_columns(self):
        result = self.tf._transform_fact_diem(self.diem_df, self.dk_df)
        for col in ["ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky",
                    "diem_tong_ket", "diem_chu", "diem_he_4", "dat_mon"]:
            self.assertIn(col, result.columns, f"Thiếu cột: {col}")

    def test_tinh_lai_diem_tong_ket(self):
        """SV1: dtk = 0.1*8 + 0.1*7 + 0.2*7 + 0.6*9 = 8.1"""
        result = self.tf._transform_fact_diem(self.diem_df, self.dk_df)
        sv1 = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        expected_dtk = round(0.1*8.0 + 0.1*7.0 + 0.2*7.0 + 0.6*9.0, 2)
        self.assertAlmostEqual(float(sv1["diem_tong_ket"]), expected_dtk, places=1)

    def test_diem_chu_correct(self):
        """SV1 dtk=8.1 → B+"""
        result = self.tf._transform_fact_diem(self.diem_df, self.dk_df)
        sv1 = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        self.assertEqual(sv1["diem_chu"], "B+")

    def test_dat_mon_true_when_pass(self):
        """SV1 dtk=8.1 ≥ 4.0 → dat_mon=True"""
        result = self.tf._transform_fact_diem(self.diem_df, self.dk_df)
        sv1 = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        self.assertTrue(sv1["dat_mon"])

    def test_dat_mon_false_when_fail(self):
        """SV2 dtk=3.2 < 4.0 → dat_mon=False"""
        result = self.tf._transform_fact_diem(self.diem_df, self.dk_df)
        sv2 = result[result["ma_sinh_vien"] == "B21DCCN002"].iloc[0]
        self.assertFalse(sv2["dat_mon"])

    def test_khong_tinh_lai_khi_co_san(self):
        """SV2 đã có diem_tong_ket=3.2 → không tính lại"""
        result = self.tf._transform_fact_diem(self.diem_df, self.dk_df)
        sv2 = result[result["ma_sinh_vien"] == "B21DCCN002"].iloc[0]
        self.assertAlmostEqual(float(sv2["diem_tong_ket"]), 3.2, places=1)

    def test_empty_diem(self):
        result = self.tf._transform_fact_diem(pd.DataFrame(), self.dk_df)
        self.assertTrue(result.empty)

    def test_empty_dk(self):
        result = self.tf._transform_fact_diem(self.diem_df, pd.DataFrame())
        self.assertTrue(result.empty)


class TestTransformFactRenLuyen(unittest.TestCase):
    """Test _transform_fact_ren_luyen."""

    def setUp(self):
        self.tf = DataTransformer()
        self.csv_df = pd.DataFrame([
            # SV có học bổng, không kỷ luật
            {"ma_sinh_vien": "b21dccn001", "hoc_ky": "HK1-2024-25",
             "diem_ren_luyen": 92, "xep_loai_rl": None,
             "loai_hoc_bong": "KKHT loại 1", "muc_tien_hb": 3600000,
             "hinh_thuc_ky_luat": "", "ly_do_ky_luat": ""},
            # SV bị kỷ luật, không học bổng
            {"ma_sinh_vien": "B21DCCN002", "hoc_ky": "HK1-2024-25",
             "diem_ren_luyen": 40, "xep_loai_rl": "Yếu",
             "loai_hoc_bong": "", "muc_tien_hb": 0,
             "hinh_thuc_ky_luat": "Cảnh cáo lần 1", "ly_do_ky_luat": "Thi hộ"},
            # SV DRL NULL
            {"ma_sinh_vien": "B21DCCN003", "hoc_ky": "HK1-2024-25",
             "diem_ren_luyen": None, "xep_loai_rl": None,
             "loai_hoc_bong": None, "muc_tien_hb": None,
             "hinh_thuc_ky_luat": None, "ly_do_ky_luat": None},
        ])

    def test_ma_sv_uppercase(self):
        """ma_sinh_vien phải uppercase."""
        result = self.tf._transform_fact_ren_luyen(self.csv_df)
        self.assertEqual(result.iloc[0]["ma_sinh_vien"], "B21DCCN001")

    def test_co_hoc_bong_flag(self):
        result = self.tf._transform_fact_ren_luyen(self.csv_df)
        sv1 = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        sv2 = result[result["ma_sinh_vien"] == "B21DCCN002"].iloc[0]
        self.assertTrue(sv1["co_hoc_bong"])
        self.assertFalse(sv2["co_hoc_bong"])

    def test_bi_ky_luat_flag(self):
        result = self.tf._transform_fact_ren_luyen(self.csv_df)
        sv1 = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        sv2 = result[result["ma_sinh_vien"] == "B21DCCN002"].iloc[0]
        self.assertFalse(sv1["bi_ky_luat"])
        self.assertTrue(sv2["bi_ky_luat"])

    def test_xep_loai_rl_auto_classify(self):
        """SV1: diem_ren_luyen=92, xep_loai_rl=None → tự tính 'Xuất sắc'."""
        result = self.tf._transform_fact_ren_luyen(self.csv_df)
        sv1 = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        self.assertEqual(sv1["xep_loai_rl"], "Xuất sắc")

    def test_xep_loai_rl_giu_nguyen(self):
        """SV2: đã có xep_loai_rl='Yếu' → không tính lại."""
        result = self.tf._transform_fact_ren_luyen(self.csv_df)
        sv2 = result[result["ma_sinh_vien"] == "B21DCCN002"].iloc[0]
        self.assertEqual(sv2["xep_loai_rl"], "Yếu")

    def test_empty_string_to_nan(self):
        """loai_hoc_bong='' → NaN sau transform."""
        result = self.tf._transform_fact_ren_luyen(self.csv_df)
        sv2 = result[result["ma_sinh_vien"] == "B21DCCN002"].iloc[0]
        self.assertTrue(pd.isna(sv2["loai_hoc_bong"]))

    def test_row_count(self):
        result = self.tf._transform_fact_ren_luyen(self.csv_df)
        self.assertEqual(len(result), 3)

    def test_empty_input(self):
        result = self.tf._transform_fact_ren_luyen(pd.DataFrame())
        self.assertTrue(result.empty)


class TestTransformFactTaiChinh(unittest.TestCase):
    """Test _transform_fact_tai_chinh."""

    def setUp(self):
        self.tf = DataTransformer()
        self.api_df = pd.DataFrame([
            {"ma_sinh_vien": "b21dccn001", "hoc_ky": "HK1-2024-25",
             "hoc_phi_phai_dong": 8800000, "da_dong": 8800000, "con_no": 0,
             "duoc_mien_giam": False, "ly_do_mien_giam": "",
             "so_tien_mien_giam": 0, "ngay_dong_cuoi": "2024-10-07"},
            {"ma_sinh_vien": "B21DCCN002", "hoc_ky": "HK1-2024-25",
             "hoc_phi_phai_dong": 8800000, "da_dong": 4000000,
             "con_no": 4800000,
             "duoc_mien_giam": True, "ly_do_mien_giam": "Hộ nghèo",
             "so_tien_mien_giam": 4400000, "ngay_dong_cuoi": None},
        ])

    def test_ma_sv_uppercase(self):
        result = self.tf._transform_fact_tai_chinh(self.api_df)
        self.assertEqual(result.iloc[0]["ma_sinh_vien"], "B21DCCN001")

    def test_so_tien_numeric(self):
        result = self.tf._transform_fact_tai_chinh(self.api_df)
        self.assertTrue(pd.api.types.is_numeric_dtype(result["hoc_phi_phai_dong"]))
        self.assertTrue(pd.api.types.is_numeric_dtype(result["con_no"]))

    def test_ngay_dong_cuoi_parsed(self):
        result = self.tf._transform_fact_tai_chinh(self.api_df)
        sv1 = result[result["ma_sinh_vien"] == "B21DCCN001"].iloc[0]
        self.assertIsNotNone(sv1["ngay_dong_cuoi"])

    def test_ngay_dong_cuoi_null_ok(self):
        result = self.tf._transform_fact_tai_chinh(self.api_df)
        sv2 = result[result["ma_sinh_vien"] == "B21DCCN002"].iloc[0]
        self.assertTrue(pd.isna(sv2["ngay_dong_cuoi"]))

    def test_con_no_no_nan(self):
        """con_no không được là NaN — fillna(0)."""
        result = self.tf._transform_fact_tai_chinh(self.api_df)
        self.assertFalse(result["con_no"].isna().any())

    def test_empty_input(self):
        result = self.tf._transform_fact_tai_chinh(pd.DataFrame())
        self.assertTrue(result.empty)


class TestAggStudentSummary(unittest.TestCase):
    """Test _build_agg_student_summary — logic 3 nguồn + business flags.

    Lưu ý: _calculate_scholarship phân nhóm theo mã SV format B21DCCN001:
      - str[:3]  = khóa học  (B21)
      - str[5:7] = mã ngành  (CN)
    Dùng đúng format để nhóm hóa hoạt động đúng.
    """

    def setUp(self):
        self.tf = DataTransformer()

        # ── Mã SV đúng format PTIT ──
        # SV001 = B21DCCN001 (khóa B21, ngành CN)
        # SV002 = B21DCCN002 (cùng nhóm → xếp hạng với nhau)
        SV1 = "B21DCCN001"
        SV2 = "B21DCCN002"

        self.hp_df = pd.DataFrame([
            {"ma_hoc_phan": "CN001", "so_tin_chi": 3},
            {"ma_hoc_phan": "CN002", "so_tin_chi": 3},
        ])
        self.dk_df = pd.DataFrame([
            {"ma_dang_ky": 1, "ma_sinh_vien": SV1,
             "ma_hoc_phan": "CN001", "ma_hoc_ky": "HK1-2024-25"},
            {"ma_dang_ky": 2, "ma_sinh_vien": SV1,
             "ma_hoc_phan": "CN002", "ma_hoc_ky": "HK1-2024-25"},
            {"ma_dang_ky": 3, "ma_sinh_vien": SV2,
             "ma_hoc_phan": "CN001", "ma_hoc_ky": "HK1-2024-25"},
        ])

        # SV1 giỏi (GPA 3.6), SV2 yếu (GPA 0.0)
        self.fact_diem = pd.DataFrame([
            {"ma_dang_ky": 1, "ma_sinh_vien": SV1,
             "ma_hoc_phan": "CN001", "ma_hoc_ky": "HK1-2024-25",
             "diem_tong_ket": 8.5, "diem_he_4": 3.7, "dat_mon": True},
            {"ma_dang_ky": 2, "ma_sinh_vien": SV1,
             "ma_hoc_phan": "CN002", "ma_hoc_ky": "HK1-2024-25",
             "diem_tong_ket": 8.0, "diem_he_4": 3.5, "dat_mon": True},
            {"ma_dang_ky": 3, "ma_sinh_vien": SV2,
             "ma_hoc_phan": "CN001", "ma_hoc_ky": "HK1-2024-25",
             "diem_tong_ket": 1.5, "diem_he_4": 0.0, "dat_mon": False},
        ])

        # Cả 2 SV có RL — SV1 tốt (85), SV2 yếu (45)
        self.fact_rl = pd.DataFrame([
            {"ma_sinh_vien": SV1, "hoc_ky": "HK1-2024-25",
             "diem_ren_luyen": 85, "xep_loai_rl": "Tốt",
             "co_hoc_bong": False, "bi_ky_luat": False,
             "loai_hoc_bong": None, "muc_tien_hb": 0},
            {"ma_sinh_vien": SV2, "hoc_ky": "HK1-2024-25",
             "diem_ren_luyen": 45, "xep_loai_rl": "Yếu",
             "co_hoc_bong": False, "bi_ky_luat": False,
             "loai_hoc_bong": None, "muc_tien_hb": 0},
        ])
        # SV2 nợ HP > 50%
        self.fact_tc = pd.DataFrame([
            {"ma_sinh_vien": SV2, "hoc_ky": "HK1-2024-25",
             "hoc_phi_phai_dong": 8000000, "con_no": 6000000,
             "duoc_mien_giam": False},
        ])

        self.SV1 = SV1
        self.SV2 = SV2

    def test_output_has_both_sv(self):
        result = self.tf._build_agg_student_summary(
            self.fact_diem, self.fact_rl, self.fact_tc,
            self.dk_df, self.hp_df
        )
        svs = set(result["ma_sinh_vien"].tolist())
        self.assertIn(self.SV1, svs)
        self.assertIn(self.SV2, svs)

    def test_gpa_sv001_correct(self):
        """SV1: (3.7*3 + 3.5*3) / 6 = 3.6"""
        result = self.tf._build_agg_student_summary(
            self.fact_diem, self.fact_rl, self.fact_tc,
            self.dk_df, self.hp_df
        )
        sv1 = result[result["ma_sinh_vien"] == self.SV1].iloc[0]
        self.assertAlmostEqual(float(sv1["gpa_hoc_ky_he4"]), 3.6, places=1)

    def test_canh_bao_hoc_vu(self):
        """SV2 GPA 0.0 < 1.0 → canh_bao_hoc_vu=True."""
        result = self.tf._build_agg_student_summary(
            self.fact_diem, self.fact_rl, self.fact_tc,
            self.dk_df, self.hp_df
        )
        sv2 = result[result["ma_sinh_vien"] == self.SV2].iloc[0]
        self.assertTrue(sv2["canh_bao_hoc_vu"])

    def test_nguy_co_bo_hoc(self):
        """SV2: GPA<2.0 + DRL<50 + nợ HP>50% → nguy_co_bo_hoc=True."""
        result = self.tf._build_agg_student_summary(
            self.fact_diem, self.fact_rl, self.fact_tc,
            self.dk_df, self.hp_df
        )
        sv2 = result[result["ma_sinh_vien"] == self.SV2].iloc[0]
        self.assertTrue(sv2["nguy_co_bo_hoc"])

    def test_sv001_khong_nguy_co(self):
        """SV1 GPA cao → không nguy cơ, không cảnh báo."""
        result = self.tf._build_agg_student_summary(
            self.fact_diem, self.fact_rl, self.fact_tc,
            self.dk_df, self.hp_df
        )
        sv1 = result[result["ma_sinh_vien"] == self.SV1].iloc[0]
        self.assertFalse(sv1["nguy_co_bo_hoc"])
        self.assertFalse(sv1["canh_bao_hoc_vu"])

    def test_du_dieu_kien_hoc_bong(self):
        """SV1: GPA=3.6, RL=85, không KL, không nợ HP → top 10% → đủ ĐK."""
        result = self.tf._build_agg_student_summary(
            self.fact_diem, self.fact_rl, self.fact_tc,
            self.dk_df, self.hp_df
        )
        sv1 = result[result["ma_sinh_vien"] == self.SV1].iloc[0]
        self.assertTrue(sv1["du_dieu_kien_hoc_bong"])

    def test_empty_fact_diem(self):
        result = self.tf._build_agg_student_summary(
            pd.DataFrame(), self.fact_rl, self.fact_tc,
            self.dk_df, self.hp_df
        )
        self.assertTrue(result.empty)


if __name__ == "__main__":
    print("=" * 60)
    print("RUNNING test_transform.py")
    print("=" * 60)
    unittest.main(verbosity=2)