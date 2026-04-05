"""
Test 2: Chạy từng suite với data giả sạch
Chạy: python scripts/test_ge_suites.py --mock
"""
import pandas as pd
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.validation.ge_validation import _run_suite

print("=" * 50)
print("TEST 2: Validate với data giả sạch")
print("=" * 50)

# --- Tạo DataFrame giả cho từng suite ---

# Thêm 2 cột còn thiếu vào df_students:
df_students = pd.DataFrame({
    "ma_sinh_vien":       [f"B21DCCN{i:03d}" for i in range(1, 101)],
    "ho":                 ["Nguyễn Văn"] * 100,
    "ten":                ["An"] * 100,
    "ngay_sinh":          ["2003-01-15"] * 100,   # ← thêm
    "gioi_tinh":          ["Nam"] * 100,           # ← thêm
    "email":              [f"sv{i:03d}@student.ptit.edu.vn"
                           for i in range(1, 101)],
    "khoa_hoc":           ["B21"] * 100,
    "trang_thai_hoc_tap": ["Đang học"] * 100,
    "ma_nganh":           ["CNTT"] * 100,
    "ma_lop":             ["D21CQCN01-B"] * 100,
})

# grades_suite cần 1000 dòng:
df_grades = pd.DataFrame({
    "ma_diem":         list(range(1, 1001)),
    "ma_dang_ky":      list(range(1001, 2001)),
    "diem_chuyen_can": [8.5] * 1000,
    "diem_bai_tap":    [8.0] * 1000,
    "diem_giua_ky":    [7.5] * 1000,
    "diem_cuoi_ky":    [8.0] * 1000,
    "diem_tong_ket":   [8.0] * 1000,
    "diem_chu":        ["B+"] * 1000,
    "diem_he_4":       [3.5] * 1000,
    "dat_mon":         [True] * 1000,
    "hoc_lai":         [False] * 1000,
})

# warehouse_suite cần 100 dòng:
df_warehouse = pd.DataFrame({
    "ma_sinh_vien":    [f"B21DCCN{i:03d}" for i in range(1, 101)],
    "gpa_he_4":        [3.2] * 100,
    "muc_do_rui_ro":   ["Thấp"] * 100,
    "tong_no_hoc_phi": [0] * 100,
    "canh_bao_hoc_vu": [False] * 100,
})

# ctsv_suite
df_ctsv = pd.DataFrame({
    "ma_sinh_vien": ["B21DCCN001", "B21DCCN002"],
    "hoc_ky":       ["HK1-2024-25", "HK1-2024-25"],
    "diem_ren_luyen": [85.0, 72.0],
    "xep_loai_rl":  ["Tốt", "Khá"],
    "loai_hoc_bong": ["KKHT Loại Giỏi", ""],
    "muc_tien_hb":  [3600000, 0],
    "hinh_thuc_ky_luat": ["", ""],
    "ly_do_ky_luat": ["", ""],
})

# tai_chinh_suite
df_tc = pd.DataFrame({
    "ma_sinh_vien":       ["B21DCCN001", "B21DCCN002"],
    "hoc_ky":             ["HK1-2024-25", "HK1-2024-25"],
    "hoc_phi_phai_dong":  [7200000, 7200000],
    "da_dong":            [7200000, 5000000],
    "con_no":             [0, 2200000],
    "duoc_mien_giam":     [False, False],
    "ly_do_mien_giam":    ["", ""],
    "so_tien_mien_giam":  [0, 0],
    "ngay_dong_cuoi":     ["2024-10-01", "2024-10-15"],
})


# --- Chạy từng suite ---
suites = [
    (df_students,  "students_suite",  "mock_students"),
    (df_grades,    "grades_suite",    "mock_grades"),
    (df_ctsv,      "ctsv_suite",      "mock_ctsv"),
    (df_tc,        "tai_chinh_suite", "mock_tc"),
    (df_warehouse, "warehouse_suite", "mock_warehouse"),
]

all_pass = True
for df, suite_name, asset_name in suites:
    result = _run_suite(df, suite_name, asset_name)
    status = "✅ PASS" if result["success"] else "❌ FAIL"
    print(f"  {status} {suite_name}")
    print(f"         {result['passed']}/{result['evaluated']} expectations")
    if result["failures"]:
        for f in result["failures"]:
            print(f"         → {f}")
    if not result["success"]:
        all_pass = False

print()
print("KẾT QUẢ:", "✅ TẤT CẢ PASS" if all_pass else "❌ CÓ SUITE FAIL")