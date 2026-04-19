"""
generate_sample_data.py — PTIT School ETL Platform
===================================================
Version: 3.0 — "HK3 Summer Retake + Bug Fixes + Profile Evolution"

═══════════════════════════════════════════════════════════════════════
PHÂN TÍCH & SỬA LỖI TỪ v2.2 → v3.0
═══════════════════════════════════════════════════════════════════════

▌ BUG 1 (NGHIÊM TRỌNG) — HK_MODIFIER bị áp dụng 2 lần:
│  Trong _gen_diem_mon(), base += hk_modifier là áp dụng lần 1.
│  Nhưng _gen_diem_raw() không nhận hk_modifier, nó chỉ dùng profile.
│  Tuy nhiên, hk_idx += min((hk_idx-1)*0.10, 0.7) cộng thêm 1 bonus
│  độc lập với hk_modifier, nên KHÔNG phải double-apply.
│  → Bug thực sự: cc = random.uniform(...) + hk_modifier * 0.3  (dòng 583)
│    VÀ TRONG DK_BUF dùng trang_thai_dk cho TẤT CẢ môn của 1 SV trong
│    1 HK — điều này OK về logic nhưng cần document rõ.
│  ✅ FIX: Tách riêng hk_modifier cho cc vs các component khác
│         để tránh nhân đôi khi các HK có modifier âm lớn.

▌ BUG 2 (TRUNG BÌNH) — buoc_thoi_hoc_gpa_hard = 0.5 quá thấp:
│  Ngưỡng GPA 0.5 gần như không SV nào đạt được → thôi học = 0.
│  Theo quy chế PTIT thực tế: GPA tích lũy < 1.0 sau 2 HK → cảnh báo,
│  tiếp tục < 1.0 sau HK tiếp → thôi học. Hoặc GPA HK < 0.8 liên tiếp.
│  ✅ FIX: Đổi thành 0.80 (thực tế hơn, sẽ có ~2-3% SV thôi học).

▌ BUG 3 (NHẸ) — Profile học lực KHÔNG thay đổi theo thời gian:
│  Một SV "yếu" sẽ LUÔN yếu suốt 4-5 năm học → không thực tế.
│  Trong thực tế: ~8% SV yếu cải thiện lên trung bình mỗi năm,
│  ~3% SV khá/tốt sa sút xuống trung bình.
│  ✅ FIX: Thêm hàm _evolve_profile() gọi sau mỗi HK2.

▌ BUG 4 (NHẸ) — Học phí không tăng theo năm:
│  Tất cả năm đều random trong [440k, 460k, 480k, 500k] đồng/TC.
│  Thực tế PTIT tăng giá mỗi năm ~5%.
│  ✅ FIX: Thêm GIA_TC_BY_YEAR với giá tăng dần từ 2021.

▌ BUG 5 (NHẸ) — hk_idx bị offset khi thêm HK3 vào sequence:
│  Nếu HK3 được chèn vào HK_SEQ_BY_COHORT, enumerate() sẽ cho
│  hk_idx_0 sai (HK3 tính là 1 HK trong curriculum), dẫn đến
│  tra cứu sai mon_by_hk[hk_idx].
│  ✅ FIX: Dùng curriculum_hk_counter riêng, chỉ tăng cho HK1/HK2.

▌ TÍNH NĂNG MỚI — HK3 (Học kỳ Hè / Học lại):
│  ★ [HK3_SUMMER] Thêm HK3 cho 4 năm học 2021-22 đến 2024-25
│  ★ [HK3_RETAKE_LOGIC] Chỉ SV có môn rớt mới tham gia HK3
│  ★ [HK3_RATE] 55% SV rớt môn đăng ký học lại HK3 (tùy chọn)
│  ★ [HK3_MAX_COURSES] Tối đa 3 môn/SV trong HK3
│  ★ [HK3_FINANCE] Học phí HK3 theo tín chỉ, không có miễn giảm
│  ★ [HK3_CSV] CSV rèn luyện cho HK3 (điểm RL thấp hơn, không có HB)
│  ★ [HK3_DURATION] HK3 kéo dài 7 tuần (tháng 7-8)

═══════════════════════════════════════════════════════════════════════
GIỮ NGUYÊN TỪ v2.2 (không thay đổi):
═══════════════════════════════════════════════════════════════════════
★ [DUPLICATE_RECORDS]     ~1-2% records bị duplicate trong CSV/JSON
★ [TRANG_THAI_VARIANTS]   Biến thể format trang_thai (uppercase, không dấu)
★ [DATE_FORMAT_MISMATCH]  ~2.5% JSON dùng format ngày dd/mm/yyyy
★ [ORPHAN_RECORDS]        ~0.5% CSV/JSON có mã SV ma (không có trong PG)
★ [ENCODING_ISSUES]       ~0.5% lỗi encoding tiếng Việt trong CSV
★ [DYNAMIC_RECENT_HKS]    RECENT_HKS tính động từ HK_MASTER
★ [JSON_METADATA]         File JSON có wrapper metadata
★ [NATURAL_MISSING]       3-8% điểm HK gần nhất chưa nhập
★ [COURSE_WITHDRAWAL]     1.5% SV rút môn giữa HK
★ [PAYMENT_INSTALLMENT]   20-30% SV đóng học phí 2 đợt
★ [FINANCIAL_ROUNDING]    ±200-800 VND sai số tự nhiên trong con_no
★ [LATE_REGISTRATION]     3% đăng ký muộn
★ [GRADE_BOUNDARY]        Điểm tập trung gần ngưỡng đổi bậc
★ [LATE_GRADE_ENTRY]      GV nhập điểm muộn
★ [SKEWED_PAYMENT_DATE]   Ngày đóng tiền tập trung đầu tháng
"""

import os
import random
import json
import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple, Optional

from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Integer,
    Numeric, String, Text, UniqueConstraint, create_engine, func, text
)
from sqlalchemy.orm import declarative_base, Session

# ═══════════════════════════════════════════════════════
# KẾT NỐI
# ═══════════════════════════════════════════════════════
DB_URL = "postgresql+psycopg2://school_user:school_pass@localhost:5434/school_source"
engine = create_engine(DB_URL, echo=False)

random.seed(42)

OUTPUT_DIR = "data"
os.makedirs(f"{OUTPUT_DIR}/csv", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/api_json", exist_ok=True)

Base = declarative_base()

# ═══════════════════════════════════════════════════════
# CẤU HÌNH IMPERFECTION TỰ NHIÊN
# ═══════════════════════════════════════════════════════
NATURAL_IMPERFECTION = {
    "grade_not_entered_recent_hk": 0.045,
    "grade_not_entered_older_hk":  0.008,
    "course_withdrawal_rate":       0.015,
    "payment_installment_rate":     0.25,
    "payment_installment_first":    (0.55, 0.75),
    "financial_rounding_rate":      0.06,
    "financial_rounding_range":     (-600, 800),
    "late_registration_rate":       0.03,
    "late_registration_days":       (11, 22),
    "csv_missing_rl_rate":          0.04,
    "csv_trailing_space_rate":      0.02,
    "csv_late_submission_hk":       1,
    "boundary_score_boost":         0.08,
    "very_late_grade_entry_rate":   0.03,
    "late_grade_entry_rate":        0.12,
    "csv_duplicate_rate":           0.012,
    "json_duplicate_rate":          0.008,
    "trang_thai_variant_rate":      0.015,
    "json_date_format_rate":        0.025,
    "csv_orphan_rate":              0.004,
    "json_orphan_rate":             0.003,
    "csv_encoding_error_rate":      0.005,
}

GRADE_BOUNDARIES = [4.0, 5.0, 5.5, 6.5, 7.0, 8.0, 8.5, 9.5]

# ★ v3.0 — Tỷ lệ SV rớt môn tham gia học lại HK3
HK3_RETAKE_RATE = 0.55   # 55% SV có môn rớt đăng ký học HK3
HK3_MAX_COURSES = 3       # Tối đa 3 môn/HK3

# ★ v3.0 — Xác suất profile evolution mỗi năm học (sau HK2)
PROFILE_EVOLVE_UP_RATE   = 0.08  # 8% SV yếu/tb cải thiện 1 bậc
PROFILE_EVOLVE_DOWN_RATE = 0.03  # 3% SV giỏi/khá sa sút 1 bậc

# ★ v3.0 — Học phí theo năm (VND/tín chỉ, tăng ~5%/năm)
GIA_TC_BY_YEAR = {
    2021: 440_000,
    2022: 460_000,
    2023: 484_000,
    2024: 508_000,
    2025: 534_000,
}

# ═══════════════════════════════════════════════════════
# SQLALCHEMY MODELS
# ═══════════════════════════════════════════════════════
class Khoa(Base):
    __tablename__ = "khoa"
    ma_khoa  = Column(String(10), primary_key=True)
    ten_khoa = Column(String(200), nullable=False)
    ngay_tao = Column(DateTime, server_default=func.now())


class Nganh(Base):
    __tablename__ = "nganh"
    ma_nganh  = Column(String(20), primary_key=True)
    ten_nganh = Column(String(200), nullable=False)
    ma_khoa   = Column(String(10), ForeignKey("khoa.ma_khoa"), nullable=True)
    ngay_tao  = Column(DateTime, server_default=func.now())


class GiangVien(Base):
    __tablename__ = "giang_vien"
    ma_giang_vien       = Column(String(20), primary_key=True)
    ho                  = Column(String(50), nullable=False)
    ten                 = Column(String(50), nullable=False)
    email               = Column(String(100), unique=True, nullable=False)
    so_dien_thoai       = Column(String(15), nullable=True)
    chuc_danh           = Column(String(50), nullable=True)
    trang_thai_cong_tac = Column(String(20), default="Đang công tác")
    ma_khoa             = Column(String(10), ForeignKey("khoa.ma_khoa"), nullable=True)
    ngay_tao            = Column(DateTime, server_default=func.now())
    ngay_cap_nhat       = Column(DateTime, server_default=func.now())


class LopHanhChinh(Base):
    __tablename__ = "lop_hanh_chinh"
    ma_lop    = Column(String(20), primary_key=True)
    ten_lop   = Column(String(100), nullable=False)
    khoa_hoc  = Column(String(10), nullable=False)
    ma_nganh  = Column(String(20), ForeignKey("nganh.ma_nganh"), nullable=True)
    ma_co_van = Column(String(20), ForeignKey("giang_vien.ma_giang_vien"), nullable=True)
    ngay_tao  = Column(DateTime, server_default=func.now())


class SinhVien(Base):
    __tablename__ = "sinh_vien"
    ma_sinh_vien       = Column(String(20), primary_key=True)
    ho                 = Column(String(50), nullable=False)
    ten                = Column(String(50), nullable=False)
    ngay_sinh          = Column(Date, nullable=False)
    gioi_tinh          = Column(String(10), nullable=True)
    email              = Column(String(100), unique=True, nullable=False)
    ma_nganh           = Column(String(20), ForeignKey("nganh.ma_nganh"), nullable=True)
    ma_lop             = Column(String(20), ForeignKey("lop_hanh_chinh.ma_lop"), nullable=True)
    khoa_hoc           = Column(String(10), nullable=False)
    trang_thai_hoc_tap = Column(String(30), default="Đang học")
    ngay_tao           = Column(DateTime, server_default=func.now())


class HocPhan(Base):
    __tablename__ = "hoc_phan"
    ma_hoc_phan      = Column(String(20), primary_key=True)
    ma_mon           = Column(String(10), unique=True, nullable=False)
    ten_mon          = Column(String(200), nullable=False)
    so_tin_chi       = Column(Integer, nullable=False)
    so_gio_ly_thuyet = Column(Integer, default=0)
    so_gio_thuc_hanh = Column(Integer, default=0)
    hoc_ky_de_xuat   = Column(Integer, nullable=True)
    bat_buoc         = Column(Boolean, default=True)
    ma_khoa          = Column(String(10), ForeignKey("khoa.ma_khoa"), nullable=True)
    ngay_tao         = Column(DateTime, server_default=func.now())


class HocKyNamHoc(Base):
    __tablename__ = "hoc_ky_nam_hoc"
    ma_hoc_ky     = Column(String(50), primary_key=True)
    nam_hoc       = Column(String(50), nullable=False)
    hoc_ky        = Column(String(50), nullable=False)
    ngay_bat_dau  = Column(Date, nullable=True)
    ngay_ket_thuc = Column(Date, nullable=True)


class DangKyHocPhan(Base):
    __tablename__ = "dang_ky_hoc_phan"
    ma_dang_ky    = Column(Integer, primary_key=True, autoincrement=True)
    ma_sinh_vien  = Column(String(20), ForeignKey("sinh_vien.ma_sinh_vien"), nullable=False)
    ma_hoc_phan   = Column(String(20), ForeignKey("hoc_phan.ma_hoc_phan"), nullable=False)
    ma_hoc_ky     = Column(String(50), ForeignKey("hoc_ky_nam_hoc.ma_hoc_ky"), nullable=False)
    ma_giang_vien = Column(String(20), ForeignKey("giang_vien.ma_giang_vien"), nullable=True)
    ngay_dang_ky  = Column(Date, server_default=func.current_date())
    trang_thai    = Column(String(30), default="Đã đăng ký")
    __table_args__ = (
        UniqueConstraint("ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky",
                         name="uq_dang_ky_sv_hp_hk"),
    )


class DiemHocPhan(Base):
    __tablename__ = "diem_hoc_phan"
    ma_diem         = Column(Integer, primary_key=True, autoincrement=True)
    ma_dang_ky      = Column(Integer, ForeignKey("dang_ky_hoc_phan.ma_dang_ky"), unique=True)
    diem_chuyen_can = Column(Numeric(4, 2), nullable=True)
    diem_bai_tap    = Column(Numeric(4, 2), nullable=True)
    diem_giua_ky    = Column(Numeric(4, 2), nullable=True)
    diem_cuoi_ky    = Column(Numeric(4, 2), nullable=True)
    diem_tong_ket   = Column(Numeric(4, 2), nullable=True)
    diem_chu        = Column(String(2), nullable=True)
    diem_he_4       = Column(Numeric(3, 2), nullable=True)
    dat_mon         = Column(Boolean, nullable=True)
    hoc_lai         = Column(Boolean, default=False)
    ngay_cham       = Column(DateTime, nullable=True)
    ngay_tao        = Column(DateTime, server_default=func.now())


class TongHopKetQua(Base):
    __tablename__ = "tong_hop_ket_qua"
    ma_sinh_vien     = Column(String(20), ForeignKey("sinh_vien.ma_sinh_vien"), primary_key=True)
    tong_tin_chi     = Column(Integer, default=0)
    tin_chi_tich_luy = Column(Integer, default=0)
    gpa_he_10        = Column(Numeric(4, 2), nullable=True)
    gpa_he_4         = Column(Numeric(3, 2), nullable=True)
    canh_bao_hoc_vu  = Column(Boolean, default=False)
    ngay_cap_nhat    = Column(DateTime, server_default=func.now())


# ═══════════════════════════════════════════════════════
# DỮ LIỆU CỐ ĐỊNH
# ═══════════════════════════════════════════════════════
KHOA_DATA = [
    ("CNTT1", "Khoa Công nghệ thông tin 1"),
    ("KKT",   "Khoa Kế toán - Kiểm toán"),
    ("KVT",   "Khoa Viễn thông 1"),
    ("KCB",   "Khoa Cơ bản"),
]

HO_LIST = [
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ",
    "Võ", "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý",
    "Đào", "Đinh", "Trương", "Lương", "Mai", "Tạ", "Trịnh", "Lâm",
    "Cao", "Hà", "Tống", "Nghiêm", "La", "Chu"
]
TEN_DEM_NAM = ["Văn", "Đức", "Minh", "Quang", "Xuân", "Hữu", "Thanh", "Đình",
               "Công", "Tiến", "Duy", "Quốc", "Bá", "Ngọc", "Anh", "Trung",
               "Hoàng", "Danh", "Trọng", "Huy"]
TEN_DEM_NU  = ["Thị", "Ngọc", "Thanh", "Phương", "Thuỷ", "Kim", "Hoàng", "Bích",
               "Quỳnh", "Diệu", "Mỹ", "Hà", "Khánh", "Lan", "Tuyết", "Minh"]
TEN_NAM = ["Anh", "Bình", "Cường", "Đạt", "Dũng", "Đức", "Hải", "Hiếu",
           "Hoàng", "Hùng", "Huy", "Khải", "Khánh", "Khoa", "Long",
           "Mạnh", "Minh", "Nam", "Nghĩa", "Nhân", "Phong", "Phúc",
           "Quân", "Quang", "Sơn", "Tài", "Thành", "Thắng", "Thiên",
           "Thịnh", "Tiến", "Tín", "Trung", "Tuấn", "Vinh", "Vũ",
           "Bảo", "Doanh", "Lâm", "Phát"]
TEN_NU = ["Anh", "Chi", "Dung", "Hà", "Hân", "Hằng", "Hạnh", "Hoa",
          "Hương", "Khánh", "Lan", "Linh", "Mai", "Mỹ", "Nga",
          "Ngân", "Ngọc", "Nhi", "Nhung", "Phương", "Quyên", "Sương",
          "Tâm", "Thanh", "Thảo", "Thi", "Thu", "Thuỷ", "Trang",
          "Trúc", "Tuyết", "Uyên", "Vân", "Vy", "Yến", "Diễm",
          "Giang", "Hiền", "Huyền", "Ly"]

CHUC_DANH   = ["ThS", "TS", "PGS.TS", "GS.TS"]
CHUC_DANH_W = [50, 35, 12, 3]

HB_QUOTA_RATE = 0.10
HB_TIERS = [
    {"loai": "KKHT Loại Xuất sắc", "min_drl": 90, "min_gpa": 3.60, "muc_tien": 8_000_000},
    {"loai": "KKHT Loại Giỏi",     "min_drl": 80, "min_gpa": 3.20, "muc_tien": 3_600_000},
    {"loai": "KKHT Loại Khá",      "min_drl": 65, "min_gpa": 2.50, "muc_tien": 1_200_000},
]

HK_MODIFIER = {
    # HK chính quy
    "HK1-2021-22": +0.40,
    "HK2-2021-22": -0.30,
    "HK1-2022-23": -0.60,
    "HK2-2022-23": +0.60,
    "HK1-2023-24":  0.00,
    "HK2-2023-24": -0.80,
    "HK1-2024-25": +0.30,
    "HK2-2024-25": -0.20,
    "HK1-2025-26": +0.10,
    
    "HK3-2021-22": +0.50,
    "HK3-2022-23": +0.45,
    "HK3-2023-24": +0.40,
    "HK3-2024-25": +0.35,
}

# ═══════════════════════════════════════════════════════
# CHƯƠNG TRÌNH ĐÀO TẠO
# ═══════════════════════════════════════════════════════
CHUONG_TRINH_KT = {
    1: {"bat_buoc": [("DC100","Triết học Mác-Lênin",2,30,0,0.0),("KT002","Kinh tế vi mô 1",3,45,0,1.0),("DC106","Tin học cơ sở 1",2,20,20,0.5),("KT004","Toán cao cấp 1",2,30,0,1.0),("DC105","Pháp luật đại cương",2,30,0,0.3)],"tu_chon":[],"so_chon":0},
    2: {"bat_buoc": [("DC101","Kinh tế chính trị Mác-Lênin",2,30,0,0.0),("DC107","Tiếng Anh (Course 1)",4,60,0,0.8),("KT008","Toán cao cấp 2",2,30,0,1.8),("KT009","Lý thuyết xác suất và thống kê",3,45,0,2.0),("KT010","Tin học cơ sở 3",3,30,20,0.8),("KT011","Kinh tế vĩ mô 1",3,45,0,1.0)],"tu_chon":[],"so_chon":0},
    3: {"bat_buoc": [("DC103","Tư tưởng Hồ Chí Minh",2,30,0,0.0),("DC108","Tiếng Anh (Course 2)",4,60,0,0.8),("KT014","Toán kinh tế",3,45,0,1.5),("KT015","Nguyên lý kế toán",3,45,0,1.3),("KT016","Marketing căn bản",3,45,0,0.8),("KT017","Quản trị học",3,45,0,0.8)],"tu_chon":[],"so_chon":0},
    4: {"bat_buoc": [("DC104","Lịch sử Đảng cộng sản VN",3,45,0,0.0),("DC109","Tiếng Anh (Course 3)",4,60,0,1.0),("KT020","Kế toán quản trị 1",3,30,20,1.3),("KT021","Kế toán tài chính 1",3,30,20,1.5),("KT022","Tài chính tiền tệ",3,45,0,1.2),("KT023","Luật kinh doanh",4,60,0,0.5),("KT024","Quản trị tài chính doanh nghiệp",3,45,0,1.3)],"tu_chon":[],"so_chon":0},
    5: {"bat_buoc": [("KT025","Tiếng Anh A22/B12",4,60,0,1.0),("KT026","Thanh toán quốc tế",2,30,0,1.0),("KT027","Kiểm toán căn bản",3,45,0,1.3),("KT028","Kế toán tài chính 2",3,30,20,1.8),("KT029","Kế toán quản trị 2",3,30,20,1.5),("KT030","Hệ thống thông tin kế toán",3,30,20,1.2)],"tu_chon":[],"so_chon":0},
    6: {"bat_buoc": [("KT031","Nguyên lý thống kê kinh tế",3,45,0,1.2),("KT032","ACCA",3,45,0,1.5),("KT033","Thuế và kế toán thuế",3,45,0,1.3),("KT034","Phân tích báo cáo tài chính DN",2,30,0,1.5),("KT035","Kế toán tài chính 3",3,30,20,2.0)],"tu_chon":[("KT036","Kế toán ngân hàng",2,30,0,1.0),("KT037","Kế toán hành chính sự nghiệp",2,30,0,1.0),("KT038","Kế toán hợp nhất",2,30,0,1.2)],"so_chon":3},
    7: {"bat_buoc": [("KT039","Phương pháp luận NCKH",2,30,0,0.5),("KT040","Phân tích hoạt động kinh doanh",2,30,0,1.2),("KT041","CFA",3,45,0,1.5),("KT042","Kế toán máy",2,20,20,1.0),("KT043","Kiểm toán tài chính",3,30,20,1.8)],"tu_chon":[("KT044","Kế toán quốc tế",2,30,0,1.0),("KT045","Định giá tài sản",2,30,0,1.2),("KT046","Kế toán môi trường",2,30,0,0.8)],"so_chon":3},
    8: {"bat_buoc": [("KT047","Thực tập và tốt nghiệp",10,0,150,0.5)],"tu_chon":[],"so_chon":0},
}

CHUONG_TRINH_VT = {
    1: {"bat_buoc": [("DC100","Triết học Mác-Lênin",2,30,0,0.0),("DC106","Tin học cơ sở 1",2,20,20,0.5),("DC001","Giải tích 1",3,45,0,2.5),("DC003","Đại số",3,45,0,2.0)],"tu_chon":[],"so_chon":0},
    2: {"bat_buoc": [("DC101","Kinh tế chính trị Mác-Lênin",2,30,0,0.0),("DC107","Tiếng Anh (Course 1)",4,60,0,0.8),("DC008","Tin học cơ sở 2",2,20,20,0.8),("DC002","Giải tích 2",3,45,0,2.5),("DC006","Vật lý đại cương",4,45,15,1.8),("DC007","Xác suất thống kê",2,30,0,1.5)],"tu_chon":[],"so_chon":0},
    3: {"bat_buoc": [("DC102","Chủ nghĩa xã hội khoa học",2,30,0,0.0),("DC108","Tiếng Anh (Course 2)",4,60,0,0.8),("VT013","Tín hiệu và hệ thống",3,30,20,1.8),("VT014","Vật lý và Thí nghiệm",4,45,15,1.5),("VT015","Lý thuyết mạch",3,30,20,1.8),("VT016","Linh kiện và mạch điện tử",3,30,20,1.5)],"tu_chon":[],"so_chon":0},
    4: {"bat_buoc": [("DC103","Tư tưởng Hồ Chí Minh",2,30,0,0.0),("DC109","Tiếng Anh (Course 3)",4,60,0,1.0),("DC004","Xử lý tín hiệu số",3,20,20,1.8),("VT020","Kỹ thuật siêu cao tần",3,30,20,2.2),("VT021","Điện tử số",3,20,20,1.5),("VT022","Lý thuyết truyền tin",3,45,0,1.8)],"tu_chon":[],"so_chon":0},
    5: {"bat_buoc": [("VT023","Tiếng Anh (Course 3 Plus)",2,30,0,0.8),("DC104","Lịch sử Đảng cộng sản VN",2,30,0,0.0),("VT025","Truyền sóng và anten",3,30,20,1.8),("VT026","Toán rời rạc",3,45,0,1.5),("VT027","Kỹ thuật lập trình",3,30,30,1.3),("DC005","Kiến trúc máy tính",3,30,20,1.3),("VT029","Kỹ thuật vi xử lý",3,30,20,1.5)],"tu_chon":[],"so_chon":0},
    6: {"bat_buoc": [("VT030","Hệ điều hành",2,20,20,1.3),("VT031","Cấu trúc dữ liệu và giải thuật",3,30,30,2.0),("VT032","Kỹ thuật thông tin quang",3,30,20,1.5),("VT033","Kỹ thuật mạng truyền thông",3,30,20,1.3),("VT034","Kỹ thuật thông tin vô tuyến",2,20,20,1.5),("VT035","Công nghệ phần mềm",3,30,20,1.0),("VT036","Mô phỏng hệ thống truyền thông",2,10,30,1.3)],"tu_chon":[],"so_chon":0},
    7: {"bat_buoc": [("VT037","Phương pháp luận NCKH",2,30,0,0.5),("VT038","Internet và các giao thức",3,30,20,1.3),("VT039","Mạng truyền thông và quang",3,30,20,1.5),("VT040","Thông tin di động",3,30,20,1.3),("VT041","An toàn mạng thông tin",3,30,20,1.5),("VT042","Cơ sở dữ liệu",3,30,20,1.5)],"tu_chon":[],"so_chon":0},
    8: {"bat_buoc": [("VT043","Điện toán và đám mây",2,20,20,1.2),("VT044","Lập trình hướng đối tượng",3,30,30,1.3),("VT045","Chuyên đề mạng và dịch vụ Internet",1,10,10,1.0)],"tu_chon":[("VT046","Thiết kế mạng viễn thông",2,20,20,1.2),("VT047","Xử lý ảnh số",2,20,20,1.5),("VT048","Mạng cảm biến",3,30,20,1.3),("VT049","An ninh mạng",3,30,20,1.5),("VT050","IoT cơ bản",3,20,30,1.2)],"so_chon":5},
    9: {"bat_buoc": [("VT051","Thực tập và tốt nghiệp",12,0,180,0.5)],"tu_chon":[],"so_chon":0},
}

CHUONG_TRINH_CN = {
    1: {"bat_buoc": [("DC100","Triết học Mác-Lênin",2,30,0,0.0),("DC106","Tin học cơ sở 1",2,20,20,0.5),("DC001","Giải tích 1",3,45,0,2.5),("DC003","Đại số",3,45,0,2.0)],"tu_chon":[],"so_chon":0},
    2: {"bat_buoc": [("DC101","Kinh tế chính trị Mác-Lênin",2,30,0,0.0),("DC107","Tiếng Anh (Course 1)",4,60,0,0.8),("DC008","Tin học cơ sở 2",2,20,20,0.8),("DC002","Giải tích 2",3,45,0,2.5),("DC105","Pháp luật đại cương",2,30,0,0.3),("DC006","Vật lý đại cương",4,45,15,1.8),("CN011","Kỹ thuật số",2,20,10,1.2)],"tu_chon":[],"so_chon":0},
    3: {"bat_buoc": [("DC102","Chủ nghĩa xã hội khoa học",2,30,0,0.0),("DC108","Tiếng Anh (Course 2)",4,60,0,0.8),("CN014","Ngôn ngữ lập trình C++",3,30,30,1.5),("CN015","Toán rời rạc 1",3,45,0,1.8),("DC004","Xử lý tín hiệu số",2,20,10,1.5),("DC007","Xác suất thống kê",3,45,0,1.5)],"tu_chon":[],"so_chon":0},
    4: {"bat_buoc": [("DC103","Tư tưởng Hồ Chí Minh",2,30,0,0.0),("DC109","Tiếng Anh (Course 3)",4,60,0,1.0),("DC005","Kiến trúc máy tính",3,30,20,1.3),("CN021","Toán rời rạc 2",3,45,0,2.0),("CN022","Cấu trúc dữ liệu và giải thuật",3,30,30,2.0),("CN023","Lý thuyết thông tin",3,45,0,1.5)],"tu_chon":[],"so_chon":0},
    5: {"bat_buoc": [("DC104","Lịch sử Đảng cộng sản VN",2,30,0,0.0),("CN025","Tiếng Anh (Course 3 Plus)",2,30,0,0.8),("CN026","Hệ điều hành",3,30,20,1.3),("CN027","Lập trình hướng đối tượng",3,30,30,1.3),("CN028","Cơ sở dữ liệu",3,30,20,1.5),("CN029","Mạng máy tính",3,30,20,1.2),("CN030","Lập trình Python",3,30,30,1.0)],"tu_chon":[],"so_chon":0},
    6: {"bat_buoc": [("CN031","Nhập môn công nghệ phần mềm",3,30,20,1.0),("CN032","Nhập môn trí tuệ nhân tạo",3,30,20,1.5),("CN033","An toàn và bảo mật HTTT",3,30,20,1.3),("CN034","Lập trình web",3,20,40,1.2),("CN035","Cơ sở dữ liệu phân tán",3,30,20,1.8),("CN036","Thực tập cơ sở",4,0,60,0.5)],"tu_chon":[],"so_chon":0},
    7: {"bat_buoc": [("CN037","QLDA phần mềm",3,30,20,1.0),("CN038","IoT và ứng dụng",3,20,40,1.3),("CN039","Phân tích và thiết kế HTTT",3,30,20,1.5),("CN040","Xử lý ảnh",3,20,40,1.8)],"tu_chon":[("CN041","Kiến trúc và thuật toán song song",3,30,20,1.2),("CN042","Hệ CSDL đa phương tiện",3,30,20,1.2),("CN043","Mạng viễn thông thế hệ mới",3,30,20,1.2)],"so_chon":2},
    8: {"bat_buoc": [("CN044","Thiết kế mạng máy tính",3,30,20,1.3),("CN045","Đánh giá hiệu năng mạng",3,30,20,1.5),("CN046","Quản lý mạng máy tính",3,30,20,1.2),("CN047","An ninh mạng",3,30,20,1.5)],"tu_chon":[("CN048","Điện toán đám mây",3,30,20,1.2),("CN049","Nhập môn khoa học dữ liệu",3,30,20,1.2),("CN050","Các hệ thống phân tán",3,30,20,1.3),("CN051","Phương pháp luận NCKH",2,30,0,0.5)],"so_chon":2},
    9: {"bat_buoc": [("CN052","Thực tập và tốt nghiệp",12,0,180,0.5)],"tu_chon":[],"so_chon":0},
}

# ═══════════════════════════════════════════════════════
# CẤU HÌNH HỌC KỲ & NGÀNH
# ═══════════════════════════════════════════════════════
NGANH_CONFIG = {
    "KT": {"ma_nganh": "KETOAN", "ten_nganh": "Kế toán",                     "ma_khoa": "KKT",   "ma_viet_tat": "KT", "chuong_trinh": CHUONG_TRINH_KT, "max_hk": 8, "so_lop_per_khoa": 4, "ty_le_nam": 0.35, "do_kho_nganh":  0.0},
    "VT": {"ma_nganh": "DTVT",   "ten_nganh": "Kỹ thuật Điện tử viễn thông", "ma_khoa": "KVT",   "ma_viet_tat": "VT", "chuong_trinh": CHUONG_TRINH_VT, "max_hk": 9, "so_lop_per_khoa": 4, "ty_le_nam": 0.75, "do_kho_nganh": -0.2},
    "CN": {"ma_nganh": "CNTT",   "ten_nganh": "Công nghệ thông tin",          "ma_khoa": "CNTT1", "ma_viet_tat": "CN", "chuong_trinh": CHUONG_TRINH_CN, "max_hk": 9, "so_lop_per_khoa": 4, "ty_le_nam": 0.85, "do_kho_nganh": -0.4},
}

SV_PER_KHOA = {"B21": 130, "B22": 140, "B23": 145, "B24": 150}

COHORT_CONFIG = {
    "B21": (2021, 9, 9),
    "B22": (2022, 7, 7),
    "B23": (2023, 5, 5),
    "B24": (2024, 3, 3),
}

EVAL_CRITERIA = {
    "canh_bao_1_gpa": 1.5,
    "canh_bao_2_gpa": 1.2,
    # ★ v3.0 FIX BUG 2: Đổi từ 0.5 → 0.80 (thực tế hơn, ~2-3% SV thôi học)
    # Ngưỡng 0.5 quá thấp → gần như 0 SV thôi học; 0.80 phản ánh đúng quy chế PTIT
    "buoc_thoi_hoc_gpa_hard": 0.80,
    "ap_dung_tu_hk": 2,
    "bao_luu_gpa_threshold": 1.5,
    "bao_luu_prob_yeu": 0.08,
    "bao_luu_prob_tb": 0.03,
    "tot_nghiep_gpa_min": 2.2,
}

PROFILE_WEIGHTS = {
    "xuất sắc": 5,
    "giỏi": 18,
    "khá": 48,
    "trung bình": 21,
    "yếu": 8,
}

# ★ v3.0 — Thứ tự profile để hỗ trợ evolution
PROFILE_ORDER = ["yếu", "trung bình", "khá", "giỏi", "xuất sắc"]

HK_MASTER = {
    # Học kỳ chính quy
    "HK1-2021-22": ("2021-2022", "Học kỳ 1", date(2021, 9, 6),  date(2022, 1, 15)),
    "HK2-2021-22": ("2021-2022", "Học kỳ 2", date(2022, 2, 14), date(2022, 6, 30)),
    "HK1-2022-23": ("2022-2023", "Học kỳ 1", date(2022, 9, 5),  date(2023, 1, 14)),
    "HK2-2022-23": ("2022-2023", "Học kỳ 2", date(2023, 2, 13), date(2023, 6, 30)),
    "HK1-2023-24": ("2023-2024", "Học kỳ 1", date(2023, 9, 4),  date(2024, 1, 13)),
    "HK2-2023-24": ("2023-2024", "Học kỳ 2", date(2024, 2, 12), date(2024, 6, 28)),
    "HK1-2024-25": ("2024-2025", "Học kỳ 1", date(2024, 9, 2),  date(2025, 1, 11)),
    "HK2-2024-25": ("2024-2025", "Học kỳ 2", date(2025, 2, 10), date(2025, 6, 27)),
    "HK1-2025-26": ("2025-2026", "Học kỳ 1", date(2025, 9, 1),  date(2026, 2, 8)),
    # ★ v3.0 — HK3 hè (7 tuần, tháng 7-8 hàng năm)
    # Chỉ mở cho SV học lại/cải thiện điểm; không có môn mới trong chương trình
    "HK3-2021-22": ("2021-2022", "Học kỳ Hè", date(2022, 7, 11), date(2022, 8, 31)),
    "HK3-2022-23": ("2022-2023", "Học kỳ Hè", date(2023, 7, 10), date(2023, 8, 31)),
    "HK3-2023-24": ("2023-2024", "Học kỳ Hè", date(2024, 7, 8),  date(2024, 8, 30)),
    "HK3-2024-25": ("2024-2025", "Học kỳ Hè", date(2025, 7, 7),  date(2025, 8, 29)),
}

# ★ v3.0 — HK3 được chèn SAU HK2 của cùng năm học
# Lưu ý: hk_idx trong curriculum KHÔNG tăng khi gặp HK3
# (HK3 không thuộc curriculum, chỉ là học lại)
HK_SEQ_BY_COHORT = {
    "B21": [
        "HK1-2021-22","HK2-2021-22","HK3-2021-22",
        "HK1-2022-23","HK2-2022-23","HK3-2022-23",
        "HK1-2023-24","HK2-2023-24","HK3-2023-24",
        "HK1-2024-25","HK2-2024-25","HK3-2024-25",
        "HK1-2025-26",
    ],
    "B22": [
        "HK1-2022-23","HK2-2022-23","HK3-2022-23",
        "HK1-2023-24","HK2-2023-24","HK3-2023-24",
        "HK1-2024-25","HK2-2024-25","HK3-2024-25",
        "HK1-2025-26",
    ],
    "B23": [
        "HK1-2023-24","HK2-2023-24","HK3-2023-24",
        "HK1-2024-25","HK2-2024-25","HK3-2024-25",
        "HK1-2025-26",
    ],
    "B24": [
        "HK1-2024-25","HK2-2024-25","HK3-2024-25",
        "HK1-2025-26",
    ],
}

# ★ [DYNAMIC_RECENT_HKS] — Tính động, kể cả HK3
_TODAY = date.today()
RECENT_HKS = {
    ma_hk
    for ma_hk, (_, _, _, end_date) in HK_MASTER.items()
    if (_TODAY - end_date).days < 180
}
if not RECENT_HKS:
    all_hk_sorted = sorted(HK_MASTER.keys(), key=lambda k: HK_MASTER[k][3], reverse=True)
    RECENT_HKS = set(all_hk_sorted[:2])

TRANG_THAI_VARIANTS = {
    "Đã đăng ký": ["DA DANG KY", "da dang ky", "Registered",  "DANG KY"],
    "Đã rút":     ["DA RUT",     "da rut",     "Withdrawn",    "RUT MON"],
}

_GHOST_IDS_CSV  = [f"B19DCKT{i:03d}" for i in range(1, 8)]
_GHOST_IDS_JSON = [f"B19DCCN{i:03d}" for i in range(1, 6)]

_ENCODING_CORRUPTIONS = [
    ("ễ", "?"), ("ắ", "?"), ("ồ", "?"), ("ổ", "?"), ("ọ", "?"),
    ("ơ", "o"), ("ư", "u"), ("đ", "d"), ("ả", "a"), ("ẽ", "e"),
]

GV_DATA = {
    "KKT":   [("Nguyễn","Văn Thành"),("Trần","Thị Hương"),("Lê","Quốc Bình"),("Phạm","Minh Tuấn"),("Hoàng","Thị Lan"),("Vũ","Đức Mạnh"),("Đỗ","Thị Ngọc"),("Bùi","Văn Hải"),("Ngô","Thị Mai"),("Đặng","Quốc Hùng")],
    "KVT":   [("Nguyễn","Hữu Đức"),("Trần","Quang Vinh"),("Lê","Minh Sơn"),("Phạm","Thị Hoa"),("Hoàng","Văn Long"),("Vũ","Thị Hiền"),("Đỗ","Anh Tuấn"),("Bùi","Quốc Đạt"),("Ngô","Văn Phong"),("Đặng","Thị Thu")],
    "CNTT1": [("Nguyễn","Minh Hiếu"),("Trần","Văn Cường"),("Lê","Thị Phương"),("Phạm","Đức Anh"),("Hoàng","Quốc Trung"),("Vũ","Văn Nam"),("Đỗ","Thị Linh"),("Bùi","Minh Khoa"),("Ngô","Đức Thịnh"),("Đặng","Văn Huy")],
    "KCB":   [("Nguyễn","Thị Hạnh"),("Trần","Đức Thắng"),("Lê","Văn Khánh"),("Phạm","Thị Dung"),("Hoàng","Minh Trí"),("Vũ","Quốc Phương"),("Đỗ","Thị Hằng"),("Bùi","Văn Tâm"),("Ngô","Thị Thuỷ"),("Đặng","Minh Đức"),("Lê","Thuỷ Tiên"),("Phạm","Anh Thư"),("Hoàng","Bích Ngọc"),("Trần","Quốc Việt"),("Nguyễn","Thị Huyền")],
}


# ═══════════════════════════════════════════════════════
# HELPERS — ĐIỂM SỐ
# ═══════════════════════════════════════════════════════

def _diem_sang_chu(d: float):
    if d >= 9.5: return "A+", 4.0
    if d >= 8.5: return "A",  3.7
    if d >= 8.0: return "B+", 3.5
    if d >= 7.0: return "B",  3.0
    if d >= 6.5: return "C+", 2.5
    if d >= 5.5: return "C",  2.0
    if d >= 5.0: return "D+", 1.5
    if d >= 4.0: return "D",  1.0
    return "F", 0.0


def _snap_to_boundary(score: float) -> float:
    """Kéo điểm về gần ngưỡng ranh giới (tự nhiên hơn)."""
    if random.random() > NATURAL_IMPERFECTION["boundary_score_boost"]:
        return score
    for boundary in GRADE_BOUNDARIES:
        if boundary - 0.4 <= score < boundary - 0.05:
            snapped = boundary + round(random.uniform(0.05, 0.35), 2)
            return min(snapped, 10.0)
    return score


def _gen_diem_raw(profile: str) -> float:
    """Sinh điểm thô theo profile, thêm noise tự nhiên."""
    r = random.random()
    if profile == "xuất sắc":
        tbl = [(0.20, 9.5, 10.0),(0.65, 8.5, 9.49),(0.85, 7.5, 8.49),(0.95, 6.0, 7.49),(1.00, 5.0, 5.99)]
    elif profile == "giỏi":
        tbl = [(0.07, 9.5, 10.0),(0.32, 8.5, 9.49),(0.67, 7.5, 8.49),(0.90, 6.0, 7.49),(1.00, 5.0, 5.99)]
    elif profile == "khá":
        tbl = [(0.03, 9.5, 10.0),(0.12, 8.5, 9.49),(0.37, 7.0, 8.49),(0.72, 5.5, 6.99),(1.00, 4.0, 5.49)]
    elif profile == "yếu":
        tbl = [(0.03, 7.0, 10.0),(0.10, 5.5, 6.99),(0.38, 4.0, 5.49),(0.65, 3.0, 3.99),(1.00, 2.0, 2.99)]
    else:  # trung bình
        tbl = [(0.02, 9.5, 10.0),(0.07, 8.5, 9.49),(0.22, 6.5, 8.49),(0.60, 5.0, 6.49),(0.85, 4.0, 4.99),(1.00, 3.0, 3.99)]

    base = 5.0
    for thr, lo, hi in tbl:
        if r < thr:
            base = round(random.uniform(lo, hi), 2)
            break
    noise = round(random.gauss(0, 0.08), 2)
    return round(max(0.0, min(10.0, base + noise)), 2)


def _gen_diem_mon(profile: str, do_kho: float, hk_idx: int,
                  la_hoc_lai: bool, hk_modifier: float = 0.0) -> float:
    """
    Sinh điểm cho 1 thành phần (BT, GK, CK).
    ★ v3.0 FIX: hk_modifier áp dụng ĐÃ ĐƯỢC CAP để tránh kéo điểm
    xuống quá mạnh khi modifier âm lớn (ví dụ HK2-2023-24 = -0.8).
    """
    p = "trung bình" if (la_hoc_lai and profile == "yếu") else profile
    base = _gen_diem_raw(p)
    base -= do_kho * 0.25
    # Bonus kinh nghiệm học kỳ (tối đa +0.7 ở HK8+)
    base += min((hk_idx - 1) * 0.10, 0.7)
    # ★ v3.0 FIX: Cap hk_modifier effect để tránh âm cực (max -0.5, +0.5 per component)
    capped_mod = max(-0.5, min(0.5, hk_modifier * 0.6))
    base += capped_mod
    if la_hoc_lai:
        base += 1.2   # bonus học lại (SV ôn kỹ hơn)
    base = _snap_to_boundary(base)
    return round(max(0.0, min(10.0, base)), 2)


def _tao_diem_record(ma_dk, do_kho: float, profile: str, hk_idx: int,
                     la_hoc_lai: bool, ngay_ket_thuc_hk: date,
                     hk_modifier: float = 0.0,
                     is_recent_hk: bool = False) -> dict:
    """Tạo record điểm đầy đủ cho 1 lượt đăng ký."""
    missing_rate = (
        NATURAL_IMPERFECTION["grade_not_entered_recent_hk"] if is_recent_hk
        else NATURAL_IMPERFECTION["grade_not_entered_older_hk"]
    )
    if random.random() < missing_rate:
        return {
            "ma_dang_ky": ma_dk,
            "diem_chuyen_can": None, "diem_bai_tap": None,
            "diem_giua_ky": None, "diem_cuoi_ky": None,
            "diem_tong_ket": None, "diem_chu": None,
            "diem_he_4": None, "dat_mon": None,
            "hoc_lai": la_hoc_lai, "ngay_cham": None,
            "_grade_not_entered": True,
        }

    # ★ v3.0 FIX: cc riêng không nhân đôi modifier như trong v2.2
    if profile == "xuất sắc":
        cc = round(random.uniform(9.0, 10.0), 2)
    elif profile == "giỏi":
        cc = round(random.uniform(8.5, 10.0), 2)
    elif profile == "khá":
        cc = round(max(0.0, random.uniform(7.0, 9.5) - do_kho * 0.05), 2)
    elif profile == "yếu":
        cc = round(max(0.0, random.uniform(5.0, 8.5) - do_kho * 0.2), 2)
    else:
        cc = round(max(0.0, random.uniform(6.5, 9.5) - do_kho * 0.1), 2)
    # Chuyên cần bị ảnh hưởng nhẹ bởi modifier HK (đi học ít hơn khi HK khó)
    cc = round(max(0.0, min(10.0, cc + hk_modifier * 0.15)), 2)

    bt  = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai, hk_modifier)
    gk  = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai, hk_modifier)
    ck  = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai, hk_modifier)

    dtk = round(max(0.0, min(10.0, 0.1*cc + 0.1*bt + 0.2*gk + 0.6*ck)), 2)
    dtk = _snap_to_boundary(dtk)
    dtk = round(max(0.0, min(10.0, dtk)), 2)
    chu, he4 = _diem_sang_chu(dtk)

    r = random.random()
    if r < NATURAL_IMPERFECTION["very_late_grade_entry_rate"]:
        days_after = random.randint(35, 56)
    elif r < NATURAL_IMPERFECTION["very_late_grade_entry_rate"] + NATURAL_IMPERFECTION["late_grade_entry_rate"]:
        days_after = random.randint(22, 34)
    else:
        days_after = random.randint(7, 21)

    return {
        "ma_dang_ky": ma_dk,
        "diem_chuyen_can": cc, "diem_bai_tap": bt,
        "diem_giua_ky": gk, "diem_cuoi_ky": ck,
        "diem_tong_ket": dtk, "diem_chu": chu,
        "diem_he_4": float(he4), "dat_mon": dtk >= 4.0,
        "hoc_lai": la_hoc_lai,
        "ngay_cham": datetime.combine(
            ngay_ket_thuc_hk + timedelta(days=days_after), datetime.min.time()
        ),
        "_grade_not_entered": False,
    }


# ★ v3.0 NEW — Profile Evolution
def _evolve_profile(current_profile: str, cum_gpa: float) -> str:
    """
    Cập nhật profile sau mỗi năm học (gọi sau HK2).
    
    Logic thực tế:
    - SV yếu/trung bình với GPA đang tăng → có thể cải thiện (8%)
    - SV giỏi/khá với GPA giảm → có thể sa sút (3%)
    - Biến động profile phản ánh nỗ lực, môi trường, tâm lý.
    
    Returns: profile mới (có thể giống profile cũ)
    """
    idx = PROFILE_ORDER.index(current_profile)

    # Khả năng cải thiện: cao hơn nếu GPA đang "gần" ngưỡng lên bậc
    if current_profile in ("yếu", "trung bình"):
        if random.random() < PROFILE_EVOLVE_UP_RATE:
            return PROFILE_ORDER[min(idx + 1, len(PROFILE_ORDER) - 1)]

    # Khả năng sa sút: SV giỏi/xuất sắc đôi khi mất động lực
    if current_profile in ("giỏi", "xuất sắc", "khá"):
        if random.random() < PROFILE_EVOLVE_DOWN_RATE:
            return PROFILE_ORDER[max(idx - 1, 0)]

    return current_profile


def _gen_sdt() -> str:
    p = random.choice(["032","033","034","035","036","038","086","096","097","098"])
    return p + str(random.randint(1_000_000, 9_999_999))


# ─────────────────────────────────────────────────────────
# HELPERS — Cross-Source Integrity (v2.2, giữ nguyên)
# ─────────────────────────────────────────────────────────

def _corrupt_encoding(name: str) -> str:
    if not name:
        return name
    corruption = random.choice(_ENCODING_CORRUPTIONS)
    old_char, new_char = corruption
    if old_char in name:
        return name.replace(old_char, new_char, 1)
    return name[:-1] + "?"


def _corrupt_date_format(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return random.choice([
            dt.strftime("%d/%m/%Y"),
            dt.strftime("%d-%m-%Y"),
            dt.strftime("%m/%d/%Y"),
            str(date_str).replace("-", "."),
        ])
    except (ValueError, TypeError):
        return date_str


def _make_trang_thai_variant(trang_thai: str) -> str:
    variants = TRANG_THAI_VARIANTS.get(trang_thai, [])
    if variants:
        return random.choice(variants)
    return trang_thai


def _build_ghost_csv_record(ma_hk: str, ghost_id: str) -> dict:
    drl = random.randint(40, 85)
    return {
        "ma_sinh_vien": ghost_id,
        "hoc_ky": ma_hk,
        "diem_ren_luyen": drl,
        "xep_loai_rl": "Khá" if drl >= 65 else "Trung bình",
        "loai_hoc_bong": "",
        "muc_tien_hb": 0,
        "hinh_thuc_ky_luat": "",
        "ly_do_ky_luat": "",
    }


def _build_ghost_json_record(ma_hk: str, ghost_id: str, ngay_bat_dau) -> dict:
    hp = random.randint(4, 8) * 3 * random.choice([440_000, 460_000, 480_000])
    da_dong = int(hp * random.uniform(0.5, 1.0))
    return {
        "ma_sinh_vien": ghost_id,
        "hoc_ky": ma_hk,
        "hoc_phi_phai_dong": hp,
        "da_dong": da_dong,
        "con_no": max(0, hp - da_dong),
        "duoc_mien_giam": False,
        "ly_do_mien_giam": "",
        "so_tien_mien_giam": 0,
        "ngay_dong_cuoi": str(ngay_bat_dau + timedelta(days=random.randint(10, 30))),
    }


def _build_mon_list_for_hk(hk_data: dict) -> list:
    result = list(hk_data["bat_buoc"])
    tc_list = hk_data.get("tu_chon", [])
    so_chon = hk_data.get("so_chon", 0)
    if tc_list and so_chon > 0:
        result.extend(tc_list[:so_chon])
    return result


def _tinh_tong_tc_den_hk(chuong_trinh: dict, hk_count: int) -> int:
    total = 0
    for hk_idx in range(1, hk_count + 1):
        mons = _build_mon_list_for_hk(chuong_trinh[hk_idx])
        total += sum(m[2] for m in mons)
    return total


# ═══════════════════════════════════════════════════════
# PHÂN CÔNG GIẢNG VIÊN
# ═══════════════════════════════════════════════════════
def _collect_all_courses():
    courses_by_khoa = {}
    dc_courses = []
    for _, cfg in NGANH_CONFIG.items():
        ma_khoa = cfg["ma_khoa"]
        ct = cfg["chuong_trinh"]
        for hk_idx in range(1, cfg["max_hk"] + 1):
            mons = _build_mon_list_for_hk(ct[hk_idx])
            for course in mons:
                ma_mon = course[0]
                if ma_mon.startswith("DC"):
                    if ma_mon not in dc_courses:
                        dc_courses.append(ma_mon)
                else:
                    courses_by_khoa.setdefault(ma_khoa, [])
                    if ma_mon not in [c[0] for c in courses_by_khoa[ma_khoa]]:
                        courses_by_khoa[ma_khoa].append(course)
    return courses_by_khoa, dc_courses


def _assign_instructors_to_courses(gv_ids_by_khoa, courses_by_khoa, dc_courses,
                                   dc_gv_per_course=4, major_gv_per_course=3):
    global_gv_per_mon = {}
    for ma_khoa, course_list in courses_by_khoa.items():
        gv_list = gv_ids_by_khoa.get(ma_khoa, [])
        if not gv_list:
            continue
        gv_load = {gv: 0 for gv in gv_list}
        for course in course_list:
            ma_mon = course[0]
            k = min(major_gv_per_course, len(gv_list))
            chosen = sorted(gv_list, key=lambda g: gv_load[g])[:k]
            global_gv_per_mon[ma_mon] = chosen
            for gv in chosen:
                gv_load[gv] += 1
    gv_kcb = gv_ids_by_khoa.get("KCB", [])
    gv_load_dc = {gv: 0 for gv in gv_kcb}
    for ma_mon in dc_courses:
        k = min(dc_gv_per_course, len(gv_kcb))
        chosen = sorted(gv_kcb, key=lambda g: gv_load_dc[g])[:k]
        global_gv_per_mon[ma_mon] = chosen
        for gv in chosen:
            gv_load_dc[gv] += 1
    return global_gv_per_mon


# ═══════════════════════════════════════════════════════
# HELPER TÀI CHÍNH
# ═══════════════════════════════════════════════════════

def _get_gia_tc(ma_hk: str) -> int:
    """
    ★ v3.0 — Lấy giá tín chỉ theo năm học từ HK master.
    Giá tăng ~5%/năm từ 2021. HK3 dùng cùng giá năm đó.
    """
    hk_info = HK_MASTER.get(ma_hk)
    if hk_info:
        year = hk_info[2].year   # ngay_bat_dau.year
        return GIA_TC_BY_YEAR.get(year, 460_000)
    return 460_000


def _gen_tai_chinh_record(ma_sv: str, ma_hk: str, hk_obj,
                           profile: str, mons_hk: list, hk_mod: float) -> dict:
    """Tài chính HK chính quy (có thể có miễn giảm, đóng 2 đợt)."""
    tc_hk  = sum(m[2] for m in mons_hk)
    gia_tc = _get_gia_tc(ma_hk)   # ★ v3.0: giá theo năm
    hoc_phi = tc_hk * gia_tc

    duoc_mg, ly_do_mg, so_tien_mg = False, "", 0
    if random.random() < 0.08:
        duoc_mg   = True
        ly_do_mg  = random.choice(["Chính sách","Hộ nghèo","Cận hộ nghèo","Dân tộc thiểu số","Mồ côi","Con thương binh"])
        so_tien_mg = int(hoc_phi * random.choice([0.3, 0.5, 0.7, 1.0]))

    hp_phai_dong = hoc_phi - so_tien_mg
    installment_roll = random.random()

    if profile in ("xuất sắc", "giỏi"):
        da_dong = hp_phai_dong if installment_roll < 0.95 else int(hp_phai_dong * random.uniform(0.6, 0.8))
    elif profile == "khá":
        da_dong = hp_phai_dong if installment_roll < 1 - NATURAL_IMPERFECTION["payment_installment_rate"] else int(hp_phai_dong * random.uniform(*NATURAL_IMPERFECTION["payment_installment_first"]))
    elif profile == "trung bình":
        da_dong = hp_phai_dong if installment_roll < 0.80 else int(hp_phai_dong * random.uniform(0.5, 0.85))
    else:  # yếu
        da_dong = hp_phai_dong if installment_roll < 0.50 else int(hp_phai_dong * random.uniform(0.1, 0.6))

    con_no_exact = hp_phai_dong - da_dong
    if random.random() < NATURAL_IMPERFECTION["financial_rounding_rate"]:
        lo, hi = NATURAL_IMPERFECTION["financial_rounding_range"]
        con_no = max(0, con_no_exact + random.randint(lo, hi))
    else:
        con_no = max(0, con_no_exact)

    days_offset = random.choices(
        [random.randint(5, 12), random.randint(13, 25), random.randint(26, 45)],
        weights=[50, 30, 20]
    )[0]
    ngay_dong = hk_obj.ngay_bat_dau + timedelta(days=days_offset)

    return {
        "ma_sinh_vien": ma_sv, "hoc_ky": ma_hk,
        "hoc_phi_phai_dong": hp_phai_dong,
        "da_dong": da_dong, "con_no": con_no,
        "duoc_mien_giam": duoc_mg, "ly_do_mien_giam": ly_do_mg,
        "so_tien_mien_giam": so_tien_mg,
        "ngay_dong_cuoi": str(ngay_dong),
    }


def _gen_tai_chinh_hk3_record(ma_sv: str, ma_hk: str, hk_obj,
                               courses_hk3: list) -> dict:
    """
    ★ v3.0 NEW — Tài chính HK3 (học lại hè).
    
    Đặc điểm tài chính HK3:
    - Không có miễn giảm học phí (trừ trường hợp đặc biệt: <2%)
    - Giá tín chỉ cao hơn HK chính quy ~10% (do tổ chức riêng)
    - 70% SV đóng đủ 1 lần (ít môn, ít tiền hơn)
    - 30% còn lại đóng 2 đợt (chờ đến lúc có tiền)
    """
    tc_hk3  = sum(tc for _, tc in courses_hk3)
    gia_tc  = int(_get_gia_tc(ma_hk) * 1.10)   # giá HK3 = +10%
    hoc_phi = tc_hk3 * gia_tc

    # HK3 rất hiếm có miễn giảm
    duoc_mg, ly_do_mg, so_tien_mg = False, "", 0
    if random.random() < 0.02:
        duoc_mg    = True
        ly_do_mg   = "Hoàn cảnh đặc biệt khó khăn"
        so_tien_mg = int(hoc_phi * 0.5)

    hp_phai_dong = hoc_phi - so_tien_mg

    # 70% đóng đủ, 30% đóng 2 đợt
    if random.random() < 0.70:
        da_dong = hp_phai_dong
    else:
        da_dong = int(hp_phai_dong * random.uniform(0.4, 0.7))

    con_no = max(0, hp_phai_dong - da_dong)

    # Đóng tiền sớm hơn HK chính (deadline HK3 ngắn hơn)
    days_offset = random.randint(3, 15)
    ngay_dong = hk_obj.ngay_bat_dau + timedelta(days=days_offset)

    return {
        "ma_sinh_vien": ma_sv, "hoc_ky": ma_hk,
        "hoc_phi_phai_dong": hp_phai_dong,
        "da_dong": da_dong, "con_no": con_no,
        "duoc_mien_giam": duoc_mg, "ly_do_mien_giam": ly_do_mg,
        "so_tien_mien_giam": so_tien_mg,
        "ngay_dong_cuoi": str(ngay_dong),
    }


# ═══════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════
def main():
    COHORTS = list(COHORT_CONFIG.keys())
    total_sv_est = sum(SV_PER_KHOA.values()) * len(NGANH_CONFIG)

    print("=" * 70)
    print(f" v3.0 — HK3 Summer Retake + Bug Fixes + Profile Evolution")
    print(f" 3 ngành: Kế toán, CNTT, Điện tử viễn thông")
    print(f" Tổng dự kiến: ~{total_sv_est} SV | 4 khóa B21-B24")
    print(f" HK3 hè: 4 năm học (2021-22 → 2024-25)")
    print("=" * 70)
    
    global_course_enrollment_count = {}
    tong_tc_chuong_trinh = {}
    for nk, cfg in NGANH_CONFIG.items():
        ct = cfg["chuong_trinh"]
        total_tc = _tinh_tong_tc_den_hk(ct, cfg["max_hk"])
        tong_tc_chuong_trinh[cfg["ma_nganh"]] = total_tc
        tc_per_hk = []
        for hk_idx in range(1, cfg["max_hk"] + 1):
            mons = _build_mon_list_for_hk(ct[hk_idx])
            tc_per_hk.append(f"HK{hk_idx}={sum(m[2] for m in mons)}")
        print(f" {cfg['ten_nganh']}: {total_tc} TC | {' | '.join(tc_per_hk)}")
    print()

    print("[0] Rebuild schema...")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("    OK")

    all_csv_records = []
    all_api_records = []
    global_hp_inserted = set()

    with Session(bind=engine) as s:
        # ── 1. Khoa ──
        khoas = [Khoa(ma_khoa=ma, ten_khoa=ten) for ma, ten in KHOA_DATA]
        s.add_all(khoas); s.flush()
        print(f"\n[1] Khoa        : {len(khoas)}")

        # ── 2. Ngành ──
        nganh_objs = [Nganh(ma_nganh=cfg["ma_nganh"], ten_nganh=cfg["ten_nganh"], ma_khoa=cfg["ma_khoa"]) for cfg in NGANH_CONFIG.values()]
        s.add_all(nganh_objs); s.flush()
        print(f"[2] Ngành       : {len(nganh_objs)}")

        # ── 3. Giảng viên ──
        gvs = []
        gv_ids_by_khoa = {}
        gv_counter = 1
        for ma_khoa_gv, names in GV_DATA.items():
            gv_ids_by_khoa[ma_khoa_gv] = []
            for ho, ten in names:
                ma_gv = f"GV{gv_counter:03d}"
                gvs.append(GiangVien(
                    ma_giang_vien=ma_gv, ho=ho, ten=ten,
                    email=f"gv{gv_counter:03d}@ptit.edu.vn",
                    so_dien_thoai=_gen_sdt(),
                    chuc_danh=random.choices(CHUC_DANH, weights=CHUC_DANH_W)[0],
                    trang_thai_cong_tac="Đang công tác", ma_khoa=ma_khoa_gv,
                ))
                gv_ids_by_khoa[ma_khoa_gv].append(ma_gv)
                gv_counter += 1
        s.add_all(gvs); s.flush()
        print(f"[3] Giảng viên  : {len(gvs)}")

        # ── 4. Học kỳ (bao gồm HK3) ──
        all_hk_keys = set()
        for seq in HK_SEQ_BY_COHORT.values():
            all_hk_keys.update(seq)
        hoc_kys = []
        for ma_hk in sorted(all_hk_keys):
            nh, hk, bd, kt = HK_MASTER[ma_hk]
            hoc_kys.append(HocKyNamHoc(ma_hoc_ky=ma_hk, nam_hoc=nh, hoc_ky=hk,
                                        ngay_bat_dau=bd, ngay_ket_thuc=kt))
        s.add_all(hoc_kys); s.flush()
        hk_by_key = {hk.ma_hoc_ky: hk for hk in hoc_kys}
        hk_ngay_bat_dau_map = {ma_hk: hk_obj.ngay_bat_dau for ma_hk, hk_obj in hk_by_key.items()}
        print(f"[4] Học kỳ      : {len(hoc_kys)} (gồm {sum(1 for k in all_hk_keys if 'HK3' in k)} HK3 hè)")

        # ── 5. Phân công GV ──
        print("\n[5] Đang tính toán phân công Giảng viên...")
        courses_by_khoa, dc_courses = _collect_all_courses()
        global_gv_per_mon = _assign_instructors_to_courses(
            gv_ids_by_khoa=gv_ids_by_khoa,
            courses_by_khoa=courses_by_khoa,
            dc_courses=dc_courses,
        )
        print(f"    -> Đã phân công cho {len(global_gv_per_mon)} môn học.")

        total_sv_created    = 0
        total_dk_created    = 0
        total_diem_created  = 0
        total_missing_grades = 0
        total_withdrawals   = 0
        total_hk3_enrollments = 0   # ★ v3.0
        temp_hk_data = {}

        for nganh_key, cfg in NGANH_CONFIG.items():
            print(f"\n{'─' * 50}")
            print(f" NGÀNH: {cfg['ten_nganh']} ({cfg['ma_nganh']})")
            print(f"{'─' * 50}")

            vt             = cfg["ma_viet_tat"]
            chuong_trinh   = cfg["chuong_trinh"]
            max_hk         = cfg["max_hk"]
            do_kho_nganh   = cfg.get("do_kho_nganh", 0.0)

            mon_by_hk = {}
            for hk_idx in range(1, max_hk + 1):
                mon_by_hk[hk_idx] = _build_mon_list_for_hk(chuong_trinh[hk_idx])

            # ── 6. Lớp hành chính ──
            lops = []
            lop_by_cohort = {}
            
            for cohort in COHORTS:
                yr = cohort[1:]
                lop_by_cohort[cohort] = []
                for stt in range(1, cfg["so_lop_per_khoa"] + 1):
                    ml = f"D{yr}CQ{vt}{stt:02d}-B"
                    lops.append(LopHanhChinh(
                        ma_lop=ml, ten_lop=ml, khoa_hoc=cohort,
                        ma_nganh=cfg["ma_nganh"],
                        ma_co_van=random.choice(gv_ids_by_khoa[cfg["ma_khoa"]]),
                    ))
                    lop_by_cohort[cohort].append(ml)
            s.add_all(lops); s.flush()
            print(f"  -> {len(lops)} Lớp hành chính")

            # ── 7. Học phần ──
            hoc_phans = []
            for hk_idx in range(1, max_hk + 1):
                for ma_mon, ten_mon, tc, lt, th, do_kho in mon_by_hk[hk_idx]:
                    if ma_mon in global_hp_inserted:
                        continue
                    global_hp_inserted.add(ma_mon)
                    hp_ma_khoa = "KCB" if ma_mon.startswith("DC") else cfg["ma_khoa"]
                    hoc_phans.append(HocPhan(
                        ma_hoc_phan=ma_mon, ma_mon=ma_mon, ten_mon=ten_mon,
                        so_tin_chi=tc, so_gio_ly_thuyet=lt, so_gio_thuc_hanh=th,
                        hoc_ky_de_xuat=hk_idx, bat_buoc=True, ma_khoa=hp_ma_khoa,
                    ))
            s.add_all(hoc_phans); s.flush()
            print(f"  -> {len(hoc_phans)} Học phần mới (tổng DB: {len(global_hp_inserted)})")

            # ── 8. Sinh viên ──
            svs = []
            hoc_luc_sv = {}
            for cohort in COHORTS:
                nam_nhap = COHORT_CONFIG[cohort][0]
                so_sv    = SV_PER_KHOA[cohort]
                for stt in range(1, so_sv + 1):
                    is_nam = random.random() < cfg["ty_le_nam"]
                    gioi   = "Nam" if is_nam else "Nữ"
                    ma_sv  = f"{cohort}DC{vt}{stt:03d}"
                    email  = f"{cohort.lower()}dc{vt.lower()}{stt:03d}@student.ptit.edu.vn"
                    nam_sinh = nam_nhap - random.randint(18, 21)
                    ho = random.choice(HO_LIST)
                    if gioi == "Nam":
                        ten_dem = random.choice(TEN_DEM_NAM)
                        ten     = random.choice(TEN_NAM)
                    else:
                        ten_dem = random.choice(TEN_DEM_NU)
                        ten     = random.choice(TEN_NU)
                    svs.append(SinhVien(
                        ma_sinh_vien=ma_sv, ho=f"{ho} {ten_dem}", ten=ten,
                        ngay_sinh=date(nam_sinh, random.randint(1, 12), random.randint(1, 28)),
                        gioi_tinh=gioi, email=email, ma_nganh=cfg["ma_nganh"],
                        ma_lop=random.choice(lop_by_cohort[cohort]),
                        khoa_hoc=cohort, trang_thai_hoc_tap="Đang học",
                    ))
                    hoc_luc_sv[ma_sv] = random.choices(
                        list(PROFILE_WEIGHTS.keys()),
                        weights=list(PROFILE_WEIGHTS.values()),
                    )[0]
            s.add_all(svs); s.flush()
            total_sv_created += len(svs)
            print(f"  -> {len(svs)} SV (tất cả 'Đang học')")

            # ── Tracking state ──
            sv_active      = {sv.ma_sinh_vien: True for sv in svs}
            sv_cum         = {sv.ma_sinh_vien: {"w_sum": 0.0, "total_tc": 0, "passed_tc": 0} for sv in svs}
            sv_final_status = {}
            dk_buf         = []
            dk_set         = set()
            sv_failed_courses = {}
            sv_hk_gpa     = {}
            # ★ v3.0: Theo dõi số HK2 đã hoàn thành (để trigger profile evolution)
            sv_hk2_count  = {sv.ma_sinh_vien: 0 for sv in svs}

            # ══════════════════════════════════════════════════════════
            # VÒNG LẶP CHÍNH: TỪNG KHÓA → TỪNG HK (bao gồm HK3)
            # ══════════════════════════════════════════════════════════
            for cohort in COHORTS:
                hk_seq     = HK_SEQ_BY_COHORT[cohort]
                cohort_svs = [sv for sv in svs if sv.khoa_hoc == cohort]

                # ★ v3.0 FIX BUG 5: Dùng curriculum_hk_counter riêng
                # HK3 KHÔNG tăng curriculum_hk_counter → tra cứu mon_by_hk đúng
                curriculum_hk_counter = 0

                for ma_hk in hk_seq:
                    # Xác định đây là HK3 (hè) hay HK chính quy
                    is_hk3 = "HK3" in ma_hk

                    if not is_hk3:
                        curriculum_hk_counter += 1
                    hk_idx = curriculum_hk_counter

                    # Dừng nếu đã vượt quá số HK của chương trình
                    if not is_hk3 and hk_idx > max_hk:
                        break

                    hk_obj = hk_by_key.get(ma_hk)
                    if hk_obj is None:
                        continue

                    is_graduation_hk = (not is_hk3) and (hk_idx == max_hk)
                    is_recent        = ma_hk in RECENT_HKS
                    hk_mod           = HK_MODIFIER.get(ma_hk, 0.0) + do_kho_nganh
                    active           = [sv for sv in cohort_svs if sv_active[sv.ma_sinh_vien]]

                    if not active:
                        break

                    # ══════════════════════════════════════════════
                    # NHÁNH HK3 — Học lại hè
                    # ══════════════════════════════════════════════
                    if is_hk3:
                        sv_with_failures = [
                            sv for sv in active
                            if sv_failed_courses.get(sv.ma_sinh_vien)
                        ]
                        # 55% SV có môn rớt chọn đăng ký HK3
                        hk3_participants = [
                            sv for sv in sv_with_failures
                            if random.random() < HK3_RETAKE_RATE
                        ]

                        hk3_count_this = 0
                        for sv in hk3_participants:
                            ma_sv   = sv.ma_sinh_vien
                            profile = hoc_luc_sv[ma_sv]
                            failed  = sv_failed_courses.get(ma_sv, [])

                            # Chọn tối đa HK3_MAX_COURSES môn rớt để học lại
                            # Ưu tiên môn rớt sớm nhất (đã nằm lâu trong danh sách)
                            courses_to_retake = failed[:HK3_MAX_COURSES]
                            if not courses_to_retake:
                                continue

                            sv_grades_hk3 = []
                            for (r_ma_mon, r_ten_mon, r_tc, r_lt, r_th, r_do_kho, orig_hk_idx) in courses_to_retake:
                                key = (ma_sv, r_ma_mon, ma_hk)
                                if key in dk_set:
                                    continue
                                dk_set.add(key)

                                gv_list = global_gv_per_mon.get(r_ma_mon)
                                if not gv_list:
                                    continue

                                # HK3 luôn là hoc_lai=True; modifier tích cực hơn
                                grade = _tao_diem_record(
                                    None, r_do_kho, profile, hk_idx, True,
                                    hk_obj.ngay_ket_thuc, hk_mod,
                                    is_recent_hk=is_recent
                                )
                                chosen_gv = random.choice(gv_list)

                                sv_grades_hk3.append({
                                    "ma_mon": r_ma_mon, "tc": r_tc, "ten_mon": r_ten_mon,
                                    "he4": grade["diem_he_4"] or 0.0,
                                    "dat": grade["dat_mon"],
                                    "grade_full": grade,
                                    "gv": chosen_gv, "do_kho": r_do_kho,
                                    "not_entered": grade.get("_grade_not_entered", False),
                                })

                                dk_buf.append({
                                    "ma_sinh_vien": ma_sv,
                                    "ma_hoc_phan": r_ma_mon,
                                    "ma_hoc_ky": ma_hk,
                                    "ma_giang_vien": chosen_gv,
                                    "ngay_dang_ky": hk_obj.ngay_bat_dau + timedelta(days=random.randint(1, 7)),
                                    "trang_thai": "Đã đăng ký",
                                    "_pre_grade": grade,
                                    "_withdrawn": False,
                                })

                                if grade.get("_grade_not_entered"):
                                    total_missing_grades += 1
                                hk3_count_this += 1

                            if not sv_grades_hk3:
                                continue

                            # Cập nhật danh sách môn rớt sau HK3
                            for g in sv_grades_hk3:
                                if g["dat"] and not g["not_entered"]:
                                    for ci in sv_failed_courses.get(ma_sv, [])[:]:
                                        if ci[0] == g["ma_mon"]:
                                            sv_failed_courses[ma_sv].remove(ci)
                                            break

                            # Điểm rèn luyện HK3 (thấp hơn HK chính, không có HB)
                            drl_hk3 = None
                            xl_hk3 = ""

                            temp_hk_data.setdefault(ma_hk, []).append({
                                "ma_sinh_vien": ma_sv, "hoc_ky": ma_hk, "khoa_hoc": cohort,
                                "drl": drl_hk3, "gpa_hk": 0.0, "xep_loai_rl": xl_hk3,
                                "hinh_thuc_kl": "", "ly_do_kl": "", "profile": profile,
                                "_is_hk3": True,    # flag: bỏ qua xét học bổng
                            })

                            # Tài chính HK3
                            courses_hk3_tc = [(g["ma_mon"], g["tc"]) for g in sv_grades_hk3]
                            api_rec_hk3 = _gen_tai_chinh_hk3_record(ma_sv, ma_hk, hk_obj, courses_hk3_tc)
                            if api_rec_hk3.get("ngay_dong_cuoi") and random.random() < NATURAL_IMPERFECTION["json_date_format_rate"]:
                                api_rec_hk3["ngay_dong_cuoi"] = _corrupt_date_format(api_rec_hk3["ngay_dong_cuoi"])
                            all_api_records.append(api_rec_hk3)

                        total_hk3_enrollments += hk3_count_this
                        print(f"    {cohort}/{ma_hk} [HK3-HÈ]: "
                              f"{len(hk3_participants)} SV học lại | "
                              f"{hk3_count_this} lượt đăng ký")
                        continue   # ← Quan trọng: bỏ qua phần HK chính phía dưới

                    # ══════════════════════════════════════════════
                    # NHÁNH HK CHÍNH QUY (HK1, HK2)
                    # ══════════════════════════════════════════════
                    mons       = mon_by_hk[hk_idx]
                    hk_eval_data = []

                    for sv in active:
                        ma_sv   = sv.ma_sinh_vien
                        profile = hoc_luc_sv[ma_sv]

                        if is_graduation_hk:
                            cum = sv_cum[ma_sv]
                            cum_gpa = (cum["w_sum"] / cum["total_tc"] if cum["total_tc"] > 0 else 0)
                            if cum_gpa < EVAL_CRITERIA["tot_nghiep_gpa_min"]:
                                continue

                        sv_grades_raw = []

                        for (ma_mon, ten_mon, tc, lt, th, do_kho) in mons:
                            key = (ma_sv, ma_mon, ma_hk)
                            if key in dk_set:
                                continue

                            # [COURSE_WITHDRAWAL] Rút môn giữa HK
                            if (not is_graduation_hk and do_kho > 1.0
                                    and random.random() < NATURAL_IMPERFECTION["course_withdrawal_rate"]):
                                dk_set.add(key)
                                dk_buf.append({
                                    "ma_sinh_vien": ma_sv, "ma_hoc_phan": ma_mon,
                                    "ma_hoc_ky": ma_hk, "ma_giang_vien": None,
                                    "ngay_dang_ky": hk_obj.ngay_bat_dau + timedelta(days=random.randint(1, 8)),
                                    "trang_thai": "Đã rút",
                                    "_pre_grade": None, "_withdrawn": True,
                                })
                                total_withdrawals += 1
                                continue

                            dk_set.add(key)
                            gv_list = global_gv_per_mon.get(ma_mon)
                            if not gv_list:
                                continue

                            grade = _tao_diem_record(
                                None, do_kho, profile, hk_idx, False,
                                hk_obj.ngay_ket_thuc, hk_mod, is_recent_hk=is_recent
                            )
                            current_count = global_course_enrollment_count.get(ma_mon, 0)
                            global_course_enrollment_count[ma_mon] = current_count + 1
                            chosen_gv = gv_list[current_count % len(gv_list)]

                            if random.random() < NATURAL_IMPERFECTION["late_registration_rate"]:
                                lo, hi = NATURAL_IMPERFECTION["late_registration_days"]
                                reg_days = random.randint(lo, hi)
                            else:
                                reg_days = random.randint(1, 10)

                            trang_thai_dk = "Đã đăng ký"
                            if random.random() < NATURAL_IMPERFECTION["trang_thai_variant_rate"]:
                                trang_thai_dk = _make_trang_thai_variant("Đã đăng ký")

                            sv_grades_raw.append({
                                "ma_mon": ma_mon, "tc": tc, "ten_mon": ten_mon,
                                "he4": grade["diem_he_4"] or 0.0,
                                "chu": grade["diem_chu"], "tong_ket": grade["diem_tong_ket"],
                                "dat": grade["dat_mon"], "grade_full": grade,
                                "gv": chosen_gv, "do_kho": do_kho, "lt": lt, "th": th,
                                "reg_days": reg_days, "trang_thai_dk": trang_thai_dk,
                                "not_entered": grade.get("_grade_not_entered", False),
                            })
                            if grade.get("_grade_not_entered"):
                                total_missing_grades += 1

                        # ── Học lại môn đã rớt (trong HK chính quy) ──
                        failed_list      = sv_failed_courses.get(ma_sv, [])
                        retake_this_hk   = []
                        retake_candidates = failed_list[:2]  # tối đa 2 môn/HK chính

                        for (r_ma_mon, r_ten_mon, r_tc, r_lt, r_th, r_do_kho, orig_hk_idx) in retake_candidates:
                            if (hk_idx % 2) != (orig_hk_idx % 2):
                                continue
                            key = (ma_sv, r_ma_mon, ma_hk)
                            if key in dk_set:
                                continue
                            dk_set.add(key)
                            gv_list_r = global_gv_per_mon.get(r_ma_mon)
                            if not gv_list_r:
                                continue
                            c_cnt = global_course_enrollment_count.get(r_ma_mon, 0)
                            global_course_enrollment_count[r_ma_mon] = c_cnt + 1
                            chosen_gv_r = gv_list_r[c_cnt % len(gv_list_r)]
                            grade = _tao_diem_record(
                                None, r_do_kho, profile, hk_idx, True,
                                hk_obj.ngay_ket_thuc, hk_mod, is_recent_hk=is_recent
                            )
                            sv_grades_raw.append({
                                "ma_mon": r_ma_mon, "tc": r_tc, "ten_mon": r_ten_mon,
                                "he4": grade["diem_he_4"] or 0.0,
                                "chu": grade["diem_chu"], "tong_ket": grade["diem_tong_ket"],
                                "dat": grade["dat_mon"], "grade_full": grade,
                                "gv": chosen_gv_r, "do_kho": r_do_kho, "lt": r_lt, "th": r_th,
                                "reg_days": random.randint(1, 10), "trang_thai_dk": "Đã đăng ký",
                                "not_entered": grade.get("_grade_not_entered", False),
                            })
                            retake_this_hk.append((r_ma_mon, r_ten_mon, r_tc, r_lt, r_th, r_do_kho, orig_hk_idx))
                            if grade.get("_grade_not_entered"):
                                total_missing_grades += 1

                        if not sv_grades_raw:
                            continue

                        # Thêm vào dk_buf
                        for g in sv_grades_raw:
                            dk_buf.append({
                                "ma_sinh_vien": ma_sv, "ma_hoc_phan": g["ma_mon"],
                                "ma_hoc_ky": ma_hk, "ma_giang_vien": g["gv"],
                                "ngay_dang_ky": hk_obj.ngay_bat_dau + timedelta(days=g.get("reg_days", 1)),
                                "trang_thai": g.get("trang_thai_dk", "Đã đăng ký"),
                                "_pre_grade": g["grade_full"], "_withdrawn": False,
                            })

                        # Cập nhật GPA tích lũy
                        for g in sv_grades_raw:
                            if g["not_entered"] or g["he4"] is None:
                                continue
                            sv_cum[ma_sv]["w_sum"]    += g["he4"] * g["tc"]
                            sv_cum[ma_sv]["total_tc"] += g["tc"]
                            if g["dat"]:
                                sv_cum[ma_sv]["passed_tc"] += g["tc"]

                        c = sv_cum[ma_sv]
                        cum_gpa = round(max(c["w_sum"] / c["total_tc"], 0.10), 4) if c["total_tc"] > 0 else 0.10

                        graded = [g for g in sv_grades_raw if not g["not_entered"] and g["he4"] is not None]
                        if graded:
                            total_w  = sum(g["he4"] * g["tc"] for g in graded)
                            total_tc = sum(g["tc"] for g in graded)
                            gpa_hk   = round(total_w / total_tc, 4) if total_tc > 0 else 0.10
                        else:
                            gpa_hk = 0.10
                        gpa_hk = max(gpa_hk, 0.10)
                        sv_hk_gpa[(ma_sv, ma_hk)] = gpa_hk

                        hk_eval_data.append({
                            "ma_sv": ma_sv, "gpa_hk": gpa_hk, "cum_gpa": cum_gpa,
                            "profile": profile, "hk_idx": hk_idx,
                        })

                        # Cập nhật danh sách môn rớt
                        for course_info in retake_this_hk:
                            retake_grade = next(
                                (g for g in sv_grades_raw if g["ma_mon"] == course_info[0]
                                 and g["grade_full"].get("hoc_lai")), None
                            )
                            if retake_grade and retake_grade.get("dat") and not retake_grade.get("not_entered"):
                                if course_info in sv_failed_courses.get(ma_sv, []):
                                    sv_failed_courses[ma_sv].remove(course_info)

                        for g in sv_grades_raw:
                            if not g.get("dat") and not g["grade_full"].get("hoc_lai") and not g["not_entered"]:
                                course_info = (g["ma_mon"], g["ten_mon"], g["tc"], g["lt"], g["th"], g["do_kho"], hk_idx)
                                sv_failed_courses.setdefault(ma_sv, [])
                                if course_info not in sv_failed_courses[ma_sv]:
                                    sv_failed_courses[ma_sv].append(course_info)

                    # ── Đánh giá sau mỗi HK chính ──
                    if is_graduation_hk:
                        for ev in hk_eval_data:
                            sv_final_status[ev["ma_sv"]] = "Tốt nghiệp"
                            sv_active[ev["ma_sv"]]       = False
                    else:
                        for ev in hk_eval_data:
                            ma_sv    = ev["ma_sv"]
                            cum_gpa  = ev["cum_gpa"]
                            profile  = ev["profile"]
                            hi       = ev["hk_idx"]

                            # DRL + Học bổng + Kỷ luật
                            drl_shift = int(hk_mod * 4)
                            drl_means = {"xuất sắc": 88,"giỏi": 82,"khá": 73,"yếu": 48,"trung bình": 62}
                            drl_stds  = {"xuất sắc": 4, "giỏi": 5, "khá": 6, "yếu": 8, "trung bình": 7}
                            drl = int(max(0, min(100, random.gauss(
                                drl_means.get(profile, 62) + drl_shift,
                                drl_stds.get(profile, 7)
                            ))))
                            xl_rl = "Kém"
                            for lo, hi_rl, label in [(90,"","Xuất sắc"),(80,"","Tốt"),(65,"","Khá"),(50,"","Trung bình"),(35,"","Yếu")]:
                                if drl >= lo:
                                    xl_rl = label
                                    break

                            kl_ht, kl_ld = "", ""
                            if profile == "yếu" and random.random() < 0.15:
                                kl_ht = random.choice(["Cảnh cáo lần 1","Cảnh cáo lần 2","Khiển trách"])
                                kl_ld = random.choice(["Thi hộ","Vi phạm quy chế thi","Gian lận bài tập","Nghỉ học quá nhiều","Vi phạm nội quy KTX"])

                            temp_hk_data.setdefault(ma_hk, []).append({
                                "ma_sinh_vien": ma_sv, "hoc_ky": ma_hk, "khoa_hoc": cohort,
                                "drl": drl, "gpa_hk": ev["gpa_hk"], "xep_loai_rl": xl_rl,
                                "hinh_thuc_kl": kl_ht, "ly_do_kl": kl_ld, "profile": profile,
                                "_is_hk3": False,
                            })

                            # Tài chính
                            api_record = _gen_tai_chinh_record(ma_sv, ma_hk, hk_obj, profile, mon_by_hk[hk_idx], hk_mod)
                            if api_record.get("ngay_dong_cuoi") and random.random() < NATURAL_IMPERFECTION["json_date_format_rate"]:
                                api_record["ngay_dong_cuoi"] = _corrupt_date_format(api_record["ngay_dong_cuoi"])
                            all_api_records.append(api_record)
                            if random.random() < NATURAL_IMPERFECTION["json_duplicate_rate"]:
                                all_api_records.append(api_record.copy())

                            # Đánh giá thôi học / bảo lưu
                            if hi >= EVAL_CRITERIA["ap_dung_tu_hk"]:
                                if cum_gpa < EVAL_CRITERIA["buoc_thoi_hoc_gpa_hard"]:
                                    sv_active[ma_sv]        = False
                                    sv_final_status[ma_sv]  = "Thôi học"
                                elif sv_active.get(ma_sv, True) is not False:
                                    prob = 0.0
                                    if profile == "yếu"        and cum_gpa < EVAL_CRITERIA["bao_luu_gpa_threshold"]:
                                        prob = EVAL_CRITERIA["bao_luu_prob_yeu"]
                                    elif profile == "trung bình" and cum_gpa < EVAL_CRITERIA["bao_luu_gpa_threshold"]:
                                        prob = EVAL_CRITERIA["bao_luu_prob_tb"]
                                    if prob > 0 and random.random() < prob:
                                        sv_active[ma_sv]       = False
                                        sv_final_status[ma_sv] = "Bảo lưu"

                            # ★ v3.0 FIX BUG 3: Profile evolution sau HK2 mỗi năm
                            if "HK2" in ma_hk and sv_active.get(ma_sv, False):
                                sv_hk2_count[ma_sv] = sv_hk2_count.get(ma_sv, 0) + 1
                                # Evolve sau mỗi năm học (HK2 kết thúc năm học)
                                new_profile = _evolve_profile(profile, cum_gpa)
                                if new_profile != profile:
                                    hoc_luc_sv[ma_sv] = new_profile

                    print(f"    {cohort}/{ma_hk} (HK{hk_idx}): "
                          f"{len(hk_eval_data)} SV có điểm | "
                          f"{'[RECENT]' if is_recent else ''} "
                          f"Còn active: {sum(1 for sv in cohort_svs if sv_active[sv.ma_sinh_vien])}")

            # ── PASS 2: Học bổng (bỏ qua HK3) ──
            total_hb_nganh = {"Xuất sắc": 0, "Giỏi": 0, "Khá": 0}
            for ma_hk, records in sorted(temp_hk_data.items()):
                by_cohort = {}
                for rec in records:
                    by_cohort.setdefault(rec["khoa_hoc"], []).append(rec)
                for cohort, cohort_records in sorted(by_cohort.items()):
                    cohort_records.sort(key=lambda x: (x["gpa_hk"], x["drl"]), reverse=True)
                    quota   = max(1, int(len(cohort_records) * HB_QUOTA_RATE))
                    granted = 0
                    for rec in cohort_records:
                        loai_hb, muc_tien = "", 0
                        # ★ v3.0: Bỏ qua học bổng cho HK3
                        is_hk3_rec = rec.get("_is_hk3", False)
                        if not is_hk3_rec and granted < quota and not rec["hinh_thuc_kl"]:
                            for tier in HB_TIERS:
                                if rec["drl"] >= tier["min_drl"] and rec["gpa_hk"] >= tier["min_gpa"]:
                                    loai_hb   = tier["loai"]
                                    muc_tien  = tier["muc_tien"]
                                    granted  += 1
                                    break

                        drl_val       = rec["drl"]
                        xep_loai_val  = rec["xep_loai_rl"]
                        ma_sv_csv     = rec["ma_sinh_vien"]

                        if random.random() < NATURAL_IMPERFECTION["csv_missing_rl_rate"]:
                            drl_val, xep_loai_val = None, None

                        if random.random() < NATURAL_IMPERFECTION["csv_trailing_space_rate"]:
                            ma_sv_csv = rec["ma_sinh_vien"] + " "

                        if (xep_loai_val is not None
                                and random.random() < NATURAL_IMPERFECTION["csv_encoding_error_rate"]):
                            xep_loai_val = _corrupt_encoding(xep_loai_val)

                        # HK3: không có học bổng; note trong CSV
                        csv_rec = {
                            "ma_sinh_vien": ma_sv_csv,
                            "hoc_ky": rec["hoc_ky"],
                            "diem_ren_luyen": drl_val,
                            "xep_loai_rl": xep_loai_val,
                            "loai_hoc_bong": loai_hb,      # rỗng nếu HK3
                            "muc_tien_hb": muc_tien,
                            "hinh_thuc_ky_luat": rec["hinh_thuc_kl"],
                            "ly_do_ky_luat": rec["ly_do_kl"],
                        }
                        all_csv_records.append(csv_rec)

                        if random.random() < NATURAL_IMPERFECTION["csv_duplicate_rate"]:
                            all_csv_records.append(csv_rec.copy())

                        if loai_hb:
                            if "Xuất sắc" in loai_hb: total_hb_nganh["Xuất sắc"] += 1
                            elif "Giỏi"    in loai_hb: total_hb_nganh["Giỏi"]    += 1
                            else:                       total_hb_nganh["Khá"]     += 1

            print(f"  -> Học bổng: XS={total_hb_nganh['Xuất sắc']} | "
                  f"Giỏi={total_hb_nganh['Giỏi']} | Khá={total_hb_nganh['Khá']}")

            # ── Cập nhật trạng thái SV ──
            status_counts = {}
            for sv in svs:
                if sv.ma_sinh_vien in sv_final_status:
                    sv.trang_thai_hoc_tap = sv_final_status[sv.ma_sinh_vien]
                status_counts[sv.trang_thai_hoc_tap] = status_counts.get(sv.trang_thai_hoc_tap, 0) + 1
            s.flush()
            print(f"\n  -> Trạng thái: {' | '.join(f'{k}:{v}' for k, v in sorted(status_counts.items()))}")

            # ── Insert Đăng ký ──
            dk_clean = [{k: v for k, v in d.items() if not k.startswith("_")} for d in dk_buf]
            inserted_dk = 0
            for i in range(0, len(dk_clean), 500):
                batch = dk_clean[i:i + 500]
                res = s.execute(text("""
                    INSERT INTO dang_ky_hoc_phan
                    (ma_sinh_vien, ma_hoc_phan, ma_hoc_ky, ma_giang_vien, ngay_dang_ky, trang_thai)
                    VALUES (:ma_sinh_vien, :ma_hoc_phan, :ma_hoc_ky, :ma_giang_vien, :ngay_dang_ky, :trang_thai)
                    ON CONFLICT (ma_sinh_vien, ma_hoc_phan, ma_hoc_ky) DO NOTHING
                """), batch)
                inserted_dk += res.rowcount
            s.flush()
            total_dk_created += inserted_dk
            print(f"  -> {inserted_dk:,} Đăng ký HP "
                  f"(rút: {total_withdrawals} | HK3: {total_hk3_enrollments})")

            # ── Load IDs ──
            like_pats = " OR ".join([f"ma_sinh_vien LIKE '{c}DC{vt}%'" for c in COHORTS])
            all_dk = s.execute(text(
                f"SELECT ma_dang_ky, ma_sinh_vien, ma_hoc_phan, ma_hoc_ky "
                f"FROM dang_ky_hoc_phan WHERE {like_pats}"
            )).fetchall()
            dk_to_id = {(r.ma_sinh_vien, r.ma_hoc_phan, r.ma_hoc_ky): r.ma_dang_ky for r in all_dk}

            # ── Insert Điểm ──
            diem_buf = []
            for d in dk_buf:
                if d.get("_withdrawn") or d.get("_pre_grade") is None:
                    continue
                key   = (d["ma_sinh_vien"], d["ma_hoc_phan"], d["ma_hoc_ky"])
                ma_dk = dk_to_id.get(key)
                if ma_dk is None:
                    continue
                pg = d["_pre_grade"]
                diem_buf.append({
                    "ma_dang_ky": ma_dk,
                    "diem_chuyen_can": pg["diem_chuyen_can"],
                    "diem_bai_tap":    pg["diem_bai_tap"],
                    "diem_giua_ky":    pg["diem_giua_ky"],
                    "diem_cuoi_ky":    pg["diem_cuoi_ky"],
                    "diem_tong_ket":   pg["diem_tong_ket"],
                    "diem_chu":        pg["diem_chu"],
                    "diem_he_4":       pg["diem_he_4"],
                    "dat_mon":         pg["dat_mon"],
                    "hoc_lai":         pg["hoc_lai"],
                    "ngay_cham":       pg["ngay_cham"],
                })

            inserted_diem = 0
            for i in range(0, len(diem_buf), 500):
                batch = diem_buf[i:i + 500]
                res = s.execute(text("""
                    INSERT INTO diem_hoc_phan
                    (ma_dang_ky, diem_chuyen_can, diem_bai_tap, diem_giua_ky,
                     diem_cuoi_ky, diem_tong_ket, diem_chu, diem_he_4,
                     dat_mon, hoc_lai, ngay_cham)
                    VALUES
                    (:ma_dang_ky, :diem_chuyen_can, :diem_bai_tap, :diem_giua_ky,
                     :diem_cuoi_ky, :diem_tong_ket, :diem_chu, :diem_he_4,
                     :dat_mon, :hoc_lai, :ngay_cham)
                    ON CONFLICT (ma_dang_ky) DO NOTHING
                """), batch)
                inserted_diem += res.rowcount
            s.flush()
            total_diem_created += inserted_diem
            print(f"  -> {inserted_diem:,} Điểm HP ({total_missing_grades} chưa nhập)")

        # ── 11. GPA Tổng hợp (SQL dùng MAX → tự xử lý retake đúng) ──
        print(f"\n[11] Đang tính GPA tổng hợp (MAX per môn)...")
        rows_gpa = s.execute(text("""
            WITH best_diem AS (
                SELECT dk.ma_sinh_vien, dk.ma_hoc_phan, hp.so_tin_chi,
                       MAX(d.diem_he_4)    AS best_he4,
                       BOOL_OR(d.dat_mon)  AS dat_mon
                FROM diem_hoc_phan d
                JOIN dang_ky_hoc_phan dk ON d.ma_dang_ky = dk.ma_dang_ky
                JOIN hoc_phan hp         ON dk.ma_hoc_phan = hp.ma_hoc_phan
                WHERE d.diem_he_4 IS NOT NULL
                GROUP BY dk.ma_sinh_vien, dk.ma_hoc_phan, hp.so_tin_chi
            )
            SELECT ma_sinh_vien,
                   SUM(best_he4 * so_tin_chi) AS tong_cl,
                   SUM(so_tin_chi)             AS tc_da_hoc,
                   SUM(CASE WHEN dat_mon THEN so_tin_chi ELSE 0 END) AS tc_dat
            FROM best_diem GROUP BY ma_sinh_vien
        """)).fetchall()

        sv_nganh_map = {
            r.ma_sinh_vien: r.ma_nganh
            for r in s.execute(text("SELECT ma_sinh_vien, ma_nganh FROM sinh_vien")).fetchall()
        }
        th_buf = []
        for r in rows_gpa:
            if not r.tc_da_hoc:
                continue
            gpa4 = round(float(r.tong_cl) / float(r.tc_da_hoc), 2)
            ma_nganh = sv_nganh_map.get(r.ma_sinh_vien, "CNTT")
            tong_tc_ct = tong_tc_chuong_trinh.get(ma_nganh, 151)
            if gpa4 <= 0.0:
                gpa4 = 0.10
            th_buf.append({
                "ma_sinh_vien":     r.ma_sinh_vien,
                "tong_tin_chi":     int(tong_tc_ct),
                "tin_chi_tich_luy": int(r.tc_dat),
                "gpa_he_10":        round(min(gpa4 * 2.5, 10.0), 2),
                "gpa_he_4":         gpa4,
                "canh_bao_hoc_vu":  gpa4 < 2.0,
            })
        s.bulk_insert_mappings(TongHopKetQua, th_buf)
        s.commit()
        print(f"  -> Đã lưu GPA cho {len(th_buf)} SV")

    # ── 12. Xuất File ──
    print(f"\n[12] Đang xuất files CSV & JSON...")

    # [ORPHAN_RECORDS] — CSV
    all_hk_in_data = list({rec["hoc_ky"] for rec in all_csv_records})
    hks_with_orphan_csv = random.sample(all_hk_in_data, k=max(1, int(len(all_hk_in_data) * 0.30)))
    total_orphan_csv = 0
    for ma_hk in hks_with_orphan_csv:
        ghost_pool = random.sample(_GHOST_IDS_CSV, min(random.randint(1, 2), len(_GHOST_IDS_CSV)))
        for ghost_id in ghost_pool:
            all_csv_records.append(_build_ghost_csv_record(ma_hk, ghost_id))
            total_orphan_csv += 1

    # Xuất CSV theo HK
    hk_groups_csv = {}
    for rec in all_csv_records:
        hk_groups_csv.setdefault(rec["hoc_ky"], []).append(rec)

    csv_fields = ["ma_sinh_vien","hoc_ky","diem_ren_luyen","xep_loai_rl",
                  "loai_hoc_bong","muc_tien_hb","hinh_thuc_ky_luat","ly_do_ky_luat"]
    csv_file_count = 0
    for hk, records in sorted(hk_groups_csv.items()):
        with open(f"{OUTPUT_DIR}/csv/ctsv_{hk.replace('-','_')}.csv", "w",
                  newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=csv_fields)
            w.writeheader(); w.writerows(records)
        csv_file_count += 1
    with open(f"{OUTPUT_DIR}/csv/ctsv_all.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=csv_fields)
        w.writeheader(); w.writerows(all_csv_records)

    # [ORPHAN_RECORDS] — JSON
    all_hk_in_json = list({rec["hoc_ky"] for rec in all_api_records})
    hks_with_orphan_json = random.sample(all_hk_in_json, k=max(1, int(len(all_hk_in_json) * 0.25)))
    total_orphan_json = 0
    for ma_hk in hks_with_orphan_json:
        ngay_bd = hk_ngay_bat_dau_map.get(ma_hk)
        if ngay_bd is None:
            continue
        ghost_pool = random.sample(_GHOST_IDS_JSON, min(random.randint(1, 2), len(_GHOST_IDS_JSON)))
        for ghost_id in ghost_pool:
            ghost_rec = _build_ghost_json_record(ma_hk, ghost_id, ngay_bd)
            if random.random() < 0.3:
                ghost_rec["ngay_dong_cuoi"] = _corrupt_date_format(ghost_rec["ngay_dong_cuoi"])
            all_api_records.append(ghost_rec)
            total_orphan_json += 1

    # [JSON_METADATA] — Wrap và xuất JSON
    _GEN_TIMESTAMP = datetime.now().isoformat(timespec="seconds")

    def _wrap_with_metadata(records: list, hoc_ky: str = "all") -> dict:
        return {
            "metadata": {
                "generated_at":  _GEN_TIMESTAMP,
                "schema_version": "3.0",
                "source_system":  "PTIT_PortalTaiChinh",
                "hoc_ky":         hoc_ky,
                "record_count":   len(records),
                "api_endpoint":   f"/api/tai-chinh/sinh-vien?hoc_ky={hoc_ky}",
                # ★ v3.0: thêm flag để ETL biết có HK3 trong dataset
                "includes_hk3":   any("HK3" in r.get("hoc_ky","") for r in records),
            },
            "data": records,
        }

    hk_groups_api = {}
    for rec in all_api_records:
        hk_groups_api.setdefault(rec["hoc_ky"], []).append(rec)

    json_file_count = 0
    for hk, records in sorted(hk_groups_api.items()):
        payload = _wrap_with_metadata(records, hoc_ky=hk)
        with open(f"{OUTPUT_DIR}/api_json/taichinh_{hk.replace('-','_')}.json", "w",
                  encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        json_file_count += 1

    all_payload = _wrap_with_metadata(all_api_records, hoc_ky="all")
    with open(f"{OUTPUT_DIR}/api_json/taichinh_all.json", "w", encoding="utf-8") as f:
        json.dump(all_payload, f, ensure_ascii=False, indent=2)

    # ── Thống kê cuối ──
    n_csv   = len(all_csv_records)
    n_json  = len(all_api_records)
    hk3_hk_list = [k for k in hk_groups_csv if "HK3" in k]
    hk3_csv_recs = sum(len(hk_groups_csv[k]) for k in hk3_hk_list)
    hk3_json_recs = sum(len(hk_groups_api.get(k,[])) for k in hk3_hk_list)

    print()
    print("=" * 70)
    print(" HOÀN THÀNH — v3.0 | HK3 Summer Retake + Bug Fixes")
    print("=" * 70)
    print(f" [PostgreSQL]")
    print(f"   Sinh viên                : {total_sv_created:,}")
    print(f"   Đăng ký HP               : {total_dk_created:,}")
    print(f"     └─ Môn đã rút          : {total_withdrawals:,}   ← [COURSE_WITHDRAWAL]")
    print(f"     └─ HK3 (học lại hè)    : {total_hk3_enrollments:,}  ★ [HK3_SUMMER]")
    print(f"   Điểm HP                  : {total_diem_created:,}")
    print(f"     └─ Chưa nhập (NULL)    : {total_missing_grades:,}  ← [NATURAL_MISSING]")
    print(f"   GPA Tổng hợp             : {len(th_buf):,}")
    print()
    print(f" [CSV — {csv_file_count} files]")
    print(f"   Tổng records             : {n_csv:,}")
    print(f"     └─ HK3 hè              : ~{hk3_csv_recs:,}  ★ [HK3_SUMMER]")
    print(f"     └─ Thiếu RL            : ~{int(n_csv*NATURAL_IMPERFECTION['csv_missing_rl_rate']):,}  ← [CSV_NATURAL_NOISE]")
    print(f"     └─ Orphan SV           : {total_orphan_csv:,}  ← [ORPHAN_RECORDS]")
    print()
    print(f" [JSON — {json_file_count} files]")
    print(f"   Tổng records             : {n_json:,}")
    print(f"     └─ HK3 hè              : ~{hk3_json_recs:,}  ★ [HK3_SUMMER]")
    print(f"     └─ Ngày sai format     : ~{int(n_json*NATURAL_IMPERFECTION['json_date_format_rate']):,}  ← [DATE_FORMAT_MISMATCH]")
    print(f"     └─ Orphan SV           : {total_orphan_json:,}  ← [ORPHAN_RECORDS]")
    print(f"   Metadata wrapper v3.0    : ✅  includes_hk3 flag")
    print()
    print(f" [BUG FIXES v3.0]")
    print(f"   ✅ buoc_thoi_hoc_gpa_hard: 0.5 → 0.80 (thực tế hơn)")
    print(f"   ✅ HK_MODIFIER effect     : đã cap ±0.5 per component")
    print(f"   ✅ curriculum_hk_counter  : tách riêng khỏi HK3 index")
    print(f"   ✅ Profile evolution      : ~{PROFILE_EVOLVE_UP_RATE*100:.0f}% cải thiện, ~{PROFILE_EVOLVE_DOWN_RATE*100:.0f}% sa sút/năm")
    print(f"   ✅ Học phí theo năm       : {GIA_TC_BY_YEAR}")
    print("=" * 70)


if __name__ == "__main__":
    main()