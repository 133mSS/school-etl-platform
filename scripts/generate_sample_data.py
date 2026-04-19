"""
generate_sample_data.py — PTIT School ETL Platform
===================================================
Version: 2.2 — "Realistic Imperfections + Cross-Source Integrity"

TRIẾT LÝ CẢI TIẾN (so với v2.0):
─────────────────────────────────
Thay vì sinh dữ liệu 100% sạch rồi dùng inject_errors.py để "bẩn hóa",
phiên bản này BAKE các imperfection tự nhiên NGAY TRONG quá trình sinh,
phản ánh đúng môi trường vận hành thực tế của hệ thống PTIT.

Nguyên tắc: "Dữ liệu bẩn không phải do ai cố tình tạo ra,
             mà là hệ quả tự nhiên của quy trình vận hành."

THAY ĐỔI TỪ v2.1 → v2.2:
──────────────────────────
★ [DUPLICATE_RECORDS]     ~1-2% records bị duplicate trong CSV/JSON (export 2 lần)
★ [TRANG_THAI_VARIANTS]   Trường trang_thai có biến thể format (uppercase, không dấu)
★ [DATE_FORMAT_MISMATCH]  ~2.5% JSON dùng format ngày dd/mm/yyyy (vendor API khác chuẩn)
★ [ORPHAN_RECORDS]        ~0.5% CSV/JSON có ma_sinh_vien không tồn tại trong PostgreSQL
★ [ENCODING_ISSUES]       ~0.5% tên SV trong CSV bị lỗi encoding (Windows-1252 artifact)
★ [DYNAMIC_RECENT_HKS]    RECENT_HKS tính động từ HK_MASTER, không hardcode
★ [JSON_METADATA]         File JSON có wrapper metadata (generated_at, version, source)

THAY ĐỔI TỪ v2.0 → v2.1 (giữ nguyên):
────────────────────────────────────────
★ [NATURAL_MISSING]   3-8% điểm của 2 HK gần nhất chưa được GV nhập
★ [COURSE_WITHDRAWAL] 1.5% SV rút môn giữa HK → dang_ky tồn tại, không có diem
★ [PAYMENT_INSTALLMENT] 20-30% SV đóng học phí thành 2 đợt
★ [FINANCIAL_ROUNDING]  ±200-800 VND rounding tự nhiên trong con_no
★ [LATE_REGISTRATION]   3% đăng ký muộn (add/drop period, 10-20 ngày sau HK start)
★ [CSV_NATURAL_NOISE]   2% trailing space trong ma_sinh_vien, 4% thiếu diem_ren_luyen
★ [GRADE_BOUNDARY]      Thêm nhiều điểm ngưỡng ranh giới (3.9x, 4.9x, 5.4x)
★ [REMOVE_CAP_GPA_HK]   Bỏ hàm _cap_gpa_hk nhân tạo — SV xuất sắc xứng đáng được 4.0
★ [FIX_GRADUATION_BUG]  Đảm bảo SV không đủ GPA tốt nghiệp được đánh dấu "Đang học"
★ [LATE_GRADE_ENTRY]    Một số GV nhập điểm rất sát deadline (3-6 tuần sau kỳ thi)
★ [SKEWED_PAYMENT_DATE] Ngày đóng tiền tập trung vào đầu tháng (thực tế hơn)

inject_errors.py vẫn có thể dùng để TĂNG CƯỜNG thêm lỗi nếu cần demo ETL error handling,
nhưng không còn là bắt buộc để có dữ liệu "thực tế".
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
# ★ CẤU HÌNH IMPERFECTION TỰ NHIÊN
# (điều chỉnh các con số này để thay đổi mức độ "bẩn")
# ═══════════════════════════════════════════════════════
NATURAL_IMPERFECTION = {
    # Tỷ lệ điểm chưa nhập (GV nhập muộn) — chỉ áp dụng cho 2 HK cuối
    "grade_not_entered_recent_hk": 0.045,   # 4.5% điểm chưa nhập
    "grade_not_entered_older_hk":  0.008,   # 0.8% điểm cũ vẫn chưa nhập

    # Tỷ lệ SV rút môn giữa HK (có dang_ky nhưng không có diem)
    "course_withdrawal_rate":       0.015,  # 1.5%

    # Thanh toán học phí 2 đợt
    "payment_installment_rate":     0.25,   # 25% SV "khá" đóng 2 đợt
    "payment_installment_first":    (0.55, 0.75),  # đợt 1: 55-75% học phí

    # Sai số làm tròn tự nhiên trong con_no (VND)
    "financial_rounding_rate":      0.06,   # 6% records có sai số
    "financial_rounding_range":     (-600, 800),    # sai số tối đa

    # Đăng ký muộn (add/drop period)
    "late_registration_rate":       0.03,   # 3% đăng ký muộn
    "late_registration_days":       (11, 22),       # muộn 11-22 ngày

    # CSV rèn luyện tự nhiên
    "csv_missing_rl_rate":          0.04,   # 4% thiếu điểm RL (SV chưa nộp)
    "csv_trailing_space_rate":      0.02,   # 2% mã SV có trailing space
    "csv_late_submission_hk":       1,      # bỏ dữ liệu HK gần nhất của X% SV

    # Điểm ngưỡng ranh giới (tạo nhiều điểm gần ngưỡng đổi bậc)
    "boundary_score_boost":         0.08,   # 8% điểm được "kéo" về gần ngưỡng

    # GV nhập điểm rất muộn
    "very_late_grade_entry_rate":   0.03,   # 3% nhập điểm sau 5-8 tuần
    "late_grade_entry_rate":        0.12,   # 12% nhập điểm sau 3-5 tuần

    # ── v2.2: Cross-Source Integrity Issues ──────────────────────────────
    # Duplicate records trong CSV/JSON (export 2 lần hoặc merge file thủ công)
    "csv_duplicate_rate":           0.012,  # 1.2% dòng CSV bị nhân đôi
    "json_duplicate_rate":          0.008,  # 0.8% record JSON bị nhân đôi

    # Trường trang_thai/trạng thái có biến thể format không nhất quán
    # (hệ thống cũ export uppercase, hệ thống mới có dấu tiếng Việt)
    "trang_thai_variant_rate":      0.015,  # 1.5% trang_thai dùng format cũ

    # Ngày tháng trong JSON không nhất quán với chuẩn ISO (vendor API lỗi)
    "json_date_format_rate":        0.025,  # 2.5% ngay_dong_cuoi dùng dd/mm/yyyy

    # Orphan records: ma_sinh_vien tồn tại trong CSV/JSON nhưng không có trong PG
    # (SV đã được xóa, hoặc dữ liệu từ năm trước khi migrate)
    "csv_orphan_rate":              0.004,  # 0.4% orphan SV trong CSV
    "json_orphan_rate":             0.003,  # 0.3% orphan SV trong JSON

    # Lỗi encoding tên tiếng Việt trong CSV (Windows-1252 → UTF-8 không khớp)
    # Xảy ra khi nhân viên mở/lưu file CSV bằng Excel trên máy Windows cũ
    "csv_encoding_error_rate":      0.005,  # 0.5% tên SV bị lỗi encoding
}

# Các ngưỡng điểm ranh giới (gần đây sinh viên hay bị "chênh" ở đây)
GRADE_BOUNDARIES = [4.0, 5.0, 5.5, 6.5, 7.0, 8.0, 8.5, 9.5]


# ═══════════════════════════════════════════════════════
# SQLALCHEMY MODELS (giữ nguyên từ v2.0)
# ═══════════════════════════════════════════════════════
class Khoa(Base):
    __tablename__ = "khoa"
    ma_khoa = Column(String(10), primary_key=True)
    ten_khoa = Column(String(200), nullable=False)
    ngay_tao = Column(DateTime, server_default=func.now())


class Nganh(Base):
    __tablename__ = "nganh"
    ma_nganh = Column(String(20), primary_key=True)
    ten_nganh = Column(String(200), nullable=False)
    ma_khoa = Column(String(10), ForeignKey("khoa.ma_khoa"), nullable=True)
    ngay_tao = Column(DateTime, server_default=func.now())


class GiangVien(Base):
    __tablename__ = "giang_vien"
    ma_giang_vien = Column(String(20), primary_key=True)
    ho = Column(String(50), nullable=False)
    ten = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    so_dien_thoai = Column(String(15), nullable=True)
    chuc_danh = Column(String(50), nullable=True)
    trang_thai_cong_tac = Column(String(20), default="Đang công tác")
    ma_khoa = Column(String(10), ForeignKey("khoa.ma_khoa"), nullable=True)
    ngay_tao = Column(DateTime, server_default=func.now())
    ngay_cap_nhat = Column(DateTime, server_default=func.now())


class LopHanhChinh(Base):
    __tablename__ = "lop_hanh_chinh"
    ma_lop = Column(String(20), primary_key=True)
    ten_lop = Column(String(100), nullable=False)
    khoa_hoc = Column(String(10), nullable=False)
    ma_nganh = Column(String(20), ForeignKey("nganh.ma_nganh"), nullable=True)
    ma_co_van = Column(String(20), ForeignKey("giang_vien.ma_giang_vien"), nullable=True)
    ngay_tao = Column(DateTime, server_default=func.now())


class SinhVien(Base):
    __tablename__ = "sinh_vien"
    ma_sinh_vien = Column(String(20), primary_key=True)
    ho = Column(String(50), nullable=False)
    ten = Column(String(50), nullable=False)
    ngay_sinh = Column(Date, nullable=False)
    gioi_tinh = Column(String(10), nullable=True)
    email = Column(String(100), unique=True, nullable=False)
    ma_nganh = Column(String(20), ForeignKey("nganh.ma_nganh"), nullable=True)
    ma_lop = Column(String(20), ForeignKey("lop_hanh_chinh.ma_lop"), nullable=True)
    khoa_hoc = Column(String(10), nullable=False)
    trang_thai_hoc_tap = Column(String(30), default="Đang học")
    ngay_tao = Column(DateTime, server_default=func.now())


class HocPhan(Base):
    __tablename__ = "hoc_phan"
    ma_hoc_phan = Column(String(20), primary_key=True)
    ma_mon = Column(String(10), unique=True, nullable=False)
    ten_mon = Column(String(200), nullable=False)
    so_tin_chi = Column(Integer, nullable=False)
    so_gio_ly_thuyet = Column(Integer, default=0)
    so_gio_thuc_hanh = Column(Integer, default=0)
    hoc_ky_de_xuat = Column(Integer, nullable=True)
    bat_buoc = Column(Boolean, default=True)
    ma_khoa = Column(String(10), ForeignKey("khoa.ma_khoa"), nullable=True)
    ngay_tao = Column(DateTime, server_default=func.now())


class HocKyNamHoc(Base):
    __tablename__ = "hoc_ky_nam_hoc"
    ma_hoc_ky = Column(String(50), primary_key=True)
    nam_hoc = Column(String(50), nullable=False)
    hoc_ky = Column(String(50), nullable=False)
    ngay_bat_dau = Column(Date, nullable=True)
    ngay_ket_thuc = Column(Date, nullable=True)


class DangKyHocPhan(Base):
    __tablename__ = "dang_ky_hoc_phan"
    ma_dang_ky = Column(Integer, primary_key=True, autoincrement=True)
    ma_sinh_vien = Column(String(20), ForeignKey("sinh_vien.ma_sinh_vien"), nullable=False)
    ma_hoc_phan = Column(String(20), ForeignKey("hoc_phan.ma_hoc_phan"), nullable=False)
    ma_hoc_ky = Column(String(50), ForeignKey("hoc_ky_nam_hoc.ma_hoc_ky"), nullable=False)
    ma_giang_vien = Column(String(20), ForeignKey("giang_vien.ma_giang_vien"), nullable=True)
    ngay_dang_ky = Column(Date, server_default=func.current_date())
    trang_thai = Column(String(30), default="Đã đăng ký")
    __table_args__ = (
        UniqueConstraint("ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky",
                         name="uq_dang_ky_sv_hp_hk"),
    )


class DiemHocPhan(Base):
    __tablename__ = "diem_hoc_phan"
    ma_diem = Column(Integer, primary_key=True, autoincrement=True)
    ma_dang_ky = Column(Integer, ForeignKey("dang_ky_hoc_phan.ma_dang_ky"), unique=True)
    diem_chuyen_can = Column(Numeric(4, 2), nullable=True)
    diem_bai_tap = Column(Numeric(4, 2), nullable=True)
    diem_giua_ky = Column(Numeric(4, 2), nullable=True)
    diem_cuoi_ky = Column(Numeric(4, 2), nullable=True)
    diem_tong_ket = Column(Numeric(4, 2), nullable=True)
    diem_chu = Column(String(2), nullable=True)
    diem_he_4 = Column(Numeric(3, 2), nullable=True)
    dat_mon = Column(Boolean, nullable=True)
    hoc_lai = Column(Boolean, default=False)
    ngay_cham = Column(DateTime, nullable=True)
    ngay_tao = Column(DateTime, server_default=func.now())


class TongHopKetQua(Base):
    __tablename__ = "tong_hop_ket_qua"
    ma_sinh_vien = Column(String(20), ForeignKey("sinh_vien.ma_sinh_vien"), primary_key=True)
    tong_tin_chi = Column(Integer, default=0)
    tin_chi_tich_luy = Column(Integer, default=0)
    gpa_he_10 = Column(Numeric(4, 2), nullable=True)
    gpa_he_4 = Column(Numeric(3, 2), nullable=True)
    canh_bao_hoc_vu = Column(Boolean, default=False)
    ngay_cap_nhat = Column(DateTime, server_default=func.now())


# ═══════════════════════════════════════════════════════
# DỮ LIỆU CỐ ĐỊNH (giữ nguyên từ v2.0)
# ═══════════════════════════════════════════════════════
KHOA_DATA = [
    ("CNTT1", "Khoa Công nghệ thông tin 1"),
    ("KKT", "Khoa Kế toán - Kiểm toán"),
    ("KVT", "Khoa Viễn thông 1"),
    ("KCB", "Khoa Cơ bản"),
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
TEN_DEM_NU = ["Thị", "Ngọc", "Thanh", "Phương", "Thuỷ", "Kim", "Hoàng", "Bích",
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

CHUC_DANH = ["ThS", "TS", "PGS.TS", "GS.TS"]
CHUC_DANH_W = [50, 35, 12, 3]

HB_QUOTA_RATE = 0.10
HB_TIERS = [
    {"loai": "KKHT Loại Xuất sắc", "min_drl": 90, "min_gpa": 3.60, "muc_tien": 8000000},
    {"loai": "KKHT Loại Giỏi",     "min_drl": 80, "min_gpa": 3.20, "muc_tien": 3600000},
    {"loai": "KKHT Loại Khá",      "min_drl": 65, "min_gpa": 2.50, "muc_tien": 1200000},
]

HK_MODIFIER = {
    "HK1-2021-22": +0.4,
    "HK2-2021-22": -0.3,
    "HK1-2022-23": -0.6,
    "HK2-2022-23": +0.6,
    "HK1-2023-24":  0.0,
    "HK2-2023-24": -0.8,
    "HK1-2024-25": +0.3,
    "HK2-2024-25": -0.2,
    "HK1-2025-26": +0.1,
}

# ═══════════════════════════════════════════════════════
# CHƯƠNG TRÌNH ĐÀO TẠO (giữ nguyên từ v2.0 — rút gọn)
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
    "KT": {"ma_nganh": "KETOAN", "ten_nganh": "Kế toán", "ma_khoa": "KKT", "ma_viet_tat": "KT", "chuong_trinh": CHUONG_TRINH_KT, "max_hk": 8, "so_lop_per_khoa": 4, "ty_le_nam": 0.35, "do_kho_nganh": 0.0},
    "VT": {"ma_nganh": "DTVT", "ten_nganh": "Kỹ thuật Điện tử viễn thông", "ma_khoa": "KVT", "ma_viet_tat": "VT", "chuong_trinh": CHUONG_TRINH_VT, "max_hk": 9, "so_lop_per_khoa": 4, "ty_le_nam": 0.75, "do_kho_nganh": -0.2},
    "CN": {"ma_nganh": "CNTT", "ten_nganh": "Công nghệ thông tin", "ma_khoa": "CNTT1", "ma_viet_tat": "CN", "chuong_trinh": CHUONG_TRINH_CN, "max_hk": 9, "so_lop_per_khoa": 4, "ty_le_nam": 0.85, "do_kho_nganh": -0.4},
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
    "buoc_thoi_hoc_gpa_hard": 0.5,
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

HK_MASTER = {
    "HK1-2021-22": ("2021-2022", "Học kỳ 1", date(2021, 9, 6),  date(2022, 1, 15)),
    "HK2-2021-22": ("2021-2022", "Học kỳ 2", date(2022, 2, 14), date(2022, 6, 30)),
    "HK1-2022-23": ("2022-2023", "Học kỳ 1", date(2022, 9, 5),  date(2023, 1, 14)),
    "HK2-2022-23": ("2022-2023", "Học kỳ 2", date(2023, 2, 13), date(2023, 6, 30)),
    "HK1-2023-24": ("2023-2024", "Học kỳ 1", date(2023, 9, 4),  date(2024, 1, 13)),
    "HK2-2023-24": ("2023-2024", "Học kỳ 2", date(2024, 2, 12), date(2024, 6, 28)),
    "HK1-2024-25": ("2024-2025", "Học kỳ 1", date(2024, 9, 2),  date(2025, 1, 11)),
    "HK2-2024-25": ("2024-2025", "Học kỳ 2", date(2025, 2, 10), date(2025, 6, 27)),
    "HK1-2025-26": ("2025-2026", "Học kỳ 1", date(2025, 9, 1),  date(2026, 2, 8)),
}

HK_SEQ_BY_COHORT = {
    "B21": ["HK1-2021-22","HK2-2021-22","HK1-2022-23","HK2-2022-23","HK1-2023-24","HK2-2023-24","HK1-2024-25","HK2-2024-25","HK1-2025-26"],
    "B22": ["HK1-2022-23","HK2-2022-23","HK1-2023-24","HK2-2023-24","HK1-2024-25","HK2-2024-25","HK1-2025-26"],
    "B23": ["HK1-2023-24","HK2-2023-24","HK1-2024-25","HK2-2024-25","HK1-2025-26"],
    "B24": ["HK1-2024-25","HK2-2024-25","HK1-2025-26"],
}

# ★ [DYNAMIC_RECENT_HKS] v2.2 — Tính động thay vì hardcode
# Lý do: nếu chạy lại script 1 năm sau, RECENT_HKS hardcode sẽ sai
# Logic: HK "gần đây" = kết thúc trong vòng 180 ngày tính từ hôm nay
_TODAY = date.today()
RECENT_HKS = {
    ma_hk
    for ma_hk, (_, _, _, end_date) in HK_MASTER.items()
    if (_TODAY - end_date).days < 180
}
# Fallback: nếu không có HK nào trong 180 ngày (dataset lịch sử), lấy 2 HK cuối nhất
if not RECENT_HKS:
    all_hk_sorted = sorted(HK_MASTER.keys(), key=lambda k: HK_MASTER[k][3], reverse=True)
    RECENT_HKS = set(all_hk_sorted[:2])

# ★ [TRANG_THAI_VARIANTS] v2.2 — Các biến thể format của trang_thai
# Giải thích: hệ thống cũ (trước 2022) lưu trạng thái không dấu, uppercase.
# Khi migrate, một số record giữ nguyên format cũ → ETL phải standardize.
TRANG_THAI_VARIANTS = {
    "Đã đăng ký":  ["DA DANG KY",  "da dang ky",  "Registered",   "DANG KY"],
    "Đã rút":      ["DA RUT",       "da rut",       "Withdrawn",    "RUT MON"],
}

# ★ [ORPHAN_IDS] v2.2 — Mã SV "ma" (ghost) cho orphan records
# Đây là mã SV tồn tại trong CSV/JSON nhưng KHÔNG CÓ trong PostgreSQL
# Mô phỏng: SV từ hệ thống cũ chưa migrate, hoặc SV bị xóa nhầm
_GHOST_IDS_CSV  = [f"B19DCKT{i:03d}" for i in range(1, 8)]   # 7 SV "ma" cho CSV
_GHOST_IDS_JSON = [f"B19DCCN{i:03d}" for i in range(1, 6)]   # 5 SV "ma" cho JSON

# ★ [ENCODING_ISSUES] v2.2 — Mapping lỗi encoding tiếng Việt thường gặp
# Windows-1252 → UTF-8 không khớp, xảy ra khi mở/lưu Excel trên máy Windows cũ
# ETL Transform phải detect và flag các record này
_ENCODING_CORRUPTIONS = [
    ("ễ", "?"), ("ắ", "?"), ("ồ", "?"), ("ổ", "?"), ("ọ", "?"),
    ("ơ", "o"), ("ư", "u"), ("đ", "d"), ("ả", "a"), ("ẽ", "e"),
]

GV_DATA = {
    "KKT": [("Nguyễn","Văn Thành"),("Trần","Thị Hương"),("Lê","Quốc Bình"),("Phạm","Minh Tuấn"),("Hoàng","Thị Lan"),("Vũ","Đức Mạnh"),("Đỗ","Thị Ngọc"),("Bùi","Văn Hải"),("Ngô","Thị Mai"),("Đặng","Quốc Hùng")],
    "KVT": [("Nguyễn","Hữu Đức"),("Trần","Quang Vinh"),("Lê","Minh Sơn"),("Phạm","Thị Hoa"),("Hoàng","Văn Long"),("Vũ","Thị Hiền"),("Đỗ","Anh Tuấn"),("Bùi","Quốc Đạt"),("Ngô","Văn Phong"),("Đặng","Thị Thu")],
    "CNTT1": [("Nguyễn","Minh Hiếu"),("Trần","Văn Cường"),("Lê","Thị Phương"),("Phạm","Đức Anh"),("Hoàng","Quốc Trung"),("Vũ","Văn Nam"),("Đỗ","Thị Linh"),("Bùi","Minh Khoa"),("Ngô","Đức Thịnh"),("Đặng","Văn Huy")],
    "KCB": [("Nguyễn","Thị Hạnh"),("Trần","Đức Thắng"),("Lê","Văn Khánh"),("Phạm","Thị Dung"),("Hoàng","Minh Trí"),("Vũ","Quốc Phương"),("Đỗ","Thị Hằng"),("Bùi","Văn Tâm"),("Ngô","Thị Thuỷ"),("Đặng","Minh Đức"),("Lê","Thuỷ Tiên"),("Phạm","Anh Thư"),("Hoàng","Bích Ngọc"),("Trần","Quốc Việt"),("Nguyễn","Thị Huyền")],
}


# ═══════════════════════════════════════════════════════
# HELPERS — ĐIỂM SỐ
# ═══════════════════════════════════════════════════════

def _diem_sang_chu(d: float):
    """
    Chuyển điểm số → điểm chữ + hệ 4.
    ★ v2.1: Giữ ngưỡng A+ từ 9.5 (không thay đổi).
    Bỏ hàm _cap_gpa_hk nhân tạo — SV xuất sắc xứng đáng được GPA 4.0.
    """
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
    """
    ★ [GRADE_BOUNDARY] v2.1 — Kéo điểm về gần ngưỡng ranh giới.
    
    Giải thích: Trong thực tế, phân phối điểm có "spike" tự nhiên
    ngay trên các ngưỡng đổi bậc (4.0, 5.0, 5.5...) vì SV cố gắng
    vượt qua, và GV có xu hướng làm tròn lên khi điểm gần ngưỡng.
    Ví dụ: nhiều điểm 5.1, 5.6, 7.1 hơn là 4.8, 5.3, 6.9.
    """
    if random.random() > NATURAL_IMPERFECTION["boundary_score_boost"]:
        return score

    # Tìm ngưỡng gần nhất phía trên score
    for boundary in GRADE_BOUNDARIES:
        # Nếu score nằm trong vùng [boundary-0.4, boundary-0.05]
        # → kéo lên ngưỡng + random nhỏ
        if boundary - 0.4 <= score < boundary - 0.05:
            snapped = boundary + round(random.uniform(0.05, 0.35), 2)
            return min(snapped, 10.0)
    return score


def _gen_diem_raw(profile: str) -> float:
    """
    ★ v2.1: Thêm natural noise ±0.15 vào điểm để phá vỡ distribution "quá đẹp".
    
    Giải thích noise: Điểm thực tế bị ảnh hưởng bởi nhiều yếu tố nhỏ
    (mood ngày thi, câu hỏi may mắn/xui, GV chấm tay có sai số...).
    Noise ±0.15 mô phỏng những biến động này mà không thay đổi profile tổng thể.
    """
    r = random.random()
    if profile == "xuất sắc":
        # v2.1: Giữ tự nhiên — không giới hạn nhân tạo A+
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

    # ★ [NATURAL_NOISE] Thêm noise tự nhiên ±0.15
    noise = round(random.gauss(0, 0.08), 2)  # std=0.08, 95% nằm trong ±0.16
    return round(max(0.0, min(10.0, base + noise)), 2)


def _gen_diem_mon(profile: str, do_kho: float, hk_idx: int, la_hoc_lai: bool, hk_modifier: float = 0.0) -> float:
    p = "trung bình" if (la_hoc_lai and profile == "yếu") else profile
    base = _gen_diem_raw(p)
    base -= do_kho * 0.25
    base += min((hk_idx - 1) * 0.10, 0.7)
    base += hk_modifier
    if la_hoc_lai:
        base += 1.2
    # ★ [GRADE_BOUNDARY] Kéo điểm về ngưỡng ranh giới tự nhiên
    base = _snap_to_boundary(base)
    return round(max(0.0, min(10.0, base)), 2)


def _tao_diem_record(ma_dk, do_kho: float, profile: str, hk_idx: int,
                     la_hoc_lai: bool, ngay_ket_thuc_hk: date,
                     hk_modifier: float = 0.0,
                     is_recent_hk: bool = False) -> dict:
    """
    ★ v2.1: Thêm tham số is_recent_hk để xử lý điểm chưa nhập tự nhiên.
    
    Giải thích is_recent_hk:
    - HK gần nhất (HK2-2024-25, HK1-2025-26) vẫn đang trong giai đoạn
      GV nhập điểm. Một số GV nhập muộn hoặc chưa nhập.
    - Thay vì inject sau, ta sinh trực tiếp record có điểm NULL.
    - ETL pipeline sẽ phát hiện và xử lý các record này.
    """
    # ★ [NATURAL_MISSING] Kiểm tra điểm chưa được GV nhập
    missing_rate = (
        NATURAL_IMPERFECTION["grade_not_entered_recent_hk"] if is_recent_hk
        else NATURAL_IMPERFECTION["grade_not_entered_older_hk"]
    )

    if random.random() < missing_rate:
        # GV chưa nhập điểm — record có đăng ký nhưng tất cả điểm = NULL
        # Đây là trường hợp hoàn toàn tự nhiên, không phải lỗi
        return {
            "ma_dang_ky": ma_dk,
            "diem_chuyen_can": None, "diem_bai_tap": None,
            "diem_giua_ky": None, "diem_cuoi_ky": None,
            "diem_tong_ket": None, "diem_chu": None,
            "diem_he_4": None, "dat_mon": None,
            "hoc_lai": la_hoc_lai,
            "ngay_cham": None,   # chưa chấm
            "_grade_not_entered": True,  # flag nội bộ
        }

    # Sinh điểm bình thường
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

    cc = round(max(0.0, min(10.0, cc + hk_modifier * 0.3)), 2)
    bt = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai, hk_modifier)
    gk = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai, hk_modifier)
    ck = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai, hk_modifier)

    dtk = round(max(0.0, min(10.0, 0.1 * cc + 0.1 * bt + 0.2 * gk + 0.6 * ck)), 2)
    # ★ [GRADE_BOUNDARY] Áp dụng cho điểm tổng kết
    dtk = _snap_to_boundary(dtk)
    dtk = round(max(0.0, min(10.0, dtk)), 2)

    chu, he4 = _diem_sang_chu(dtk)

    # ★ [LATE_GRADE_ENTRY] GV nhập điểm muộn — phân phối thực tế hơn
    r = random.random()
    if r < NATURAL_IMPERFECTION["very_late_grade_entry_rate"]:
        # Nhập rất muộn: 35-56 ngày sau kỳ thi
        days_after = random.randint(35, 56)
    elif r < NATURAL_IMPERFECTION["very_late_grade_entry_rate"] + NATURAL_IMPERFECTION["late_grade_entry_rate"]:
        # Nhập muộn vừa: 22-34 ngày
        days_after = random.randint(22, 34)
    else:
        # Nhập đúng hạn: 7-21 ngày (phần lớn GV)
        days_after = random.randint(7, 21)

    return {
        "ma_dang_ky": ma_dk,
        "diem_chuyen_can": cc, "diem_bai_tap": bt,
        "diem_giua_ky": gk, "diem_cuoi_ky": ck,
        "diem_tong_ket": dtk, "diem_chu": chu,
        "diem_he_4": float(he4), "dat_mon": dtk >= 4.0,
        "hoc_lai": la_hoc_lai,
        "ngay_cham": datetime.combine(
            ngay_ket_thuc_hk + timedelta(days=days_after),
            datetime.min.time()
        ),
        "_grade_not_entered": False,
    }


def _gen_sdt() -> str:
    p = random.choice(["032","033","034","035","036","038","086","096","097","098"])
    return p + str(random.randint(1000000, 9999999))


# ─────────────────────────────────────────────────────────
# ★ v2.2 HELPERS — Cross-Source Integrity
# ─────────────────────────────────────────────────────────

def _corrupt_encoding(name: str) -> str:
    """
    ★ [ENCODING_ISSUES] v2.2 — Mô phỏng lỗi encoding tên tiếng Việt.

    Tại sao xảy ra trong thực tế:
    - Nhân viên phòng CTSV dùng máy Windows cũ, Excel mặc định lưu Windows-1252
    - Khi mở file trên máy khác (UTF-8), ký tự đặc biệt bị render sai
    - ETL pipeline phát hiện qua: chuỗi chứa '?', hoặc so sánh với PostgreSQL thấy lệch

    Returns: tên với 1 ký tự đặc biệt bị corrupt
    """
    if not name:
        return name
    corruption = random.choice(_ENCODING_CORRUPTIONS)
    old_char, new_char = corruption
    if old_char in name:
        # Chỉ corrupt 1 lần duy nhất (không corrupt toàn bộ)
        return name.replace(old_char, new_char, 1)
    # Fallback: thay ký tự cuối bằng '?'
    return name[:-1] + "?"


def _corrupt_date_format(date_str: str) -> str:
    """
    ★ [DATE_FORMAT_MISMATCH] v2.2 — Chuyển ngày ISO sang format không chuẩn.

    Tại sao xảy ra trong thực tế:
    - Vendor API tài chính dùng format Việt Nam (dd/mm/yyyy) thay vì ISO (yyyy-mm-dd)
    - Một số endpoint trả "Sep 15 2024" (US locale)
    - ETL Transform cần pd.to_datetime(errors='coerce') + format detection

    Ví dụ: "2024-09-15" → "15/09/2024"
    """
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        format_choice = random.choice([
            dt.strftime("%d/%m/%Y"),        # "15/09/2024" ← phổ biến nhất
            dt.strftime("%d-%m-%Y"),        # "15-09-2024"
            dt.strftime("%m/%d/%Y"),        # "09/15/2024" (US format)
            str(date_str).replace("-", "."),# "2024.09.15"
        ])
        return format_choice
    except (ValueError, TypeError):
        return date_str


def _make_trang_thai_variant(trang_thai: str) -> str:
    """
    ★ [TRANG_THAI_VARIANTS] v2.2 — Sinh biến thể format của trang_thai.

    Tại sao xảy ra trong thực tế:
    - Hệ thống đăng ký học phần cũ (trước 2022) lưu uppercase không dấu
    - Khi migrate, ~1.5% records giữ nguyên format cũ
    - ETL Transform dùng mapping dict để standardize về chuẩn

    Ví dụ: "Đã đăng ký" → "DA DANG KY"
    """
    variants = TRANG_THAI_VARIANTS.get(trang_thai, [])
    if variants:
        return random.choice(variants)
    return trang_thai


def _build_ghost_csv_record(ma_hk: str, ghost_id: str) -> dict:
    """
    ★ [ORPHAN_RECORDS] v2.2 — Tạo record CSV với ma_sinh_vien không có trong PG.

    Tại sao xảy ra trong thực tế:
    - SV từ hệ thống cũ (trước khi deploy PostgreSQL) vẫn còn trong Excel của phòng CTSV
    - SV được xóa khỏi PostgreSQL vì lý do hành chính, nhưng CSV chưa được cập nhật
    - ETL phát hiện qua LEFT JOIN với dim_sinh_vien → sinh_vien_key = NULL

    Ghost record: có đủ cấu trúc nhưng ma_sinh_vien không tồn tại trong source DB.
    """
    drl = random.randint(40, 85)
    return {
        "ma_sinh_vien": ghost_id,       # ← key không tồn tại trong PostgreSQL
        "hoc_ky": ma_hk,
        "diem_ren_luyen": drl,
        "xep_loai_rl": "Khá" if drl >= 65 else "Trung bình",
        "loai_hoc_bong": "",
        "muc_tien_hb": 0,
        "hinh_thuc_ky_luat": "",
        "ly_do_ky_luat": "",
    }


def _build_ghost_json_record(ma_hk: str, ghost_id: str, ngay_bat_dau) -> dict:
    """★ [ORPHAN_RECORDS] v2.2 — Tạo record JSON với ma_sinh_vien không tồn tại."""
    hp = random.randint(4, 8) * 3 * random.choice([440000, 460000, 480000])
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
        # Truyền vào date object thuần Python, không phải ORM object
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
# PHÂN CÔNG GIẢNG VIÊN (giữ nguyên từ v2.0)
# ═══════════════════════════════════════════════════════
def _collect_all_courses():
    courses_by_khoa = {}
    dc_courses = []
    for _, cfg in NGANH_CONFIG.items():
        ma_khoa = cfg["ma_khoa"]
        ct = cfg["chuong_trinh"]
        max_hk = cfg["max_hk"]
        for hk_idx in range(1, max_hk + 1):
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

def _gen_tai_chinh_record(ma_sv: str, ma_hk: str, hk_obj,
                           profile: str, mons_hk: list,
                           hk_mod: float) -> dict:
    """
    ★ v2.1: Tạo record tài chính với các imperfection tự nhiên:
    - Thanh toán 2 đợt (installment)
    - Sai số làm tròn tự nhiên trong con_no
    - Ngày đóng tiền tập trung đầu tháng (thực tế hơn)
    """
    tc_hk = sum(m[2] for m in mons_hk)
    gia_tc = random.choice([440000, 460000, 480000, 500000])
    hoc_phi = tc_hk * gia_tc

    duoc_mg = False
    ly_do_mg, so_tien_mg = "", 0
    if random.random() < 0.08:
        duoc_mg = True
        ly_do_mg = random.choice(["Chính sách","Hộ nghèo","Cận hộ nghèo","Dân tộc thiểu số","Mồ côi","Con thương binh"])
        so_tien_mg = int(hoc_phi * random.choice([0.3, 0.5, 0.7, 1.0]))

    hp_phai_dong = hoc_phi - so_tien_mg

    # ★ [PAYMENT_INSTALLMENT] Xác định hành vi thanh toán
    installment_roll = random.random()

    if profile in ("xuất sắc", "giỏi"):
        # SV giỏi: 95% đóng đủ ngay, 5% đóng 2 đợt
        if installment_roll < 0.95:
            da_dong = hp_phai_dong
        else:
            # Đợt 1 trong tháng đầu, đợt 2 sau 4-6 tuần
            da_dong = int(hp_phai_dong * random.uniform(0.6, 0.8))
    elif profile == "khá":
        if installment_roll < 1 - NATURAL_IMPERFECTION["payment_installment_rate"]:
            da_dong = hp_phai_dong
        else:
            lo, hi = NATURAL_IMPERFECTION["payment_installment_first"]
            da_dong = int(hp_phai_dong * random.uniform(lo, hi))
    elif profile == "trung bình":
        if installment_roll < 0.80:
            da_dong = hp_phai_dong
        else:
            da_dong = int(hp_phai_dong * random.uniform(0.5, 0.85))
    else:  # yếu
        if installment_roll < 0.50:
            da_dong = hp_phai_dong
        else:
            da_dong = int(hp_phai_dong * random.uniform(0.1, 0.6))

    # ★ [FINANCIAL_ROUNDING] Sai số làm tròn tự nhiên
    # Hệ thống tài chính khác nhau làm tròn khác nhau → con_no có thể lệch nhỏ
    con_no_exact = hp_phai_dong - da_dong
    if random.random() < NATURAL_IMPERFECTION["financial_rounding_rate"]:
        lo, hi = NATURAL_IMPERFECTION["financial_rounding_range"]
        rounding_error = random.randint(lo, hi)
        con_no = max(0, con_no_exact + rounding_error)
    else:
        con_no = max(0, con_no_exact)

    # ★ [SKEWED_PAYMENT_DATE] Ngày đóng tiền tập trung đầu tháng
    # Thực tế SV hay đóng tiền trong tuần 2-4 sau khi HK bắt đầu
    # và tập trung vào đầu tháng (ngày 1-10) do lương cha mẹ đầu tháng
    days_offset = random.choices(
        [random.randint(5, 12), random.randint(13, 25), random.randint(26, 45)],
        weights=[50, 30, 20]
    )[0]
    ngay_dong = hk_obj.ngay_bat_dau + timedelta(days=days_offset)

    return {
        "ma_sinh_vien": ma_sv,
        "hoc_ky": ma_hk,
        "hoc_phi_phai_dong": hp_phai_dong,
        "da_dong": da_dong,
        "con_no": con_no,
        "duoc_mien_giam": duoc_mg,
        "ly_do_mien_giam": ly_do_mg,
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
    print(f" v2.1 — Realistic Imperfections")
    print(f" 3 ngành: Kế toán, CNTT, Điện tử viễn thông")
    print(f" Tổng dự kiến: ~{total_sv_est} SV | 4 khóa B21-B24")
    print(f" Natural imperfection baked-in (không cần inject_errors)")
    print("=" * 70)
    
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
        s.add_all(khoas)
        s.flush()
        print(f"\n[1] Khoa        : {len(khoas)}")

        # ── 2. Ngành ──
        nganh_objs = []
        for cfg in NGANH_CONFIG.values():
            n = Nganh(ma_nganh=cfg["ma_nganh"], ten_nganh=cfg["ten_nganh"], ma_khoa=cfg["ma_khoa"])
            nganh_objs.append(n)
        s.add_all(nganh_objs)
        s.flush()
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
                    trang_thai_cong_tac="Đang công tác",
                    ma_khoa=ma_khoa_gv,
                ))
                gv_ids_by_khoa[ma_khoa_gv].append(ma_gv)
                gv_counter += 1
        s.add_all(gvs)
        s.flush()
        print(f"[3] Giảng viên  : {len(gvs)}")

        # ── 4. Học kỳ ──
        all_hk_keys = set()
        for seq in HK_SEQ_BY_COHORT.values():
            all_hk_keys.update(seq)
        hoc_kys = []
        for ma_hk in sorted(all_hk_keys):
            nh, hk, bd, kt = HK_MASTER[ma_hk]
            hoc_kys.append(HocKyNamHoc(ma_hoc_ky=ma_hk, nam_hoc=nh, hoc_ky=hk,
                                       ngay_bat_dau=bd, ngay_ket_thuc=kt))
        s.add_all(hoc_kys)
        s.flush()
        hk_by_key = {hk.ma_hoc_ky: hk for hk in hoc_kys}
        print(f"[4] Học kỳ      : {len(hoc_kys)}")
        hk_ngay_bat_dau_map = {
        ma_hk: hk_obj.ngay_bat_dau
        for ma_hk, hk_obj in hk_by_key.items()
        }
        # ── 5. Phân công GV ──
        print("\n[5] Đang tính toán phân công Giảng viên...")
        courses_by_khoa, dc_courses = _collect_all_courses()
        global_gv_per_mon = _assign_instructors_to_courses(
            gv_ids_by_khoa=gv_ids_by_khoa,
            courses_by_khoa=courses_by_khoa,
            dc_courses=dc_courses,
        )
        print(f"    -> Đã phân công cho {len(global_gv_per_mon)} môn học.")

        total_sv_created = 0
        total_dk_created = 0
        total_diem_created = 0
        total_missing_grades = 0
        total_withdrawals = 0
        temp_hk_data = {}

        for nganh_key, cfg in NGANH_CONFIG.items():
            print(f"\n{'─' * 50}")
            print(f" NGÀNH: {cfg['ten_nganh']} ({cfg['ma_nganh']})")
            print(f"{'─' * 50}")

            vt = cfg["ma_viet_tat"]
            chuong_trinh = cfg["chuong_trinh"]
            max_hk = cfg["max_hk"]
            do_kho_nganh = cfg.get("do_kho_nganh", 0.0)

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
            s.add_all(lops)
            s.flush()
            print(f"  -> {len(lops)} Lớp hành chính")

            # ── 7. Học phần ──
            hoc_phans = []
            hp_lookup = {}
            for hk_idx in range(1, max_hk + 1):
                for ma_mon, ten_mon, tc, lt, th, do_kho in mon_by_hk[hk_idx]:
                    hp_lookup[ma_mon] = (ma_mon, do_kho)
                    if ma_mon in global_hp_inserted:
                        continue
                    global_hp_inserted.add(ma_mon)
                    hp_ma_khoa = "KCB" if ma_mon.startswith("DC") else cfg["ma_khoa"]
                    hoc_phans.append(HocPhan(
                        ma_hoc_phan=ma_mon, ma_mon=ma_mon, ten_mon=ten_mon,
                        so_tin_chi=tc, so_gio_ly_thuyet=lt, so_gio_thuc_hanh=th,
                        hoc_ky_de_xuat=hk_idx, bat_buoc=True, ma_khoa=hp_ma_khoa,
                    ))
            s.add_all(hoc_phans)
            s.flush()
            print(f"  -> {len(hoc_phans)} Học phần mới (tổng DB: {len(global_hp_inserted)})")

            # ── 8. Sinh viên ──
            svs = []
            hoc_luc_sv = {}
            for cohort in COHORTS:
                nam_nhap = COHORT_CONFIG[cohort][0]
                so_sv = SV_PER_KHOA[cohort]
                for stt in range(1, so_sv + 1):
                    is_nam = random.random() < cfg["ty_le_nam"]
                    gioi = "Nam" if is_nam else "Nữ"
                    ma_sv = f"{cohort}DC{vt}{stt:03d}"
                    email = f"{cohort.lower()}dc{vt.lower()}{stt:03d}@student.ptit.edu.vn"
                    nam_sinh = nam_nhap - random.randint(18, 21)
                    ho = random.choice(HO_LIST)
                    if gioi == "Nam":
                        ten_dem = random.choice(TEN_DEM_NAM)
                        ten = random.choice(TEN_NAM)
                    else:
                        ten_dem = random.choice(TEN_DEM_NU)
                        ten = random.choice(TEN_NU)
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
            s.add_all(svs)
            s.flush()
            total_sv_created += len(svs)
            print(f"  -> {len(svs)} SV (tất cả 'Đang học')")

            # ── Tracking state ──
            sv_active = {sv.ma_sinh_vien: True for sv in svs}
            sv_cum = {sv.ma_sinh_vien: {"w_sum": 0.0, "total_tc": 0, "passed_tc": 0} for sv in svs}
            sv_final_status = {}

            dk_buf = []
            dk_set = set()
            sv_failed_courses = {}
            sv_hk_gpa = {}

            # ══════════════════════════════════════════════════════════
            # VÒNG LẶP CHÍNH: TỪNG KHÓA → TỪNG HK → TỪNG SV
            # ══════════════════════════════════════════════════════════
            for cohort in COHORTS:
                hk_seq = HK_SEQ_BY_COHORT[cohort]
                cohort_svs = [sv for sv in svs if sv.khoa_hoc == cohort]

                for hk_idx_0, ma_hk in enumerate(hk_seq):
                    hk_idx = hk_idx_0 + 1
                    if hk_idx > max_hk:
                        break

                    hk_obj = hk_by_key.get(ma_hk)
                    if hk_obj is None:
                        continue

                    is_graduation_hk = (hk_idx == max_hk)
                    is_recent = ma_hk in RECENT_HKS
                    hk_mod = HK_MODIFIER.get(ma_hk, 0.0) + do_kho_nganh

                    active = [sv for sv in cohort_svs if sv_active[sv.ma_sinh_vien]]
                    if not active:
                        break

                    mons = mon_by_hk[hk_idx]
                    hk_eval_data = []

                    for sv in active:
                        ma_sv = sv.ma_sinh_vien
                        profile = hoc_luc_sv[ma_sv]

                        if is_graduation_hk:
                            cum = sv_cum[ma_sv]
                            cum_gpa = (cum["w_sum"] / cum["total_tc"] if cum["total_tc"] > 0 else 0)
                            if cum_gpa < EVAL_CRITERIA["tot_nghiep_gpa_min"]:
                                # ★ [FIX_GRADUATION_BUG] SV không đủ điều kiện → vẫn là "Đang học"
                                # (không set tốt nghiệp, không set active=False)
                                continue

                        sv_grades_raw = []

                        for (ma_mon, ten_mon, tc, lt, th, do_kho) in mons:
                            key = (ma_sv, ma_mon, ma_hk)
                            if key in dk_set:
                                continue

                            # ★ [COURSE_WITHDRAWAL] SV rút môn tự nhiên
                            # Chỉ áp dụng cho môn không phải bắt buộc hoặc môn khó
                            if (not is_graduation_hk
                                    and do_kho > 1.0
                                    and random.random() < NATURAL_IMPERFECTION["course_withdrawal_rate"]):
                                # SV rút môn: tạo dang_ky với trang_thai = "Đã rút" nhưng KHÔNG có điểm
                                dk_set.add(key)
                                dk_buf.append({
                                    "ma_sinh_vien": ma_sv,
                                    "ma_hoc_phan": ma_mon,
                                    "ma_hoc_ky": ma_hk,
                                    "ma_giang_vien": None,
                                    "ngay_dang_ky": hk_obj.ngay_bat_dau + timedelta(days=random.randint(1, 8)),
                                    "trang_thai": "Đã rút",      # ★ trạng thái đặc biệt
                                    "_hk_idx": hk_idx, "_do_kho": 0, "_profile": profile,
                                    "_kt_hk": hk_obj.ngay_ket_thuc, "_so_tin_chi": tc,
                                    "_hk_modifier": hk_mod, "_pre_grade": None,  # không có điểm
                                    "_withdrawn": True,
                                })
                                total_withdrawals += 1
                                continue

                            dk_set.add(key)

                            gv_list = global_gv_per_mon.get(ma_mon)
                            if not gv_list:
                                continue

                            grade = _tao_diem_record(
                                None, do_kho, profile, hk_idx, False,
                                hk_obj.ngay_ket_thuc, hk_mod,
                                is_recent_hk=is_recent
                            )

                            # ★ [LATE_REGISTRATION] 3% đăng ký muộn
                            if random.random() < NATURAL_IMPERFECTION["late_registration_rate"]:
                                lo, hi = NATURAL_IMPERFECTION["late_registration_days"]
                                reg_days = random.randint(lo, hi)
                            else:
                                reg_days = random.randint(1, 10)

                            # ★ [TRANG_THAI_VARIANTS] v2.2 — 1.5% trang_thai dùng format cũ
                            trang_thai_dk = "Đã đăng ký"
                            if random.random() < NATURAL_IMPERFECTION["trang_thai_variant_rate"]:
                                trang_thai_dk = _make_trang_thai_variant("Đã đăng ký")

                            sv_grades_raw.append({
                                "ma_mon": ma_mon, "tc": tc, "ten_mon": ten_mon,
                                "he4": grade["diem_he_4"] or 0.0,
                                "chu": grade["diem_chu"],
                                "tong_ket": grade["diem_tong_ket"],
                                "dat": grade["dat_mon"],
                                "grade_full": grade,
                                "gv": random.choice(gv_list),
                                "do_kho": do_kho, "lt": lt, "th": th,
                                "reg_days": reg_days,
                                "not_entered": grade.get("_grade_not_entered", False),
                            })

                            if grade.get("_grade_not_entered"):
                                total_missing_grades += 1

                        # ── Học lại: môn đã rớt ──
                        failed_list = sv_failed_courses.get(ma_sv, [])
                        retake_this_hk = []
                        retake_candidates = failed_list[:2]

                        for (r_ma_mon, r_ten_mon, r_tc, r_lt, r_th, r_do_kho) in retake_candidates:
                            key = (ma_sv, r_ma_mon, ma_hk)
                            if key in dk_set:
                                continue
                            dk_set.add(key)
                            gv_list = global_gv_per_mon.get(r_ma_mon)
                            if not gv_list:
                                continue
                            grade = _tao_diem_record(
                                None, r_do_kho, profile, hk_idx, True,
                                hk_obj.ngay_ket_thuc, hk_mod,
                                is_recent_hk=is_recent
                            )
                            sv_grades_raw.append({
                                "ma_mon": r_ma_mon, "tc": r_tc, "ten_mon": r_ten_mon,
                                "he4": grade["diem_he_4"] or 0.0,
                                "chu": grade["diem_chu"],
                                "tong_ket": grade["diem_tong_ket"],
                                "dat": grade["dat_mon"],
                                "grade_full": grade,
                                "gv": random.choice(gv_list),
                                "do_kho": r_do_kho, "lt": r_lt, "th": r_th,
                                "reg_days": random.randint(1, 10),
                                "not_entered": grade.get("_grade_not_entered", False),
                            })
                            retake_this_hk.append((r_ma_mon, r_ten_mon, r_tc, r_lt, r_th, r_do_kho))
                            if grade.get("_grade_not_entered"):
                                total_missing_grades += 1

                        if not sv_grades_raw:
                            continue

                        # Thêm vào dk_buf
                        for g in sv_grades_raw:
                            dk_buf.append({
                                "ma_sinh_vien": ma_sv,
                                "ma_hoc_phan": g["ma_mon"],
                                "ma_hoc_ky": ma_hk,
                                "ma_giang_vien": g["gv"],
                                "ngay_dang_ky": hk_obj.ngay_bat_dau + timedelta(days=g.get("reg_days", 1)),
                                # ★ [TRANG_THAI_VARIANTS] dùng trang_thai_dk (có thể là format cũ)
                                "trang_thai": trang_thai_dk,
                                "_hk_idx": hk_idx, "_do_kho": 0, "_profile": profile,
                                "_kt_hk": hk_obj.ngay_ket_thuc, "_so_tin_chi": g["tc"],
                                "_hk_modifier": hk_mod,
                                "_pre_grade": g["grade_full"],
                                "_withdrawn": False,
                            })

                        # Cập nhật GPA (chỉ tính môn có điểm)
                        for g in sv_grades_raw:
                            if g["not_entered"] or g["he4"] is None:
                                continue
                            sv_cum[ma_sv]["w_sum"] += g["he4"] * g["tc"]
                            sv_cum[ma_sv]["total_tc"] += g["tc"]
                            if g["dat"]:
                                sv_cum[ma_sv]["passed_tc"] += g["tc"]

                        c = sv_cum[ma_sv]
                        if c["total_tc"] > 0:
                            cum_gpa = round(max(c["w_sum"] / c["total_tc"], 0.10), 4)
                        else:
                            cum_gpa = 0.10

                        # GPA học kỳ (chỉ môn có điểm, không bao gồm môn đã rút/chưa nhập)
                        graded = [g for g in sv_grades_raw if not g["not_entered"] and g["he4"] is not None]
                        if graded:
                            total_w = sum(g["he4"] * g["tc"] for g in graded)
                            total_tc = sum(g["tc"] for g in graded)
                            gpa_hk = round(total_w / total_tc, 4) if total_tc > 0 else 0.10
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
                            ma_mon_retake = course_info[0]
                            retake_grade = next(
                                (g for g in sv_grades_raw if g["ma_mon"] == ma_mon_retake
                                 and g["grade_full"].get("hoc_lai")), None
                            )
                            if retake_grade and retake_grade.get("dat") and not retake_grade.get("not_entered"):
                                if course_info in sv_failed_courses.get(ma_sv, []):
                                    sv_failed_courses[ma_sv].remove(course_info)

                        for g in sv_grades_raw:
                            if not g.get("dat") and not g["grade_full"].get("hoc_lai") and not g["not_entered"]:
                                course_info = (g["ma_mon"], g["ten_mon"], g["tc"],
                                               g["lt"], g["th"], g["do_kho"])
                                sv_failed_courses.setdefault(ma_sv, [])
                                if course_info not in sv_failed_courses[ma_sv]:
                                    sv_failed_courses[ma_sv].append(course_info)

                    # ── Đánh giá sau mỗi HK ──
                    if is_graduation_hk:
                        for ev in hk_eval_data:
                            sv_final_status[ev["ma_sv"]] = "Tốt nghiệp"
                            sv_active[ev["ma_sv"]] = False
                    else:
                        for ev in hk_eval_data:
                            ma_sv = ev["ma_sv"]
                            cum_gpa = ev["cum_gpa"]
                            profile = ev["profile"]
                            hi = ev["hk_idx"]

                            # Sinh DRL + học bổng + kỷ luật
                            drl_shift = int(hk_mod * 4)
                            drl_means = {"xuất sắc": 88, "giỏi": 82, "khá": 73, "yếu": 48, "trung bình": 62}
                            drl_stds  = {"xuất sắc": 4,  "giỏi": 5,  "khá": 6,  "yếu": 8,  "trung bình": 7}
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
                            })

                            # Tài chính
                            api_record = _gen_tai_chinh_record(
                                ma_sv, ma_hk, hk_obj, profile,
                                mon_by_hk[hk_idx], hk_mod
                            )

                            # ★ [DATE_FORMAT_MISMATCH] v2.2 — ~2.5% ngày dùng format sai
                            # Giải thích: vendor API Portal Tài chính đôi khi trả
                            # ngay_dong_cuoi theo format Việt Nam (dd/mm/yyyy) thay vì ISO
                            # ETL cần pd.to_datetime(infer_datetime_format=True) để handle
                            if (api_record.get("ngay_dong_cuoi")
                                    and random.random() < NATURAL_IMPERFECTION["json_date_format_rate"]):
                                api_record["ngay_dong_cuoi"] = _corrupt_date_format(
                                    api_record["ngay_dong_cuoi"]
                                )

                            all_api_records.append(api_record)

                            # ★ [DUPLICATE_RECORDS] v2.2 — ~0.8% JSON records bị duplicate
                            # Giải thích: API pagination bug — gọi page cuối bị trả 2 lần
                            # Hoặc ETL extract chạy lại do timeout, không có dedup
                            if random.random() < NATURAL_IMPERFECTION["json_duplicate_rate"]:
                                all_api_records.append(api_record.copy())

                            # Đánh giá trạng thái
                            if hi >= EVAL_CRITERIA["ap_dung_tu_hk"]:
                                if cum_gpa < EVAL_CRITERIA["buoc_thoi_hoc_gpa_hard"]:
                                    sv_active[ma_sv] = False
                                    sv_final_status[ma_sv] = "Thôi học"
                                elif sv_active.get(ma_sv, True) is not False:
                                    prob = 0.0
                                    if profile == "yếu" and cum_gpa < EVAL_CRITERIA["bao_luu_gpa_threshold"]:
                                        prob = EVAL_CRITERIA["bao_luu_prob_yeu"]
                                    elif profile == "trung bình" and cum_gpa < EVAL_CRITERIA["bao_luu_gpa_threshold"]:
                                        prob = EVAL_CRITERIA["bao_luu_prob_tb"]
                                    if prob > 0 and random.random() < prob:
                                        sv_active[ma_sv] = False
                                        sv_final_status[ma_sv] = "Bảo lưu"

                    print(f"    {cohort}/{ma_hk} (HK{hk_idx}): "
                          f"{len(hk_eval_data)} SV có điểm | "
                          f"{'[RECENT]' if is_recent else ''} "
                          f"Còn active: {sum(1 for sv in cohort_svs if sv_active[sv.ma_sinh_vien])}")

            # ── PASS 2: Học bổng ──
            total_hb_nganh = {"Xuất sắc": 0, "Giỏi": 0, "Khá": 0}
            for ma_hk, records in sorted(temp_hk_data.items()):
                by_cohort = {}
                for rec in records:
                    by_cohort.setdefault(rec["khoa_hoc"], []).append(rec)
                for cohort, cohort_records in sorted(by_cohort.items()):
                    cohort_records.sort(key=lambda x: (x["gpa_hk"], x["drl"]), reverse=True)
                    quota = max(1, int(len(cohort_records) * HB_QUOTA_RATE))
                    granted = 0
                    for rec in cohort_records:
                        loai_hb, muc_tien = "", 0
                        if granted < quota and not rec["hinh_thuc_kl"]:
                            for tier in HB_TIERS:
                                if rec["drl"] >= tier["min_drl"] and rec["gpa_hk"] >= tier["min_gpa"]:
                                    loai_hb = tier["loai"]
                                    muc_tien = tier["muc_tien"]
                                    granted += 1
                                    break

                        # ★ [CSV_NATURAL_NOISE] 4% thiếu diem_ren_luyen (chưa nộp)
                        # 2% trailing space trong mã SV
                        drl_val = rec["drl"]
                        xep_loai_val = rec["xep_loai_rl"]
                        ma_sv_csv = rec["ma_sinh_vien"]

                        if random.random() < NATURAL_IMPERFECTION["csv_missing_rl_rate"]:
                            # Điểm RL chưa được cập nhật vào hệ thống
                            drl_val = None
                            xep_loai_val = None

                        if random.random() < NATURAL_IMPERFECTION["csv_trailing_space_rate"]:
                            # Mã SV có trailing space do nhập tay từ Excel
                            ma_sv_csv = rec["ma_sinh_vien"] + " "

                        # ★ [ENCODING_ISSUES] v2.2 — Lỗi encoding tên (nếu có trường tên)
                        # Ở đây ta apply vào xep_loai_rl vì tên SV không có trong CSV CTSV
                        # Trong thực tế lỗi này xuất hiện ở cột tên trong file export Excel
                        # Ta flag bằng cách corrupt xep_loai_val để ETL phải handle
                        if (xep_loai_val is not None
                                and random.random() < NATURAL_IMPERFECTION["csv_encoding_error_rate"]):
                            xep_loai_val = _corrupt_encoding(xep_loai_val)

                        csv_rec = {
                            "ma_sinh_vien": ma_sv_csv,
                            "hoc_ky": rec["hoc_ky"],
                            "diem_ren_luyen": drl_val,
                            "xep_loai_rl": xep_loai_val,
                            "loai_hoc_bong": loai_hb,
                            "muc_tien_hb": muc_tien,
                            "hinh_thuc_ky_luat": rec["hinh_thuc_kl"],
                            "ly_do_ky_luat": rec["ly_do_kl"],
                        }
                        all_csv_records.append(csv_rec)

                        # ★ [DUPLICATE_RECORDS] v2.2 — ~1.2% dòng bị duplicate tự nhiên
                        # Giải thích: nhân viên phòng CTSV export CSV 2 lần (HK1 + update),
                        # sau đó merge thủ công bằng copy-paste → một số dòng bị nhân đôi
                        if random.random() < NATURAL_IMPERFECTION["csv_duplicate_rate"]:
                            all_csv_records.append(csv_rec.copy())

                        if loai_hb:
                            if "Xuất sắc" in loai_hb: total_hb_nganh["Xuất sắc"] += 1
                            elif "Giỏi" in loai_hb: total_hb_nganh["Giỏi"] += 1
                            else: total_hb_nganh["Khá"] += 1

            print(f"  -> Học bổng: XS={total_hb_nganh['Xuất sắc']} | "
                  f"Giỏi={total_hb_nganh['Giỏi']} | Khá={total_hb_nganh['Khá']}")

            # ── Cập nhật trạng thái SV ──
            status_counts = {}
            for sv in svs:
                if sv.ma_sinh_vien in sv_final_status:
                    sv.trang_thai_hoc_tap = sv_final_status[sv.ma_sinh_vien]
                st = sv.trang_thai_hoc_tap
                status_counts[st] = status_counts.get(st, 0) + 1
            s.flush()
            print(f"\n  -> Trạng thái: {' | '.join(f'{k}:{v}' for k, v in sorted(status_counts.items()))}")

            # ── Insert Đăng ký (bỏ qua môn đã rút khi insert điểm) ──
            dk_clean = []
            for d in dk_buf:
                row = {k: v for k, v in d.items() if not k.startswith("_")}
                dk_clean.append(row)

            inserted_dk = 0
            for i in range(0, len(dk_clean), 500):
                batch = dk_clean[i:i + 500]
                res = s.execute(text("""
                    INSERT INTO dang_ky_hoc_phan
                    (ma_sinh_vien, ma_hoc_phan, ma_hoc_ky,
                     ma_giang_vien, ngay_dang_ky, trang_thai)
                    VALUES
                    (:ma_sinh_vien, :ma_hoc_phan, :ma_hoc_ky,
                     :ma_giang_vien, :ngay_dang_ky, :trang_thai)
                    ON CONFLICT (ma_sinh_vien, ma_hoc_phan, ma_hoc_ky) DO NOTHING
                """), batch)
                inserted_dk += res.rowcount
            s.flush()
            total_dk_created += inserted_dk
            print(f"  -> {inserted_dk:,} Đăng ký HP (trong đó {total_withdrawals} đã rút)")

            # ── Load IDs ──
            like_pats = " OR ".join([f"ma_sinh_vien LIKE '{c}DC{vt}%'" for c in COHORTS])
            all_dk = s.execute(text(f"""
                SELECT ma_dang_ky, ma_sinh_vien, ma_hoc_phan, ma_hoc_ky
                FROM dang_ky_hoc_phan WHERE {like_pats}
            """)).fetchall()
            dk_to_id = {(r.ma_sinh_vien, r.ma_hoc_phan, r.ma_hoc_ky): r.ma_dang_ky for r in all_dk}

            # ── Insert Điểm (bỏ qua môn đã rút) ──
            diem_buf = []
            for d in dk_buf:
                if d.get("_withdrawn") or d.get("_pre_grade") is None:
                    continue   # môn đã rút → không có điểm

                key = (d["ma_sinh_vien"], d["ma_hoc_phan"], d["ma_hoc_ky"])
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
            print(f"  -> {inserted_diem:,} Điểm HP (trong đó {total_missing_grades} chưa nhập)")

        # ── 11. GPA Tổng hợp ──
        print(f"\n[11] Đang tính toán GPA tổng hợp...")
        rows_gpa = s.execute(text("""
            WITH best_diem AS (
                SELECT dk.ma_sinh_vien, dk.ma_hoc_phan, hp.so_tin_chi,
                       MAX(d.diem_he_4) AS best_he4,
                       BOOL_OR(d.dat_mon) AS dat_mon
                FROM diem_hoc_phan d
                JOIN dang_ky_hoc_phan dk ON d.ma_dang_ky = dk.ma_dang_ky
                JOIN hoc_phan hp ON dk.ma_hoc_phan = hp.ma_hoc_phan
                WHERE d.diem_he_4 IS NOT NULL
                GROUP BY dk.ma_sinh_vien, dk.ma_hoc_phan, hp.so_tin_chi
            )
            SELECT ma_sinh_vien,
                   SUM(best_he4 * so_tin_chi) AS tong_cl,
                   SUM(so_tin_chi) AS tc_da_hoc,
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
                "ma_sinh_vien": r.ma_sinh_vien,
                "tong_tin_chi": int(tong_tc_ct),
                "tin_chi_tich_luy": int(r.tc_dat),
                "gpa_he_10": round(min(gpa4 * 2.5, 10.0), 2),
                "gpa_he_4": gpa4,
                "canh_bao_hoc_vu": gpa4 < 2.0,
            })

        s.bulk_insert_mappings(TongHopKetQua, th_buf)
        s.commit()
        print(f"  -> Đã lưu GPA cho {len(th_buf)} SV")

    # ── 12. Xuất File ──
    print(f"\n[12] Đang xuất files CSV & JSON...")

    # ─────────────────────────────────────────────────────────────────
    # ★ [ORPHAN_RECORDS] v2.2 — Inject ghost records vào CSV
    #
    # Giải thích quy trình:
    # 1. Chọn ngẫu nhiên ~30% các HK trong dataset để có orphan record
    # 2. Mỗi HK được chọn sẽ nhận 1-2 ghost record
    # 3. Ghost records được thêm vào cuối list → ETL sẽ không tìm được
    #    sinh_vien_key khi lookup → bị log là "orphan" trong load step
    #
    # Giá trị demo: ETL pipeline phải xử lý gracefully thay vì crash
    # ─────────────────────────────────────────────────────────────────
    all_hk_in_data = list({rec["hoc_ky"] for rec in all_csv_records})
    # Chọn ngẫu nhiên ~30% HK để inject orphan
    hks_with_orphan_csv = random.sample(
        all_hk_in_data,
        k=max(1, int(len(all_hk_in_data) * 0.30))
    )
    total_orphan_csv = 0
    for ma_hk in hks_with_orphan_csv:
        n_orphan = random.randint(1, 2)
        ghost_pool = random.sample(_GHOST_IDS_CSV, min(n_orphan, len(_GHOST_IDS_CSV)))
        for ghost_id in ghost_pool:
            all_csv_records.append(_build_ghost_csv_record(ma_hk, ghost_id))
            total_orphan_csv += 1

    # ── Xuất CSV ──
    hk_groups_csv = {}
    for rec in all_csv_records:
        hk_groups_csv.setdefault(rec["hoc_ky"], []).append(rec)

    csv_fields = [
        "ma_sinh_vien", "hoc_ky", "diem_ren_luyen", "xep_loai_rl",
        "loai_hoc_bong", "muc_tien_hb", "hinh_thuc_ky_luat", "ly_do_ky_luat"
    ]
    csv_file_count = 0
    for hk, records in sorted(hk_groups_csv.items()):
        with open(f"{OUTPUT_DIR}/csv/ctsv_{hk.replace('-', '_')}.csv", "w",
                  newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=csv_fields)
            w.writeheader()
            w.writerows(records)
        csv_file_count += 1
    with open(f"{OUTPUT_DIR}/csv/ctsv_all.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=csv_fields)
        w.writeheader()
        w.writerows(all_csv_records)

    # ─────────────────────────────────────────────────────────────────
    # ★ [ORPHAN_RECORDS] v2.2 — Inject ghost records vào JSON
    # ─────────────────────────────────────────────────────────────────
    all_hk_in_json = list({rec["hoc_ky"] for rec in all_api_records})
    hks_with_orphan_json = random.sample(
        all_hk_in_json,
        k=max(1, int(len(all_hk_in_json) * 0.25))
    )
    # Cần hk_by_key để lấy ngay_bat_dau cho ghost record
    total_orphan_json = 0
    for ma_hk in hks_with_orphan_json:
        hk_obj_ghost = hk_by_key.get(ma_hk)
        if hk_obj_ghost is None:
            continue
        ngay_bat_dau_ghost = hk_ngay_bat_dau_map.get(ma_hk)
        if ngay_bat_dau_ghost is None:
            continue
        n_orphan = random.randint(1, 2)
        ghost_pool = random.sample(_GHOST_IDS_JSON, min(n_orphan, len(_GHOST_IDS_JSON)))
        for ghost_id in ghost_pool:
            ghost_rec = _build_ghost_json_record(ma_hk, ghost_id, ngay_bat_dau_ghost)
            # Ghost JSON record cũng có thể bị date format mismatch
            if random.random() < 0.3:
                ghost_rec["ngay_dong_cuoi"] = _corrupt_date_format(
                    ghost_rec["ngay_dong_cuoi"]
                )
            all_api_records.append(ghost_rec)
            total_orphan_json += 1

    # ─────────────────────────────────────────────────────────────────
    # ★ [JSON_METADATA] v2.2 — Wrap JSON với metadata header
    #
    # Tại sao quan trọng:
    # - API response thực tế LUÔN có metadata (pagination, timestamp, version)
    # - ETL extract phải xử lý wrapper: lấy data["data"] thay vì dùng toàn bộ response
    # - Giúp kiểm tra data freshness: nếu generated_at quá cũ → alert
    # - source_system và schema_version giúp ETL biết cần dùng parser nào
    #
    # Cấu trúc:
    # {
    #   "metadata": { "generated_at": ..., "version": ..., "source_system": ... },
    #   "data": [ ... records ... ]
    # }
    # ─────────────────────────────────────────────────────────────────
    _GEN_TIMESTAMP = datetime.now().isoformat(timespec="seconds")

    def _wrap_with_metadata(records: list, hoc_ky: str = "all") -> dict:
        """
        Bọc danh sách records trong envelope có metadata.

        Mô phỏng response thực tế từ Portal Tài chính PTIT:
        - generated_at: thời điểm API sinh response
        - schema_version: để ETL biết cấu trúc field (v1.x vs v2.x)
        - record_count: để ETL validate số lượng trước khi xử lý
        - source_system: để dim_date biết nguồn gốc data
        """
        return {
            "metadata": {
                "generated_at":  _GEN_TIMESTAMP,
                "schema_version": "2.1",
                "source_system":  "PTIT_PortalTaiChinh",
                "hoc_ky":         hoc_ky,
                "record_count":   len(records),
                "api_endpoint":   f"/api/tai-chinh/sinh-vien?hoc_ky={hoc_ky}",
            },
            "data": records,
        }

    # Xuất JSON từng HK với metadata wrapper
    hk_groups_api = {}
    for rec in all_api_records:
        hk_groups_api.setdefault(rec["hoc_ky"], []).append(rec)

    json_file_count = 0
    for hk, records in sorted(hk_groups_api.items()):
        payload = _wrap_with_metadata(records, hoc_ky=hk)
        with open(f"{OUTPUT_DIR}/api_json/taichinh_{hk.replace('-', '_')}.json", "w",
                  encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        json_file_count += 1

    # File "all" — tổng hợp toàn bộ
    all_payload = _wrap_with_metadata(all_api_records, hoc_ky="all")
    with open(f"{OUTPUT_DIR}/api_json/taichinh_all.json", "w", encoding="utf-8") as f:
        json.dump(all_payload, f, ensure_ascii=False, indent=2)

    # ─────────────────────────────────────────────────────────────────
    # Thống kê imperfection chi tiết để người dùng nắm rõ dataset
    # ─────────────────────────────────────────────────────────────────
    n_csv   = len(all_csv_records)
    n_json  = len(all_api_records)
    n_dup_csv  = int(n_csv  * NATURAL_IMPERFECTION["csv_duplicate_rate"])
    n_dup_json = int(n_json * NATURAL_IMPERFECTION["json_duplicate_rate"])
    n_enc  = int(n_csv  * NATURAL_IMPERFECTION["csv_encoding_error_rate"])
    n_date = int(n_json * NATURAL_IMPERFECTION["json_date_format_rate"])
    n_tt   = int((total_dk_created) * NATURAL_IMPERFECTION["trang_thai_variant_rate"])

    print()
    print("=" * 70)
    print(" HOÀN THÀNH — v2.2 Realistic Imperfections + Cross-Source Integrity")
    print("=" * 70)
    print(f" [PostgreSQL]")
    print(f"   Sinh viên              : {total_sv_created:,}")
    print(f"   Đăng ký HP             : {total_dk_created:,}")
    print(f"     └─ Đã rút môn        : {total_withdrawals:,}   ← [COURSE_WITHDRAWAL]")
    print(f"     └─ trang_thai cũ     : ~{n_tt:,}    ← [TRANG_THAI_VARIANTS]")
    print(f"   Điểm HP                : {total_diem_created:,}")
    print(f"     └─ Chưa nhập (NULL)  : {total_missing_grades:,}  ← [NATURAL_MISSING]")
    print(f"   GPA Tổng hợp           : {len(th_buf):,}")
    print(f"   Môn học                : {len(global_hp_inserted)}")
    print()
    print(f" [CSV — {csv_file_count} files + ctsv_all.csv]")
    print(f"   Tổng records           : {n_csv:,}")
    print(f"     └─ Thiếu điểm RL     : ~{int(n_csv * NATURAL_IMPERFECTION['csv_missing_rl_rate']):,}   ← [CSV_NATURAL_NOISE]")
    print(f"     └─ Trailing space    : ~{int(n_csv * NATURAL_IMPERFECTION['csv_trailing_space_rate']):,}   ← [CSV_NATURAL_NOISE]")
    print(f"     └─ Duplicate rows    : ~{n_dup_csv:,}   ← [DUPLICATE_RECORDS]")
    print(f"     └─ Encoding lỗi      : ~{n_enc:,}    ← [ENCODING_ISSUES]")
    print(f"     └─ Orphan SV         : {total_orphan_csv:,}    ← [ORPHAN_RECORDS] (không có trong PG)")
    print()
    print(f" [JSON — {json_file_count} files + taichinh_all.json]")
    print(f"   Tổng records           : {n_json:,}")
    print(f"     └─ Con_no sai số     : ~{int(n_json * NATURAL_IMPERFECTION['financial_rounding_rate']):,}   ← [FINANCIAL_ROUNDING]")
    print(f"     └─ Đóng 2 đợt       : ~{int(n_json * NATURAL_IMPERFECTION['payment_installment_rate'] * 0.4):,}  ← [PAYMENT_INSTALLMENT]")
    print(f"     └─ Ngày sai format   : ~{n_date:,}   ← [DATE_FORMAT_MISMATCH]")
    print(f"     └─ Duplicate records : ~{n_dup_json:,}   ← [DUPLICATE_RECORDS]")
    print(f"     └─ Orphan SV         : {total_orphan_json:,}    ← [ORPHAN_RECORDS] (không có trong PG)")
    print(f"   Metadata wrapper       : ✅  ← [JSON_METADATA] (generated_at, version...)")
    print()
    print(f" [RECENT_HKS] = {RECENT_HKS}  ← [DYNAMIC_RECENT_HKS]")
    print()
    print(" inject_errors.py vẫn có thể dùng để TĂNG CƯỜNG thêm lỗi.")
    print(" Nhưng dataset này đã đủ 'bẩn tự nhiên' cho ETL demo.")
    print("=" * 70)


if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════════════
# ★ v2.2 BREAKING CHANGE — Cần cập nhật extract.py
# ═══════════════════════════════════════════════════════════════════════
# File JSON giờ có cấu trúc:
#   { "metadata": {...}, "data": [...records...] }
# thay vì list phẳng như v2.0/v2.1.
#
# Trong src/etl/extract.py, method _try_json_file() cần sửa:
#
#   TRƯỚC (v2.0/v2.1):
#       records = json_lib.load(f)        # list trực tiếp
#       df = pd.DataFrame(records)
#
#   SAU (v2.2):
#       payload = json_lib.load(f)
#       if isinstance(payload, dict) and "data" in payload:
#           records = payload["data"]     # unwrap envelope
#           metadata = payload.get("metadata", {})
#           logger.info(f"  API | metadata: {metadata.get('generated_at')}")
#       else:
#           records = payload             # backward compat với file cũ
#       df = pd.DataFrame(records)
#
# Tương tự trong mock_api_server.py, endpoint /api/tai-chinh/sinh-vien
# cần trả về {"metadata": {...}, "data": [...]} thay vì {"data": [...]}.
# ═══════════════════════════════════════════════════════════════════════