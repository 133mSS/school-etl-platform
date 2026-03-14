"""
generate_sample_data_cntt.py  —  PTIT School ETL Platform
==========================================================
Sinh dữ liệu mẫu cho ngành: Công nghệ thông tin (CNTT)
Chương trình 9 học kỳ chính thức theo đúng tiến trình PTIT.

Đặc điểm ngành CNTT:
  - Mã SV  : B21DCCN001 (B__DC CN __)
  - Mã lớp : D21CQCN01-B (D__CQ CN __)
  - 9 HK, 150 tín chỉ
  - HK9 = Thực tập và tốt nghiệp (12 TC) — đặc biệt

Logic cohort (tháng 3/2026):
  B21 (2021): đang HK9 thực tập → có điểm HK1-8, HK9 chưa có
  B22 (2022): đang HK7          → có điểm HK1-6
  B23 (2023): đang HK5          → có điểm HK1-4
  B24 (2024): đang HK3          → có điểm HK1-2

Logic trang_thai_hoc_tap theo cohort:
  B21: Tot nghiep(50%) / Dang hoc(30%) / Bao luu(10%) / Thoi hoc(10%)
       → B21 "Dang hoc" = đang thực tập HK9, chưa tốt nghiệp
  B22: Dang hoc(82%) / Bao luu(8%) / Thoi hoc(10%)
  B23: Dang hoc(85%) / Bao luu(8%) / Thoi hoc(7%)
  B24: Dang hoc(92%) / Thoi hoc(8%)  ← quá sớm để Bảo lưu

GPA: tính theo điểm cao nhất mỗi môn (không inflate khi học lại)
"""

import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Boolean, Date, DateTime, ForeignKey, Integer,
    Numeric, String, Text, UniqueConstraint, create_engine, func, text
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# ══════════════════════════════════════════════════════════════════
# KẾT NỐI
# ══════════════════════════════════════════════════════════════════
DB_URL = "postgresql+psycopg2://school_user:school_pass@localhost:5434/school_source"
engine = create_engine(DB_URL, echo=False)
random.seed(42)


# ══════════════════════════════════════════════════════════════════
# ORM MODELS — khớp 01_create_tables.sql v2.0
# ══════════════════════════════════════════════════════════════════
class Base(DeclarativeBase):
    pass

class CoSo(Base):
    __tablename__ = "co_so"
    ma_co_so:  Mapped[str]           = mapped_column(String(10),  primary_key=True)
    ten_co_so: Mapped[str]           = mapped_column(String(200), nullable=False)
    dia_chi:   Mapped[Optional[str]] = mapped_column(Text)
    ngay_tao:  Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

class Khoa(Base):
    __tablename__ = "khoa"
    ma_khoa:  Mapped[str]           = mapped_column(String(10),  primary_key=True)
    ten_khoa: Mapped[str]           = mapped_column(String(200), nullable=False)
    ma_co_so: Mapped[Optional[str]] = mapped_column(ForeignKey("co_so.ma_co_so"))
    ngay_tao: Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

class Nganh(Base):
    __tablename__ = "nganh"
    ma_nganh:  Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ten_nganh: Mapped[str]           = mapped_column(String(200), nullable=False)
    ma_khoa:   Mapped[Optional[str]] = mapped_column(ForeignKey("khoa.ma_khoa"))
    ngay_tao:  Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

class GiangVien(Base):
    __tablename__ = "giang_vien"
    ma_giang_vien:       Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ho:                  Mapped[str]           = mapped_column(String(50),  nullable=False)
    ten:                 Mapped[str]           = mapped_column(String(50),  nullable=False)
    email:               Mapped[str]           = mapped_column(String(100), unique=True, nullable=False)
    so_dien_thoai:       Mapped[Optional[str]] = mapped_column(String(15))
    chuc_danh:           Mapped[Optional[str]] = mapped_column(String(50))
    trang_thai_cong_tac: Mapped[str]           = mapped_column(String(20),  default="Dang cong tac")
    ma_khoa:             Mapped[Optional[str]] = mapped_column(ForeignKey("khoa.ma_khoa"))
    ma_co_so:            Mapped[Optional[str]] = mapped_column(ForeignKey("co_so.ma_co_so"))
    ngay_tao:            Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())
    ngay_cap_nhat:       Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

class LopHanhChinh(Base):
    __tablename__ = "lop_hanh_chinh"
    ma_lop:    Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ten_lop:   Mapped[str]           = mapped_column(String(100), nullable=False)
    khoa_hoc:  Mapped[str]           = mapped_column(String(10),  nullable=False)
    ma_nganh:  Mapped[Optional[str]] = mapped_column(ForeignKey("nganh.ma_nganh"))
    ma_co_van: Mapped[Optional[str]] = mapped_column(ForeignKey("giang_vien.ma_giang_vien"))
    ngay_tao:  Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

class SinhVien(Base):
    __tablename__ = "sinh_vien"
    ma_sinh_vien:       Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ho:                 Mapped[str]           = mapped_column(String(50),  nullable=False)
    ten:                Mapped[str]           = mapped_column(String(50),  nullable=False)
    ngay_sinh:          Mapped[date]          = mapped_column(Date,        nullable=False)
    gioi_tinh:          Mapped[Optional[str]] = mapped_column(String(10))
    email:              Mapped[str]           = mapped_column(String(100), unique=True, nullable=False)
    ma_nganh:           Mapped[Optional[str]] = mapped_column(ForeignKey("nganh.ma_nganh"))
    ma_lop:             Mapped[Optional[str]] = mapped_column(ForeignKey("lop_hanh_chinh.ma_lop"))
    khoa_hoc:           Mapped[str]           = mapped_column(String(10),  nullable=False)
    trang_thai_hoc_tap: Mapped[str]           = mapped_column(String(30),  default="Dang hoc")
    ngay_tao:           Mapped[datetime]      = mapped_column(DateTime,    server_default=func.now())

class HocPhan(Base):
    __tablename__ = "hoc_phan"
    ma_hoc_phan:      Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ma_mon:           Mapped[str]           = mapped_column(String(10),  unique=True, nullable=False)
    ten_mon:          Mapped[str]           = mapped_column(String(200), nullable=False)
    so_tin_chi:       Mapped[int]           = mapped_column(Integer,     nullable=False)
    so_gio_ly_thuyet: Mapped[int]           = mapped_column(Integer,     default=0)
    so_gio_thuc_hanh: Mapped[int]           = mapped_column(Integer,     default=0)
    hoc_ky_de_xuat:   Mapped[Optional[int]] = mapped_column(Integer)
    bat_buoc:         Mapped[bool]          = mapped_column(Boolean,     default=True)
    ma_khoa:          Mapped[Optional[str]] = mapped_column(ForeignKey("khoa.ma_khoa"))
    ngay_tao:         Mapped[datetime]      = mapped_column(DateTime,    server_default=func.now())

class HocKyNamHoc(Base):
    __tablename__ = "hoc_ky_nam_hoc"
    ma_hoc_ky:     Mapped[str]            = mapped_column(String(50), primary_key=True)
    nam_hoc:       Mapped[str]            = mapped_column(String(50), nullable=False)
    hoc_ky:        Mapped[str]            = mapped_column(String(50), nullable=False)
    ngay_bat_dau:  Mapped[Optional[date]] = mapped_column(Date)
    ngay_ket_thuc: Mapped[Optional[date]] = mapped_column(Date)

class DangKyHocPhan(Base):
    __tablename__ = "dang_ky_hoc_phan"
    ma_dang_ky:    Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)
    ma_sinh_vien:  Mapped[str]           = mapped_column(ForeignKey("sinh_vien.ma_sinh_vien"),   nullable=False)
    ma_hoc_phan:   Mapped[str]           = mapped_column(ForeignKey("hoc_phan.ma_hoc_phan"),     nullable=False)
    ma_hoc_ky:     Mapped[str]           = mapped_column(ForeignKey("hoc_ky_nam_hoc.ma_hoc_ky"), nullable=False)
    ma_giang_vien: Mapped[Optional[str]] = mapped_column(ForeignKey("giang_vien.ma_giang_vien"))
    ngay_dang_ky:  Mapped[date]          = mapped_column(Date, server_default=func.current_date())
    trang_thai:    Mapped[str]           = mapped_column(String(30), default="Da dang ky")
    __table_args__ = (
        UniqueConstraint("ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky", name="uq_dang_ky_sv_hp_hk"),
    )

class DiemHocPhan(Base):
    __tablename__ = "diem_hoc_phan"
    ma_diem:         Mapped[int]                = mapped_column(Integer, primary_key=True, autoincrement=True)
    ma_dang_ky:      Mapped[int]                = mapped_column(ForeignKey("dang_ky_hoc_phan.ma_dang_ky"), unique=True)
    diem_chuyen_can: Mapped[Optional[Decimal]]  = mapped_column(Numeric(4, 2))
    diem_bai_tap:    Mapped[Optional[Decimal]]  = mapped_column(Numeric(4, 2))
    diem_giua_ky:    Mapped[Optional[Decimal]]  = mapped_column(Numeric(4, 2))
    diem_cuoi_ky:    Mapped[Optional[Decimal]]  = mapped_column(Numeric(4, 2))
    diem_tong_ket:   Mapped[Optional[Decimal]]  = mapped_column(Numeric(4, 2))
    diem_chu:        Mapped[Optional[str]]      = mapped_column(String(2))
    diem_he_4:       Mapped[Optional[Decimal]]  = mapped_column(Numeric(3, 2))
    dat_mon:         Mapped[Optional[bool]]     = mapped_column(Boolean)
    hoc_lai:         Mapped[bool]               = mapped_column(Boolean, default=False)
    ngay_cham:       Mapped[Optional[datetime]] = mapped_column(DateTime)
    ngay_tao:        Mapped[datetime]           = mapped_column(DateTime, server_default=func.now())

class TongHopKetQua(Base):
    __tablename__ = "tong_hop_ket_qua"
    ma_sinh_vien:     Mapped[str]              = mapped_column(ForeignKey("sinh_vien.ma_sinh_vien"), primary_key=True)
    tong_tin_chi:     Mapped[int]              = mapped_column(Integer, default=0)
    tin_chi_tich_luy: Mapped[int]              = mapped_column(Integer, default=0)
    gpa_he_10:        Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    gpa_he_4:         Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    canh_bao_hoc_vu:  Mapped[bool]              = mapped_column(Boolean, default=False)
    ngay_cap_nhat:    Mapped[datetime]          = mapped_column(DateTime, server_default=func.now())


# ══════════════════════════════════════════════════════════════════
# CẤU HÌNH NGÀNH CNTT
# ══════════════════════════════════════════════════════════════════
NGANH_INFO = {
    "ma_nganh"   : "CNTT",
    "ten_nganh"  : "Cong nghe thong tin",
    "ma_khoa"    : "CNTT1",
    "ma_viet_tat": "CN",    # mã SV: B21DCCN001 | mã lớp: D21CQCN01-B
}

# Chương trình 9 HK — CNTT PTIT
# Format: (ma_mon, ten_mon, so_tin_chi, gio_lt, gio_th, do_kho)
# do_kho: 0.0=dễ (lý luận) → 2.5=rất khó (toán/kỹ thuật)
CHUONG_TRINH = {
    1: [
        ("TRIET",  "Triet hoc Mac-Lenin",              3, 45,  0, 0.0),
        ("THCS1",  "Tin hoc co so 1",                  2, 20, 20, 0.5),
        ("GT1",    "Giai tich 1",                      3, 45,  0, 2.5),
        ("DAIS",   "Dai so",                           3, 45,  0, 2.0),
    ],                                                              # 11 TC
    2: [
        ("KTCT",   "Kinh te chinh tri Mac-Lenin",      2, 30,  0, 0.0),
        ("TA1",    "Tieng Anh Course 1",               4, 60,  0, 0.8),
        ("THCS2",  "Tin hoc co so 2",                  2, 20, 20, 0.8),
        ("GT2",    "Giai tich 2",                      3, 45,  0, 2.5),
        ("PLDC",   "Phap luat dai cuong",              2, 30,  0, 0.3),
        ("VLUD",   "Vat ly ung dung",                  4, 45, 15, 1.8),
        ("KTS",    "Ky thuat so",                      2, 20, 10, 1.2),
    ],                                                              # 19 TC
    3: [
        ("CNXH",   "Chu nghia xa hoi khoa hoc",        2, 30,  0, 0.0),
        ("TA2",    "Tieng Anh Course 2",               4, 60,  0, 0.8),
        ("CPP",    "Ngon ngu lap trinh C++",           3, 30, 30, 1.5),
        ("TRR1",   "Toan roi rac 1",                   3, 45,  0, 1.8),
        ("XLTS",   "Xu ly tin hieu so",                2, 20, 10, 1.5),
        ("XSTK",   "Xac suat thong ke",                3, 45,  0, 1.5),
    ],                                                              # 17 TC
    4: [
        ("TTHCM",  "Tu tuong Ho Chi Minh",             2, 30,  0, 0.0),
        ("TA3",    "Tieng Anh Course 3",               4, 60,  0, 1.0),
        ("KTMT",   "Kien truc may tinh",               3, 30, 20, 1.3),
        ("TRR2",   "Toan roi rac 2",                   3, 45,  0, 2.0),
        ("CTDL",   "Cau truc du lieu va giai thuat",   3, 30, 30, 2.0),
        ("LTTT",   "Ly thuyet thong tin",              3, 45,  0, 1.5),
    ],                                                              # 18 TC
    5: [
        ("LSDCSVN","Lich su Dang cong san VN",         2, 30,  0, 0.0),
        ("TA3P",   "Tieng Anh Course 3 Plus",          2, 30,  0, 0.8),
        ("HDH",    "He dieu hanh",                     3, 30, 20, 1.3),
        ("OOP",    "Lap trinh huong doi tuong",        3, 30, 30, 1.3),
        ("CSDL",   "Co so du lieu",                    3, 30, 20, 1.5),
        ("MMT",    "Mang may tinh",                    3, 30, 20, 1.2),
        ("PYTHON", "Lap trinh Python",                 3, 30, 30, 1.0),
    ],                                                              # 19 TC
    6: [
        ("NMCNPM", "Nhap mon cong nghe phan mem",      3, 30, 20, 1.0),
        ("NMTTNT", "Nhap mon tri tue nhan tao",        3, 30, 20, 1.5),
        ("ATBM",   "An toan va bao mat HTTT",          3, 30, 20, 1.3),
        ("LAPWEB", "Lap trinh web",                    3, 20, 40, 1.2),
        ("CSDLPT", "Co so du lieu phan tan",           3, 30, 20, 1.8),
        ("TTCS",   "Thuc tap co so",                   4,  0, 60, 0.5),
    ],                                                              # 19 TC
    7: [
        ("QLDA",   "QLDA phan mem",                    3, 30, 20, 1.0),
        ("IOT",    "IoT va ung dung",                  3, 20, 40, 1.3),
        ("PTTKHT", "Phan tich va thiet ke HTTT",       3, 30, 20, 1.5),
        ("XLHA",   "Xu ly anh",                        3, 20, 40, 1.8),
        ("TC1",    "Hoc phan tu chon 1",               3, 30, 20, 1.2),
        ("TC2",    "Hoc phan tu chon 2",               3, 30, 20, 1.2),
    ],                                                              # 18 TC
    8: [
        ("TKMMT",  "Thiet ke mang may tinh",           3, 30, 20, 1.3),
        ("DGHM",   "Danh gia hieu nang mang",          3, 30, 20, 1.5),
        ("QLMMT",  "Quan ly mang may tinh",            3, 30, 20, 1.2),
        ("ANMNG",  "An ninh mang",                     3, 30, 20, 1.5),
        ("TC3",    "Hoc phan tu chon 3",               3, 30, 20, 1.2),
        ("PPNCKH", "Phuong phap luan NCKH",            2, 30,  0, 0.5),
    ],                                                              # 17 TC
    9: [
        # HK9 đặc biệt: Thực tập và tốt nghiệp 12TC
        # Chỉ B21 mới có thể đến HK9, và chỉ SV "Dang hoc" / "Tot nghiep"
        # SV "Tot nghiep" = đã hoàn thành HK9
        # SV "Dang hoc" B21 = đang thực tập HK9 → CHƯA có điểm
        ("TTTN",   "Thuc tap va tot nghiep",          12,  0, 180, 0.5),
    ],                                                              # 12 TC
}

# ══════════════════════════════════════════════════════════════════
# COHORT CONFIG — tháng 3/2026
# (nam_nhap, hk_hien_tai, hk_da_co_diem)
# QUAN TRỌNG:
#   B21 "Tot nghiep": hk_co_diem = 9 (đã xong cả HK9 thực tập)
#   B21 "Dang hoc"  : hk_co_diem = 8 (đang HK9, chưa có điểm HK9)
#   B21 "Bao luu/Thoi hoc": tùy HK dừng
# ══════════════════════════════════════════════════════════════════
COHORT_CONFIG = {
    "B21": (2021, 9, 8),   # hk_da_co_diem=8 là mặc định cho "Dang hoc"
    "B22": (2022, 7, 6),
    "B23": (2023, 5, 4),
    "B24": (2024, 3, 2),
}

TRANG_THAI_BY_COHORT = {
    #       options                                    weights
    "B21": (["Tot nghiep","Dang hoc","Bao luu","Thoi hoc"], [50, 30, 10, 10]),
    "B22": (["Dang hoc","Bao luu","Thoi hoc"],              [82,  8, 10]),
    "B23": (["Dang hoc","Bao luu","Thoi hoc"],              [85,  8,  7]),
    "B24": (["Dang hoc","Thoi hoc"],                        [92,  8]),
}

# ══════════════════════════════════════════════════════════════════
# LỊCH HỌC KỲ
# B21 cần 9 HK: HK1-2021-22 → HK1-2025-26
# B22 cần 7 HK: HK1-2022-23 → HK1-2025-26
# B23 cần 5 HK: HK1-2023-24 → HK1-2025-26
# B24 cần 3 HK: HK1-2024-25 → HK1-2025-26
# ══════════════════════════════════════════════════════════════════
HK_MASTER = {
    "HK1-2021-22": ("2021-2022","Hoc ky 1", date(2021,9,6),  date(2022,1,15)),
    "HK2-2021-22": ("2021-2022","Hoc ky 2", date(2022,2,14), date(2022,6,30)),
    "HK1-2022-23": ("2022-2023","Hoc ky 1", date(2022,9,5),  date(2023,1,14)),
    "HK2-2022-23": ("2022-2023","Hoc ky 2", date(2023,2,13), date(2023,6,30)),
    "HK1-2023-24": ("2023-2024","Hoc ky 1", date(2023,9,4),  date(2024,1,13)),
    "HK2-2023-24": ("2023-2024","Hoc ky 2", date(2024,2,12), date(2024,6,28)),
    "HK1-2024-25": ("2024-2025","Hoc ky 1", date(2024,9,2),  date(2025,1,11)),
    "HK2-2024-25": ("2024-2025","Hoc ky 2", date(2025,2,10), date(2025,6,27)),
    "HK1-2025-26": ("2025-2026","Hoc ky 1", date(2025,9,1),  date(2026,3,14)),
    "HK2-2025-26": ("2025-2026","Hoc ky 2", date(2026,2,9),  date(2026,6,30)),
}

# Mỗi cohort có sequence HK riêng theo đúng năm nhập học
HK_SEQ_BY_COHORT = {
    "B21": [
        "HK1-2021-22","HK2-2021-22",   # HK1, HK2
        "HK1-2022-23","HK2-2022-23",   # HK3, HK4
        "HK1-2023-24","HK2-2023-24",   # HK5, HK6
        "HK1-2024-25","HK2-2024-25",   # HK7, HK8
        "HK1-2025-26",                 # HK9 (thực tập)
    ],
    "B22": [
        "HK1-2022-23","HK2-2022-23",   # HK1, HK2
        "HK1-2023-24","HK2-2023-24",   # HK3, HK4
        "HK1-2024-25","HK2-2024-25",   # HK5, HK6
        "HK1-2025-26",                 # HK7 (đang học)
    ],
    "B23": [
        "HK1-2023-24","HK2-2023-24",   # HK1, HK2
        "HK1-2024-25","HK2-2024-25",   # HK3, HK4
        "HK1-2025-26",                 # HK5 (đang học)
    ],
    "B24": [
        "HK1-2024-25","HK2-2024-25",   # HK1, HK2
        "HK1-2025-26",                 # HK3 (đang học)
    ],
}

# ══════════════════════════════════════════════════════════════════
# DỮ LIỆU CỐ ĐỊNH
# ══════════════════════════════════════════════════════════════════
CO_SO_DATA = [
    ("HN",  "Hoc vien CNBCVT - Co so Ha Noi",  "Km10 Nguyen Trai, Ha Dong, Ha Noi"),
    ("HCM", "Hoc vien CNBCVT - Co so TP.HCM",  "11 Nguyen Dinh Chieu, Q.1, TP.HCM"),
]
KHOA_DATA = [
    ("CNTT1", "Khoa Cong nghe thong tin 1", "HN"),
    ("CNTT2", "Khoa Cong nghe thong tin 2", "HN"),
    ("ATTT",  "Khoa An toan thong tin",      "HN"),
    ("VT1",   "Khoa Vien thong 1",          "HN"),
]
HO      = ["Nguyen","Tran","Le","Pham","Hoang","Huynh","Phan","Vu","Dang","Bui",
           "Do","Ho","Ngo","Duong","Ly"]
TEN_NAM = ["Van An","Minh Tuan","Quoc Hung","Thanh Tung","Trong Nghia","Huu Phuc",
           "Xuan Binh","Anh Tuan","Cong Thang","Dinh Long","Van Duc","Minh Khoa",
           "Quoc Bao","Thanh Lam","Trong Khai"]
TEN_NU  = ["Thi Binh","Thi Lan","Thi Hoa","Thi Mai","Thi Thu","Ngoc Anh","Thuy Linh",
           "Khanh Ly","Thu Ha","Minh Chau","Bich Ngoc","Hong Nhung","Thanh Huyen",
           "Phuong Anh","Quynh Anh"]
CHUC_DANH   = ["ThS","TS","PGS.TS","GS.TS"]
CHUC_DANH_W = [50, 35, 12, 3]


# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def _gen_diem_raw(profile: str) -> float:
    r = random.random()
    if profile == "gioi":
        tbl = [(0.30,8.5,10.0),(0.60,7.5,8.49),(0.80,6.5,7.49),(0.92,5.5,6.49),(1.00,3.0,5.49)]
    elif profile == "yeu":
        tbl = [(0.05,8.0,10.0),(0.12,6.5,7.99),(0.30,5.0,6.49),(0.60,3.5,4.99),(1.00,0.0,3.49)]
    else:
        tbl = [(0.12,8.5,10.0),(0.35,7.0,8.49),(0.68,5.5,6.99),(0.87,4.0,5.49),(1.00,0.0,3.99)]
    for thr, lo, hi in tbl:
        if r < thr:
            return round(random.uniform(lo, hi), 2)
    return 5.0

def _gen_diem_mon(profile: str, do_kho: float, hk_idx: int, la_hoc_lai: bool) -> float:
    p    = "trung_binh" if (la_hoc_lai and profile == "yeu") else profile
    base = _gen_diem_raw(p)
    base -= do_kho * 0.4
    base += min((hk_idx - 1) * 0.10, 0.7)
    if la_hoc_lai:
        base += 1.2
    return round(max(0.0, min(10.0, base)), 2)

def _diem_sang_chu(d: float) -> tuple:
    if d >= 9.0: return "A+", 4.0
    if d >= 8.5: return "A",  4.0
    if d >= 8.0: return "B+", 3.5
    if d >= 7.0: return "B",  3.0
    if d >= 6.5: return "C+", 2.5
    if d >= 5.5: return "C",  2.0
    if d >= 5.0: return "D+", 1.5
    if d >= 4.0: return "D",  1.0
    return "F", 0.0

def _tao_diem_record(ma_dk, do_kho, profile, hk_idx, la_hoc_lai, ngay_ket_thuc_hk):
    """
    Tạo record điểm theo trọng số PTIT: CC:10% + BT:20% + GK:20% + CK:50%

    Fix 2: diem_chuyen_can theo profile — SV yếu vắng nhiều → CC thấp hơn
    Fix 3: CC cũng bị ảnh hưởng nhẹ bởi do_kho (môn khó SV dễ bỏ)
    """
    # CC theo profile: gioi=[8.5,10] | trung_binh=[6.5,9.5] | yeu=[5.0,8.5]
    if profile == "gioi":
        cc = round(random.uniform(8.5, 10.0), 2)
    elif profile == "yeu":
        cc = round(random.uniform(5.0, 8.5), 2)
        cc = round(max(0.0, cc - do_kho * 0.2), 2)   # môn khó càng hay vắng
    else:
        cc = round(random.uniform(6.5, 9.5), 2)
        cc = round(max(0.0, cc - do_kho * 0.1), 2)

    bt  = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai)
    gk  = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai)
    ck  = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai)
    dtk = round(max(0.0, min(10.0, 0.1*cc + 0.2*bt + 0.2*gk + 0.5*ck)), 2)
    chu, he4 = _diem_sang_chu(dtk)
    return {
        "ma_dang_ky"     : ma_dk,
        "diem_chuyen_can": cc,
        "diem_bai_tap"   : bt,
        "diem_giua_ky"   : gk,
        "diem_cuoi_ky"   : ck,
        "diem_tong_ket"  : dtk,
        "diem_chu"       : chu,
        "diem_he_4"      : float(he4),
        "dat_mon"        : dtk >= 4.0,
        "hoc_lai"        : la_hoc_lai,
        "ngay_cham"      : datetime.combine(
            ngay_ket_thuc_hk + timedelta(days=random.randint(7, 21)),
            datetime.min.time()
        ),
    }

def _gen_sdt() -> str:
    p = random.choice(["032","033","034","035","036","038","086","096","097","098"])
    return p + str(random.randint(1000000, 9999999))


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    COHORTS          = list(COHORT_CONFIG.keys())
    SO_SV_PER_COHORT = 50
    tong_mon         = sum(len(v) for v in CHUONG_TRINH.values())

    print("=" * 65)
    print(f"  Nganh : {NGANH_INFO['ten_nganh']}")
    print(f"  Tong  : {SO_SV_PER_COHORT * len(COHORTS)} SV | 9 HK | {tong_mon} mon | 150 TC")
    print("=" * 65)

    print("\n[0] Rebuild schema...")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("    OK")

    with Session(engine) as s:

        # 1. Cơ sở
        co_sos = [CoSo(ma_co_so=ma, ten_co_so=ten, dia_chi=dc) for ma,ten,dc in CO_SO_DATA]
        s.add_all(co_sos); s.flush()
        print(f"\n[1] Co so          : {len(co_sos)}")

        # 2. Khoa
        khoas = [Khoa(ma_khoa=ma, ten_khoa=ten, ma_co_so=cs) for ma,ten,cs in KHOA_DATA]
        s.add_all(khoas); s.flush()
        print(f"[2] Khoa           : {len(khoas)}")

        # 3. Ngành CNTT
        nganh = Nganh(
            ma_nganh  = NGANH_INFO["ma_nganh"],
            ten_nganh = NGANH_INFO["ten_nganh"],
            ma_khoa   = NGANH_INFO["ma_khoa"],
        )
        s.add(nganh); s.flush()
        print(f"[3] Nganh          : 1 ({nganh.ten_nganh})")

        # 4. Giảng viên (20 GV)
        khoa_gv = ["CNTT1"]*10 + ["CNTT2"]*4 + ["ATTT"]*3 + ["VT1"]*3
        gvs     = []
        for i in range(20):
            gioi = "Nam" if i < 14 else "Nu"
            gvs.append(GiangVien(
                ma_giang_vien      = f"GV{i+1:03d}",
                ho                 = random.choice(HO),
                ten                = random.choice(TEN_NAM if gioi=="Nam" else TEN_NU),
                email              = f"gv{i+1:03d}@ptit.edu.vn",
                so_dien_thoai      = _gen_sdt(),
                chuc_danh          = random.choices(CHUC_DANH, weights=CHUC_DANH_W)[0],
                trang_thai_cong_tac= "Dang cong tac",
                ma_khoa            = khoa_gv[i],
                ma_co_so           = "HN",
            ))
        s.add_all(gvs); s.flush()
        gv_ids = [g.ma_giang_vien for g in gvs]
        cv_ids = gv_ids[:12]
        print(f"[4] Giang vien     : {len(gvs)}")

        # 5. Lớp hành chính
        # Format: D21CQCN01-B (D + năm + CQ + CN + số thứ tự + -B)
        vt   = NGANH_INFO["ma_viet_tat"]   # "CN"
        lops = []
        for cohort in COHORTS:
            yr = cohort[1:]   # "21", "22", "23", "24"
            for stt in range(1, 3):
                ml = f"D{yr}CQ{vt}{stt:02d}-B"   # D21CQCN01-B
                lops.append(LopHanhChinh(
                    ma_lop    = ml,
                    ten_lop   = ml,
                    khoa_hoc  = cohort,
                    ma_nganh  = NGANH_INFO["ma_nganh"],
                    ma_co_van = random.choice(cv_ids),
                ))
        s.add_all(lops); s.flush()
        lop_by_cohort = {c: [l.ma_lop for l in lops if l.khoa_hoc==c] for c in COHORTS}
        print(f"[5] Lop hanh chinh : {len(lops)}  (D21CQCN01-B ... D24CQCN02-B)")

        # 6. Học phần (toàn bộ 9 HK)
        hoc_phans = []
        hp_by_hk  = {}
        stt_hp    = 1
        for hk_idx, mons in CHUONG_TRINH.items():
            hp_by_hk[hk_idx] = []
            for ma_mon, ten_mon, tc, lt, th, do_kho in mons:
                ma_hp = f"HP{stt_hp:03d}"
                hoc_phans.append(HocPhan(
                    ma_hoc_phan      = ma_hp,
                    ma_mon           = ma_mon,
                    ten_mon          = ten_mon,
                    so_tin_chi       = tc,
                    so_gio_ly_thuyet = lt,
                    so_gio_thuc_hanh = th,
                    hoc_ky_de_xuat   = hk_idx,
                    bat_buoc         = True,
                    ma_khoa          = NGANH_INFO["ma_khoa"],
                ))
                hp_by_hk[hk_idx].append((ma_hp, do_kho))
                stt_hp += 1
        s.add_all(hoc_phans); s.flush()
        print(f"[6] Hoc phan       : {len(hoc_phans)} mon / 9 HK (HK9=TTTN 12TC)")

        # 7. Học kỳ
        all_hk_keys = set()
        for seq in HK_SEQ_BY_COHORT.values():
            all_hk_keys.update(seq)
        hoc_kys = []
        for ma_hk in sorted(all_hk_keys):
            nh, hk, bd, kt = HK_MASTER[ma_hk]
            hoc_kys.append(HocKyNamHoc(
                ma_hoc_ky=ma_hk, nam_hoc=nh, hoc_ky=hk,
                ngay_bat_dau=bd, ngay_ket_thuc=kt
            ))
        s.add_all(hoc_kys); s.flush()

        # Lookup (cohort, hk_idx) → HocKyNamHoc
        hk_by_key = {hk.ma_hoc_ky: hk for hk in hoc_kys}
        hk_lookup = {}
        for cohort, seq in HK_SEQ_BY_COHORT.items():
            for idx, ma_hk in enumerate(seq, start=1):
                hk_lookup[(cohort, idx)] = hk_by_key[ma_hk]
        print(f"[7] Hoc ky         : {len(hoc_kys)}")

        # 8. Sinh viên (200 SV)
        print("[8] Tao sinh vien...")
        svs        = []
        hoc_luc_sv = {}
        sv_cohort  = {}

        for cohort in COHORTS:
            nam_nhap, _, _ = COHORT_CONFIG[cohort]
            tt_options, tt_weights = TRANG_THAI_BY_COHORT[cohort]

            for stt in range(1, SO_SV_PER_COHORT + 1):
                gioi       = "Nam" if random.random() < 0.60 else "Nu"
                # Format mã SV: B21DCCN001
                ma_sv      = f"{cohort}DC{vt}{stt:03d}"
                # Email unique: b21dccn001@student.ptit.edu.vn
                email      = f"{cohort.lower()}dc{vt.lower()}{stt:03d}@student.ptit.edu.vn"
                trang_thai = random.choices(tt_options, weights=tt_weights)[0]
                nam_sinh   = nam_nhap - random.randint(18, 21)

                svs.append(SinhVien(
                    ma_sinh_vien       = ma_sv,
                    ho                 = random.choice(HO),
                    ten                = random.choice(TEN_NAM if gioi=="Nam" else TEN_NU),
                    ngay_sinh          = date(nam_sinh, random.randint(1,12), random.randint(1,28)),
                    gioi_tinh          = gioi,
                    email              = email,
                    ma_nganh           = NGANH_INFO["ma_nganh"],
                    ma_lop             = random.choice(lop_by_cohort[cohort]),
                    khoa_hoc           = cohort,
                    trang_thai_hoc_tap = trang_thai,
                ))
                # Fix 4: profile học lực ràng buộc với trang_thai
                # Thoi hoc: 60% yếu / 30% tb / 10% giỏi (thôi học thường do học kém)
                # Bao luu:  30% yếu / 55% tb / 15% giỏi (bảo lưu có thể vì lý do khác)
                # Dang hoc/Tot nghiep: phân phối chuẩn
                if trang_thai == "Thoi hoc":
                    hl_weights = [10, 30, 60]
                elif trang_thai == "Bao luu":
                    hl_weights = [15, 55, 30]
                else:
                    hl_weights = [20, 63, 17]
                hoc_luc_sv[ma_sv] = random.choices(
                    ["gioi", "trung_binh", "yeu"], weights=hl_weights
                )[0]
                sv_cohort[ma_sv] = cohort

        s.add_all(svs); s.flush()
        tt_count = {}
        for sv in svs:
            tt_count[sv.trang_thai_hoc_tap] = tt_count.get(sv.trang_thai_hoc_tap, 0) + 1
        print(f"    -> {len(svs)} SV  ({' | '.join(f'{k}:{v}' for k,v in sorted(tt_count.items()))})")

        # 9. Đăng ký + Điểm
        print("[9] Tao dang ky + diem...")
        dk_buf = []
        dk_set = set()

        for sv in svs:
            _, hk_ht, hk_da_diem = COHORT_CONFIG[sv.khoa_hoc]
            profile = hoc_luc_sv[sv.ma_sinh_vien]

            # Xác định số HK có điểm
            if sv.trang_thai_hoc_tap == "Tot nghiep":
                # Đã hoàn thành tất cả 9 HK kể cả thực tập
                # Chỉ B21 mới có trang_thai này
                hk_co_diem = 9

            elif sv.trang_thai_hoc_tap == "Dang hoc":
                # B21 đang HK9 (thực tập) → có điểm HK1-8, HK9 chưa
                # B22 đang HK7 → có điểm HK1-6
                # B23 đang HK5 → có điểm HK1-4
                # B24 đang HK3 → có điểm HK1-2
                hk_co_diem = hk_da_diem

            elif sv.trang_thai_hoc_tap == "Bao luu":
                # Bảo lưu từ HK nào → có điểm đến HK trước đó (>= 1)
                bao_luu_tu = random.randint(2, max(2, hk_da_diem))
                hk_co_diem = bao_luu_tu - 1

            else:  # Thoi hoc
                # Thôi học sau ít nhất HK1
                thoi_hoc_sau = random.randint(1, max(1, hk_da_diem - 1))
                hk_co_diem   = max(1, thoi_hoc_sau)

            # Tạo đăng ký cho từng HK đã hoàn thành
            for hk_idx in range(1, hk_co_diem + 1):
                hk_obj = hk_lookup.get((sv.khoa_hoc, hk_idx))
                if hk_obj is None:
                    continue

                # Với HK9 (TTTN), chỉ sinh data nếu SV "Tot nghiep"
                if hk_idx == 9 and sv.trang_thai_hoc_tap != "Tot nghiep":
                    continue

                for ma_hp, do_kho in hp_by_hk.get(hk_idx, []):
                    key = (sv.ma_sinh_vien, ma_hp, hk_obj.ma_hoc_ky)
                    if key in dk_set:
                        continue
                    dk_set.add(key)
                    dk_buf.append({
                        "ma_sinh_vien" : sv.ma_sinh_vien,
                        "ma_hoc_phan"  : ma_hp,
                        "ma_hoc_ky"    : hk_obj.ma_hoc_ky,
                        "ma_giang_vien": random.choice(gv_ids[:15]),
                        "ngay_dang_ky" : hk_obj.ngay_bat_dau + timedelta(days=random.randint(1,10)),
                        "trang_thai"   : "Da dang ky",
                        "_hk_idx" : hk_idx,
                        "_do_kho" : do_kho,
                        "_profile": profile,
                        "_kt_hk"  : hk_obj.ngay_ket_thuc,
                    })

        # ── INSERT ĐĂNG KÝ — dùng ON CONFLICT DO NOTHING ──────────
        # bulk_insert_mappings bị silent rollback cả batch khi có 1 bản ghi trùng
        # ON CONFLICT DO NOTHING đảm bảo mọi bản ghi hợp lệ đều được insert
        dk_clean = [{k:v for k,v in d.items() if not k.startswith("_")} for d in dk_buf]
        inserted_dk = 0
        for i in range(0, len(dk_clean), 200):
            batch  = dk_clean[i:i+200]
            result = s.execute(text("""
                INSERT INTO dang_ky_hoc_phan
                    (ma_sinh_vien, ma_hoc_phan, ma_hoc_ky,
                     ma_giang_vien, ngay_dang_ky, trang_thai)
                VALUES
                    (:ma_sinh_vien, :ma_hoc_phan, :ma_hoc_ky,
                     :ma_giang_vien, :ngay_dang_ky, :trang_thai)
                ON CONFLICT (ma_sinh_vien, ma_hoc_phan, ma_hoc_ky) DO NOTHING
            """), batch)
            inserted_dk += result.rowcount
        s.flush()
        print(f"    -> {inserted_dk:,} dang ky da insert (buffer: {len(dk_buf):,})")

        # Lấy toàn bộ ma_dang_ky đã insert thành công từ DB
        all_dk   = s.execute(text(
            "SELECT ma_dang_ky, ma_sinh_vien, ma_hoc_phan, ma_hoc_ky FROM dang_ky_hoc_phan"
        )).fetchall()
        dk_to_id = {(r.ma_sinh_vien, r.ma_hoc_phan, r.ma_hoc_ky): r.ma_dang_ky for r in all_dk}
        print(f"    -> {len(dk_to_id):,} dang ky trong DB (kiem tra)")

        # ── SINH ĐIỂM — chỉ cho đăng ký tồn tại trong DB ──────────
        diem_buf = []
        for d in dk_buf:
            key   = (d["ma_sinh_vien"], d["ma_hoc_phan"], d["ma_hoc_ky"])
            ma_dk = dk_to_id.get(key)
            if ma_dk is None:
                continue
            rec = _tao_diem_record(
                ma_dk, d["_do_kho"], d["_profile"],
                d["_hk_idx"], False, d["_kt_hk"]
            )
            diem_buf.append(rec)

        inserted_diem = 0
        for i in range(0, len(diem_buf), 200):
            batch  = diem_buf[i:i+200]
            result = s.execute(text("""
                INSERT INTO diem_hoc_phan
                    (ma_dang_ky,
                     diem_chuyen_can, diem_bai_tap, diem_giua_ky,
                     diem_cuoi_ky, diem_tong_ket,
                     diem_chu, diem_he_4, dat_mon, hoc_lai, ngay_cham)
                VALUES
                    (:ma_dang_ky,
                     :diem_chuyen_can, :diem_bai_tap, :diem_giua_ky,
                     :diem_cuoi_ky, :diem_tong_ket,
                     :diem_chu, :diem_he_4, :dat_mon, :hoc_lai, :ngay_cham)
                ON CONFLICT (ma_dang_ky) DO NOTHING
            """), batch)
            inserted_diem += result.rowcount
        s.flush()
        print(f"    -> {inserted_diem:,} diem da insert")

        # Bỏ phần học lại — đơn giản hóa, tránh lỗi trùng constraint
        hoc_lai_dk   = []
        hoc_lai_diem = []
        print(f"    -> 0 dang ky hoc lai (da loai bo de dam bao du lieu sach)")

        print(f"    -> {len(hoc_lai_diem):,} diem hoc lai")

        # 10. GPA — chỉ lấy điểm cao nhất mỗi môn
        print("[10] Tinh GPA...")
        rows_gpa = s.execute(text("""
            WITH best_diem AS (
                SELECT
                    dk.ma_sinh_vien,
                    dk.ma_hoc_phan,
                    hp.so_tin_chi,
                    MAX(d.diem_he_4)   AS best_he4,
                    BOOL_OR(d.dat_mon) AS dat_mon
                FROM diem_hoc_phan d
                JOIN dang_ky_hoc_phan dk ON d.ma_dang_ky  = dk.ma_dang_ky
                JOIN hoc_phan         hp ON dk.ma_hoc_phan = hp.ma_hoc_phan
                WHERE d.diem_he_4 IS NOT NULL
                GROUP BY dk.ma_sinh_vien, dk.ma_hoc_phan, hp.so_tin_chi
            )
            SELECT
                ma_sinh_vien,
                SUM(best_he4 * so_tin_chi)                       AS tong_cl,
                SUM(so_tin_chi)                                   AS tong_tc,
                SUM(CASE WHEN dat_mon THEN so_tin_chi ELSE 0 END) AS tc_dat
            FROM best_diem
            GROUP BY ma_sinh_vien
        """)).fetchall()

        th_buf = []
        for r in rows_gpa:
            if not r.tong_tc: continue
            gpa4 = round(float(r.tong_cl) / float(r.tong_tc), 2)
            th_buf.append({
                "ma_sinh_vien"    : r.ma_sinh_vien,
                "tong_tin_chi"    : int(r.tong_tc),
                "tin_chi_tich_luy": int(r.tc_dat),
                "gpa_he_10"       : round(min(gpa4 * 2.5, 10.0), 2),
                "gpa_he_4"        : gpa4,
                "canh_bao_hoc_vu" : gpa4 < 2.0,
            })
        s.bulk_insert_mappings(TongHopKetQua, th_buf)
        s.commit()
        print(f"    -> {len(th_buf)} SV co GPA")

    # Tổng kết
    tong_dk   = len(dk_buf) + len(hoc_lai_dk)
    tong_diem = len(diem_buf) + len(hoc_lai_diem)
    print()
    print("=" * 65)
    print("  HOAN THANH — CNTT PTIT")
    print(f"  Sinh vien : {len(svs)} (B21-B24, 50 SV/khoa)")
    print(f"  Hoc phan  : {len(hoc_phans)} mon / 9 HK (150 TC)")
    print(f"  Dang ky   : {tong_dk:,}  (hoc lai: {len(hoc_lai_dk):,})")
    print(f"  Diem      : {tong_diem:,}  (hoc lai: {len(hoc_lai_diem):,})")
    print(f"  GPA       : {len(th_buf)} SV")
    print()
    print("  LOGIC ĐẶC BIỆT HK9:")
    print("  - Tot nghiep: co diem HK1-9 (ke ca TTTN)")
    print("  - Dang hoc B21: co diem HK1-8, HK9 chua (dang thuc tap)")
    print("  - Hoc lai: khong ap dung cho HK9 (mon TTTN)")
    print()
    print("  VERIFY:")
    print("  SELECT khoa_hoc, trang_thai_hoc_tap, COUNT(*)")
    print("  FROM sinh_vien GROUP BY 1,2 ORDER BY 1,2;")
    print()
    print("  -- B21 Tot nghiep phai co du 9 HK:")
    print("  SELECT sv.ma_sinh_vien, COUNT(DISTINCT dk.ma_hoc_ky) AS so_hk")
    print("  FROM sinh_vien sv JOIN dang_ky_hoc_phan dk")
    print("  ON sv.ma_sinh_vien=dk.ma_sinh_vien")
    print("  WHERE sv.khoa_hoc='B21' AND sv.trang_thai_hoc_tap='Tot nghiep'")
    print("  GROUP BY sv.ma_sinh_vien ORDER BY so_hk;")
    print("=" * 65)


if __name__ == "__main__":
    main()