"""
warehouse_models.py - SQLAlchemy ORM cho Data Warehouse
Version: 2.0 — Đồng bộ create_facts.sql v2.0
DB: school_warehouse | Port: 5435

Thay đổi v2.0:
  DimSinhVien  : bỏ he_dao_tao, hoc_ky_hien_tai (không còn trong source)
  FactCtsv     : MỚI — Nguồn 2 CSV (Phòng CTSV)
  FactTaiChinh : MỚI — Nguồn 3 API (Portal vendor)
  AggStudentSummary: thêm diem_rl, tong_no_hoc_phi, muc_do_rui_ro từ 3 nguồn
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    String, Integer, Numeric, Boolean, BigInteger,
    Date, DateTime, ForeignKey, Index, UniqueConstraint, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.config.database import WarehouseBase


# =============================================
# DIM_DATE
# =============================================
class DimDate(WarehouseBase):
    __tablename__ = "dim_date"

    date_key:      Mapped[int]           = mapped_column(Integer, primary_key=True)
    full_date:     Mapped[date]          = mapped_column(Date, unique=True, nullable=False)
    day_of_week:   Mapped[int]           = mapped_column(Integer, nullable=False)
    day_name:      Mapped[str]           = mapped_column(String(20), nullable=False)
    day_of_month:  Mapped[int]           = mapped_column(Integer, nullable=False)
    day_of_year:   Mapped[int]           = mapped_column(Integer, nullable=False)
    week_of_year:  Mapped[int]           = mapped_column(Integer, nullable=False)
    month_num:     Mapped[int]           = mapped_column(Integer, nullable=False)
    month_name:    Mapped[str]           = mapped_column(String(20), nullable=False)
    quarter:       Mapped[int]           = mapped_column(Integer, nullable=False)
    year:          Mapped[int]           = mapped_column(Integer, nullable=False)
    is_weekend:    Mapped[bool]          = mapped_column(Boolean, nullable=False)
    academic_year: Mapped[Optional[str]] = mapped_column(String(50))
    academic_term: Mapped[Optional[str]] = mapped_column(String(20))

    def __repr__(self):
        return f"<DimDate {self.full_date}>"


# =============================================
# DIM_SINH_VIEN (SCD Type 2)
# v2.0: bỏ he_dao_tao, hoc_ky_hien_tai
# =============================================
class DimSinhVien(WarehouseBase):
    __tablename__ = "dim_sinh_vien"

    sinh_vien_key:      Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)
    ma_sinh_vien:       Mapped[str]           = mapped_column(String(20), nullable=False)

    ho:                 Mapped[Optional[str]] = mapped_column(String(50))
    ten:                Mapped[Optional[str]] = mapped_column(String(50))
    ho_ten:             Mapped[str]           = mapped_column(String(100), nullable=False)
    ngay_sinh:          Mapped[Optional[date]]= mapped_column(Date)
    gioi_tinh:          Mapped[Optional[str]] = mapped_column(String(10))
    email:              Mapped[Optional[str]] = mapped_column(String(100))

    khoa_hoc:           Mapped[Optional[str]] = mapped_column(String(10))
    trang_thai_hoc_tap: Mapped[Optional[str]] = mapped_column(String(30))

    # Ngành (v2.0)
    ma_nganh:           Mapped[Optional[str]] = mapped_column(String(20))
    ten_nganh:          Mapped[Optional[str]] = mapped_column(String(200))

    # Khoa
    ma_khoa:            Mapped[Optional[str]] = mapped_column(String(10))
    ten_khoa:           Mapped[Optional[str]] = mapped_column(String(200))

    # Lớp
    ma_lop:             Mapped[Optional[str]] = mapped_column(String(20))
    ten_lop:            Mapped[Optional[str]] = mapped_column(String(100))

    # Cố vấn
    ma_co_van:          Mapped[Optional[str]] = mapped_column(String(20))
    ten_co_van:         Mapped[Optional[str]] = mapped_column(String(100))

    # SCD Type 2
    ngay_hieu_luc:      Mapped[date]          = mapped_column(Date, nullable=False, server_default=func.current_date())
    ngay_het_hieu_luc:  Mapped[Optional[date]]= mapped_column(Date)
    la_ban_hien_tai:    Mapped[bool]          = mapped_column(Boolean, nullable=False, default=True)
    phien_ban:          Mapped[int]           = mapped_column(Integer, nullable=False, default=1)
    ngay_tao:           Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_dim_sv_ma",         "ma_sinh_vien"),
        Index("idx_dim_sv_hien_tai",   "la_ban_hien_tai"),
        Index("idx_dim_sv_nganh",      "ma_nganh"),
        Index("idx_dim_sv_lop",        "ma_lop"),
        Index("idx_dim_sv_khoa_hoc",   "khoa_hoc"),
        Index("idx_dim_sv_trang_thai", "trang_thai_hoc_tap"),
    )

    def __repr__(self):
        return f"<DimSinhVien {self.ma_sinh_vien}: {self.ho_ten}>"


# =============================================
# DIM_HOC_PHAN
# =============================================
class DimHocPhan(WarehouseBase):
    __tablename__ = "dim_hoc_phan"

    hoc_phan_key:     Mapped[int]            = mapped_column(Integer, primary_key=True, autoincrement=True)
    ma_hoc_phan:      Mapped[str]            = mapped_column(String(20), unique=True, nullable=False)
    ma_mon:           Mapped[Optional[str]]  = mapped_column(String(10))
    ten_mon:          Mapped[str]            = mapped_column(String(200), nullable=False)
    so_tin_chi:       Mapped[Optional[int]]  = mapped_column(Integer)
    so_gio_ly_thuyet: Mapped[Optional[int]]  = mapped_column(Integer)
    so_gio_thuc_hanh: Mapped[Optional[int]]  = mapped_column(Integer)
    hoc_ky_de_xuat:   Mapped[Optional[int]]  = mapped_column(Integer)
    bat_buoc:         Mapped[Optional[bool]] = mapped_column(Boolean)
    loai_hoc_phan:    Mapped[Optional[str]]  = mapped_column(String(50))
    ma_khoa:          Mapped[Optional[str]]  = mapped_column(String(10))
    ten_khoa:         Mapped[Optional[str]]  = mapped_column(String(200))
    ngay_tao:         Mapped[datetime]       = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_dim_hp_ma",   "ma_hoc_phan"),
        Index("idx_dim_hp_loai", "loai_hoc_phan"),
        Index("idx_dim_hp_khoa", "ma_khoa"),
    )

    def __repr__(self):
        return f"<DimHocPhan {self.ma_hoc_phan}: {self.ten_mon}>"


# =============================================
# DIM_GIANG_VIEN
# =============================================
class DimGiangVien(WarehouseBase):
    __tablename__ = "dim_giang_vien"

    giang_vien_key:      Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)
    ma_giang_vien:       Mapped[str]           = mapped_column(String(20), unique=True, nullable=False)
    ho:                  Mapped[Optional[str]] = mapped_column(String(50))
    ten:                 Mapped[Optional[str]] = mapped_column(String(50))
    ho_ten:              Mapped[str]           = mapped_column(String(100), nullable=False)
    email:               Mapped[Optional[str]] = mapped_column(String(100))
    so_dien_thoai:       Mapped[Optional[str]] = mapped_column(String(15))
    chuc_danh:           Mapped[Optional[str]] = mapped_column(String(50))
    trang_thai_cong_tac: Mapped[Optional[str]] = mapped_column(String(20))
    ma_khoa:             Mapped[Optional[str]] = mapped_column(String(10))
    ten_khoa:            Mapped[Optional[str]] = mapped_column(String(200))
    ma_co_so:            Mapped[Optional[str]] = mapped_column(String(10))
    ten_co_so:           Mapped[Optional[str]] = mapped_column(String(200))
    ngay_tao:            Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_dim_gv_ma",    "ma_giang_vien"),
        Index("idx_dim_gv_khoa",  "ma_khoa"),
    )

    def __repr__(self):
        return f"<DimGiangVien {self.ma_giang_vien}: {self.ho_ten}>"


# =============================================
# DIM_HOC_KY
# =============================================
class DimHocKy(WarehouseBase):
    __tablename__ = "dim_hoc_ky"

    hoc_ky_key:    Mapped[int]            = mapped_column(Integer, primary_key=True, autoincrement=True)
    ma_hoc_ky:     Mapped[str]            = mapped_column(String(50), unique=True, nullable=False)
    nam_hoc:       Mapped[str]            = mapped_column(String(50), nullable=False)
    hoc_ky:        Mapped[str]            = mapped_column(String(50), nullable=False)  # v2.0: đúng tên
    ngay_bat_dau:  Mapped[Optional[date]] = mapped_column(Date)
    ngay_ket_thuc: Mapped[Optional[date]] = mapped_column(Date)
    nam_bat_dau:   Mapped[Optional[int]]  = mapped_column(Integer)
    nam_ket_thuc:  Mapped[Optional[int]]  = mapped_column(Integer)
    ngay_tao:      Mapped[datetime]       = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_dim_hk_ma",      "ma_hoc_ky"),
        Index("idx_dim_hk_nam_hoc", "nam_hoc"),
    )

    def __repr__(self):
        return f"<DimHocKy {self.ma_hoc_ky}>"


# =============================================
# FACT_HOC_TAP — Nguồn 1: PostgreSQL
# =============================================
class FactHocTap(WarehouseBase):
    __tablename__ = "fact_hoc_tap"

    hoc_tap_key:     Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)

    sinh_vien_key:   Mapped[int]           = mapped_column(ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    hoc_phan_key:    Mapped[int]           = mapped_column(ForeignKey("dim_hoc_phan.hoc_phan_key"), nullable=False)
    giang_vien_key:  Mapped[Optional[int]] = mapped_column(ForeignKey("dim_giang_vien.giang_vien_key"))
    hoc_ky_key:      Mapped[int]           = mapped_column(ForeignKey("dim_hoc_ky.hoc_ky_key"), nullable=False)
    date_key:        Mapped[Optional[int]] = mapped_column(ForeignKey("dim_date.date_key"))

    ma_sinh_vien:    Mapped[str]           = mapped_column(String(20), nullable=False)
    ma_hoc_phan:     Mapped[str]           = mapped_column(String(20), nullable=False)
    ma_dang_ky:      Mapped[Optional[int]] = mapped_column(Integer)

    diem_chuyen_can: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_bai_tap:    Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_giua_ky:    Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_cuoi_ky:    Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_tong_ket:   Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_chu:        Mapped[Optional[str]]     = mapped_column(String(2))
    diem_he_4:       Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    dat_mon:         Mapped[Optional[bool]]    = mapped_column(Boolean)
    hoc_lai:         Mapped[Optional[bool]]    = mapped_column(Boolean)
    so_tin_chi:      Mapped[Optional[int]]     = mapped_column(Integer)
    diem_chat_luong: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))

    ngay_load:       Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())
    nguon_du_lieu:   Mapped[str]           = mapped_column(String(50), default="postgresql")

    __table_args__ = (
        Index("uq_fact_ht_sv_hp_hk", "ma_sinh_vien", "ma_hoc_phan", "hoc_ky_key", unique=True),
        Index("idx_fact_ht_sv",      "sinh_vien_key"),
        Index("idx_fact_ht_hk",      "hoc_ky_key"),
        Index("idx_fact_ht_ma_sv",   "ma_sinh_vien"),
        Index("idx_fact_ht_dat_mon", "dat_mon"),
    )

    def __repr__(self):
        return f"<FactHocTap SV={self.ma_sinh_vien} HP={self.ma_hoc_phan}>"


# =============================================
# FACT_DANG_KY — Nguồn 1: PostgreSQL
# =============================================
class FactDangKy(WarehouseBase):
    __tablename__ = "fact_dang_ky"

    dang_ky_key:    Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)

    sinh_vien_key:  Mapped[int]           = mapped_column(ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    hoc_phan_key:   Mapped[int]           = mapped_column(ForeignKey("dim_hoc_phan.hoc_phan_key"), nullable=False)
    giang_vien_key: Mapped[Optional[int]] = mapped_column(ForeignKey("dim_giang_vien.giang_vien_key"))
    hoc_ky_key:     Mapped[int]           = mapped_column(ForeignKey("dim_hoc_ky.hoc_ky_key"), nullable=False)
    date_key:       Mapped[Optional[int]] = mapped_column(ForeignKey("dim_date.date_key"))

    ma_sinh_vien:   Mapped[str]           = mapped_column(String(20), nullable=False)
    ma_hoc_phan:    Mapped[str]           = mapped_column(String(20), nullable=False)
    ma_dang_ky:     Mapped[Optional[int]] = mapped_column(Integer)

    trang_thai:     Mapped[Optional[str]] = mapped_column(String(30))
    so_tin_chi:     Mapped[Optional[int]] = mapped_column(Integer)
    ngay_dang_ky:   Mapped[Optional[date]]= mapped_column(Date)

    ngay_load:      Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())
    nguon_du_lieu:  Mapped[str]           = mapped_column(String(50), default="postgresql")

    __table_args__ = (
        Index("idx_fact_dk_sv",   "sinh_vien_key"),
        Index("idx_fact_dk_hk",   "hoc_ky_key"),
        Index("idx_fact_dk_ma",   "ma_sinh_vien"),
    )

    def __repr__(self):
        return f"<FactDangKy SV={self.ma_sinh_vien} HP={self.ma_hoc_phan}>"


# =============================================
# FACT_CTSV — Nguồn 2: CSV Phòng CTSV  ← MỚI
# Grain: SinhVien × HocKy
# =============================================
class FactCtsv(WarehouseBase):
    __tablename__ = "fact_ctsv"

    ctsv_key:        Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)

    sinh_vien_key:   Mapped[int]           = mapped_column(ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    hoc_ky_key:      Mapped[int]           = mapped_column(ForeignKey("dim_hoc_ky.hoc_ky_key"), nullable=False)

    ma_sinh_vien:    Mapped[str]           = mapped_column(String(20), nullable=False)
    ma_hoc_ky:       Mapped[str]           = mapped_column(String(50), nullable=False)

    # Điểm rèn luyện
    diem_rl:         Mapped[Optional[int]] = mapped_column(Integer)
    xep_loai_rl:     Mapped[Optional[str]] = mapped_column(String(20))

    # Học bổng
    loai_hoc_bong:   Mapped[Optional[str]] = mapped_column(String(100))
    muc_tien_hb:     Mapped[int]           = mapped_column(BigInteger, default=0)

    # Kỷ luật
    hinh_thuc_kl:    Mapped[Optional[str]] = mapped_column(String(100))
    ly_do_kl:        Mapped[Optional[str]] = mapped_column(String(200))

    # Cờ tổng hợp
    co_hoc_bong:     Mapped[bool]          = mapped_column(Boolean, default=False)
    bi_ky_luat:      Mapped[bool]          = mapped_column(Boolean, default=False)

    ngay_load:       Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())
    nguon_du_lieu:   Mapped[str]           = mapped_column(String(50), default="csv_ctsv")

    __table_args__ = (
        Index("uq_fact_ctsv_sv_hk", "ma_sinh_vien", "hoc_ky_key", unique=True),
        Index("idx_fact_ctsv_sv",   "sinh_vien_key"),
        Index("idx_fact_ctsv_hk",   "hoc_ky_key"),
        Index("idx_fact_ctsv_rl",   "diem_rl"),
        Index("idx_fact_ctsv_hb",   "co_hoc_bong"),
        Index("idx_fact_ctsv_kl",   "bi_ky_luat"),
    )

    def __repr__(self):
        return f"<FactCtsv SV={self.ma_sinh_vien} HK={self.ma_hoc_ky} RL={self.diem_rl}>"


# =============================================
# FACT_TAI_CHINH — Nguồn 3: API Portal  ← MỚI
# Grain: SinhVien × HocKy
# =============================================
class FactTaiChinh(WarehouseBase):
    __tablename__ = "fact_tai_chinh"

    tai_chinh_key:     Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)

    sinh_vien_key:     Mapped[int]           = mapped_column(ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    hoc_ky_key:        Mapped[int]           = mapped_column(ForeignKey("dim_hoc_ky.hoc_ky_key"), nullable=False)

    ma_sinh_vien:      Mapped[str]           = mapped_column(String(20), nullable=False)
    ma_hoc_ky:         Mapped[str]           = mapped_column(String(50), nullable=False)

    # Học phí
    hoc_phi_phai_dong: Mapped[int]           = mapped_column(BigInteger, default=0)
    da_dong:           Mapped[int]           = mapped_column(BigInteger, default=0)
    con_no:            Mapped[int]           = mapped_column(BigInteger, default=0)

    # Miễn giảm
    duoc_mien_giam:    Mapped[bool]          = mapped_column(Boolean, default=False)
    ly_do_mien_giam:   Mapped[Optional[str]] = mapped_column(String(100))
    so_tien_mien_giam: Mapped[int]           = mapped_column(BigInteger, default=0)

    ngay_dong_cuoi:    Mapped[Optional[date]]= mapped_column(Date)

    ngay_load:         Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())
    nguon_du_lieu:     Mapped[str]           = mapped_column(String(50), default="api_portal")

    __table_args__ = (
        Index("uq_fact_tc_sv_hk",  "ma_sinh_vien", "hoc_ky_key", unique=True),
        Index("idx_fact_tc_sv",    "sinh_vien_key"),
        Index("idx_fact_tc_hk",    "hoc_ky_key"),
        Index("idx_fact_tc_ma",    "ma_sinh_vien"),
        Index("idx_fact_tc_mien",  "duoc_mien_giam"),
    )

    def __repr__(self):
        return f"<FactTaiChinh SV={self.ma_sinh_vien} HK={self.ma_hoc_ky} no={self.con_no}>"


# =============================================
# AGG_STUDENT_SUMMARY — tổng hợp 3 nguồn
# =============================================
class AggStudentSummary(WarehouseBase):
    __tablename__ = "agg_student_summary"

    agg_key:               Mapped[int]               = mapped_column(Integer, primary_key=True, autoincrement=True)
    sinh_vien_key:         Mapped[int]               = mapped_column(ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    ma_sinh_vien:          Mapped[str]               = mapped_column(String(20), unique=True, nullable=False)

    # Học tập (Nguồn 1)
    gpa_he_10:             Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    gpa_he_4:              Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    xep_loai_hoc_luc:      Mapped[Optional[str]]     = mapped_column(String(30))
    tong_tin_chi_dang_ky:  Mapped[int]               = mapped_column(Integer, default=0)
    tin_chi_dat:           Mapped[int]               = mapped_column(Integer, default=0)
    tin_chi_khong_dat:     Mapped[int]               = mapped_column(Integer, default=0)
    ty_le_dat:             Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    tong_mon_dang_ky:      Mapped[int]               = mapped_column(Integer, default=0)
    so_mon_dat:            Mapped[int]               = mapped_column(Integer, default=0)
    so_mon_khong_dat:      Mapped[int]               = mapped_column(Integer, default=0)
    so_mon_hoc_lai:        Mapped[int]               = mapped_column(Integer, default=0)

    # Rèn luyện (Nguồn 2)
    diem_rl_trung_binh:    Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 1))
    xep_loai_rl_gan_nhat:  Mapped[Optional[str]]     = mapped_column(String(20))

    # Tài chính (Nguồn 3)
    tong_no_hoc_phi:       Mapped[int]               = mapped_column(BigInteger, default=0)
    co_no_hoc_phi:         Mapped[bool]              = mapped_column(Boolean, default=False)
    duoc_mien_giam:        Mapped[bool]              = mapped_column(Boolean, default=False)

    # Đánh giá rủi ro tổng hợp từ 3 nguồn
    muc_do_rui_ro:         Mapped[Optional[str]]     = mapped_column(String(20))
    canh_bao_hoc_vu:       Mapped[bool]              = mapped_column(Boolean, default=False)
    co_the_tot_nghiep:     Mapped[bool]              = mapped_column(Boolean, default=False)

    hoc_ky_key_gan_nhat:   Mapped[Optional[int]]     = mapped_column(ForeignKey("dim_hoc_ky.hoc_ky_key"))
    ngay_cap_nhat:         Mapped[datetime]          = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_agg_ss_ma",       "ma_sinh_vien"),
        Index("idx_agg_ss_rui_ro",   "muc_do_rui_ro"),
        Index("idx_agg_ss_gpa4",     "gpa_he_4"),
        Index("idx_agg_ss_canh_bao", "canh_bao_hoc_vu"),
    )

    def __repr__(self):
        return f"<AggStudentSummary {self.ma_sinh_vien}: GPA={self.gpa_he_4} RL={self.diem_rl_trung_binh} no={self.co_no_hoc_phi}>"