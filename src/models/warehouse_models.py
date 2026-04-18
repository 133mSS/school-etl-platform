"""
warehouse_models.py - SQLAlchemy ORM cho Data Warehouse
Version: 2.0
DB: school_warehouse | Port: 5435
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Column, String, Integer, Numeric, Boolean, BigInteger,
    Date, DateTime, ForeignKey, Index, func
)
from sqlalchemy.orm import relationship
from src.config.database import WarehouseBase



# =============================================
# DIM_SINH_VIEN (SCD Type 2)
# =============================================
class DimSinhVien(WarehouseBase):
    __tablename__ = "dim_sinh_vien"

    sinh_vien_key       = Column(Integer, primary_key=True, autoincrement=True)
    ma_sinh_vien        = Column(String(20), nullable=False)
    ho                  = Column(String(50))
    ten                 = Column(String(50))
    ho_ten              = Column(String(100), nullable=False)
    ngay_sinh           = Column(Date)
    gioi_tinh           = Column(String(10))
    email               = Column(String(100))
    khoa_hoc            = Column(String(10))
    trang_thai_hoc_tap  = Column(String(30))

    # Nganh
    ma_nganh    = Column(String(20))
    ten_nganh   = Column(String(200))

    # Khoa
    ma_khoa     = Column(String(10))
    ten_khoa    = Column(String(200))

    # Lop
    ma_lop      = Column(String(20))
    ten_lop     = Column(String(100))

    # Co van
    ma_co_van   = Column(String(20))
    ten_co_van  = Column(String(100))

    # SCD Type 2
    ngay_hieu_luc      = Column(Date, nullable=False, server_default=func.current_date())
    ngay_het_hieu_luc  = Column(Date)
    la_ban_hien_tai    = Column(Boolean, nullable=False, default=True)
    phien_ban          = Column(Integer, nullable=False, default=1)
    ngay_tao           = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_dim_sv_ma",        "ma_sinh_vien"),
        Index("idx_dim_sv_hien_tai",  "la_ban_hien_tai"),
        Index("idx_dim_sv_nganh",     "ma_nganh"),
        Index("idx_dim_sv_lop",       "ma_lop"),
        Index("idx_dim_sv_khoa_hoc",  "khoa_hoc"),
        Index("idx_dim_sv_trang_thai","trang_thai_hoc_tap"),
    )

    def __repr__(self):
        return f"<DimSinhVien {self.ma_sinh_vien}: {self.ho_ten}>"


# =============================================
# DIM_HOC_PHAN
# =============================================
class DimHocPhan(WarehouseBase):
    __tablename__ = "dim_hoc_phan"

    hoc_phan_key        = Column(Integer, primary_key=True, autoincrement=True)
    ma_hoc_phan         = Column(String(20), unique=True, nullable=False)
    ma_mon              = Column(String(10))
    ten_mon             = Column(String(200), nullable=False)
    so_tin_chi          = Column(Integer)
    so_gio_ly_thuyet    = Column(Integer)
    so_gio_thuc_hanh    = Column(Integer)
    hoc_ky_de_xuat      = Column(Integer)
    bat_buoc            = Column(Boolean)
    loai_hoc_phan       = Column(String(50))
    ma_khoa             = Column(String(10))
    ten_khoa            = Column(String(200))
    ngay_tao            = Column(DateTime, server_default=func.now())

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

    giang_vien_key      = Column(Integer, primary_key=True, autoincrement=True)
    ma_giang_vien       = Column(String(20), unique=True, nullable=False)
    ho                  = Column(String(50))
    ten                 = Column(String(50))
    ho_ten              = Column(String(100), nullable=False)
    email               = Column(String(100))
    so_dien_thoai       = Column(String(15))
    chuc_danh           = Column(String(50))
    trang_thai_cong_tac = Column(String(20))
    ma_khoa             = Column(String(10))
    ten_khoa            = Column(String(200))
    ngay_tao            = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_dim_gv_ma",   "ma_giang_vien"),
        Index("idx_dim_gv_khoa", "ma_khoa"),
    )

    def __repr__(self):
        return f"<DimGiangVien {self.ma_giang_vien}: {self.ho_ten}>"


# =============================================
# DIM_HOC_KY
# =============================================
class DimHocKy(WarehouseBase):
    __tablename__ = "dim_hoc_ky"

    hoc_ky_key      = Column(Integer, primary_key=True, autoincrement=True)
    ma_hoc_ky       = Column(String(50), unique=True, nullable=False)
    nam_hoc         = Column(String(50), nullable=False)
    hoc_ky          = Column(String(50), nullable=False)
    ngay_bat_dau    = Column(Date)
    ngay_ket_thuc   = Column(Date)
    nam_bat_dau     = Column(Integer)
    nam_ket_thuc    = Column(Integer)
    ngay_tao        = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_dim_hk_ma",      "ma_hoc_ky"),
        Index("idx_dim_hk_nam_hoc", "nam_hoc"),
    )

    def __repr__(self):
        return f"<DimHocKy {self.ma_hoc_ky}>"


# =============================================
# FACT_HOC_TAP - Nguon 1: PostgreSQL
# =============================================
class FactHocTap(WarehouseBase):
    __tablename__ = "fact_hoc_tap"

    hoc_tap_key     = Column(Integer, primary_key=True, autoincrement=True)
    sinh_vien_key   = Column(Integer, ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    hoc_phan_key    = Column(Integer, ForeignKey("dim_hoc_phan.hoc_phan_key"), nullable=False)
    giang_vien_key  = Column(Integer, ForeignKey("dim_giang_vien.giang_vien_key"))
    hoc_ky_key      = Column(Integer, ForeignKey("dim_hoc_ky.hoc_ky_key"), nullable=False)
  

    ma_sinh_vien    = Column(String(20), nullable=False)
    ma_hoc_phan     = Column(String(20), nullable=False)
    ma_dang_ky      = Column(Integer)

    diem_chuyen_can = Column(Numeric(4, 2))
    diem_bai_tap    = Column(Numeric(4, 2))
    diem_giua_ky    = Column(Numeric(4, 2))
    diem_cuoi_ky    = Column(Numeric(4, 2))
    diem_tong_ket   = Column(Numeric(4, 2))
    diem_chu        = Column(String(2))
    diem_he_4       = Column(Numeric(3, 2))
    dat_mon         = Column(Boolean)
    hoc_lai         = Column(Boolean)
    so_tin_chi      = Column(Integer)
    diem_chat_luong = Column(Numeric(5, 2))
    ngay_load       = Column(DateTime, server_default=func.now())
    nguon_du_lieu   = Column(String(50), default="postgresql")

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
# FACT_DANG_KY - Nguon 1: PostgreSQL
# =============================================
class FactDangKy(WarehouseBase):
    __tablename__ = "fact_dang_ky"

    dang_ky_key     = Column(Integer, primary_key=True, autoincrement=True)
    sinh_vien_key   = Column(Integer, ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    hoc_phan_key    = Column(Integer, ForeignKey("dim_hoc_phan.hoc_phan_key"), nullable=False)
    giang_vien_key  = Column(Integer, ForeignKey("dim_giang_vien.giang_vien_key"))
    hoc_ky_key      = Column(Integer, ForeignKey("dim_hoc_ky.hoc_ky_key"), nullable=False)


    ma_sinh_vien    = Column(String(20), nullable=False)
    ma_hoc_phan     = Column(String(20), nullable=False)
    ma_dang_ky      = Column(Integer)
    trang_thai      = Column(String(30))
    so_tin_chi      = Column(Integer)
    ngay_dang_ky    = Column(Date)
    ngay_load       = Column(DateTime, server_default=func.now())
    nguon_du_lieu   = Column(String(50), default="postgresql")

    __table_args__ = (
        Index("idx_fact_dk_sv", "sinh_vien_key"),
        Index("idx_fact_dk_hk", "hoc_ky_key"),
        Index("idx_fact_dk_ma", "ma_sinh_vien"),
    )

    def __repr__(self):
        return f"<FactDangKy SV={self.ma_sinh_vien} HP={self.ma_hoc_phan}>"


# =============================================
# FACT_CTSV - Nguon 2: CSV Phong CTSV
# =============================================
class FactCtsv(WarehouseBase):
    __tablename__ = "fact_ctsv"

    ctsv_key        = Column(Integer, primary_key=True, autoincrement=True)
    sinh_vien_key   = Column(Integer, ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    hoc_ky_key      = Column(Integer, ForeignKey("dim_hoc_ky.hoc_ky_key"), nullable=False)
    ma_sinh_vien    = Column(String(20), nullable=False)
    ma_hoc_ky       = Column(String(50), nullable=False)

    # Diem ren luyen
    diem_rl         = Column(Integer)
    xep_loai_rl     = Column(String(20))

    # Hoc bong
    loai_hoc_bong   = Column(String(100))
    muc_tien_hb     = Column(BigInteger, default=0)

    # Ky luat
    hinh_thuc_kl    = Column(String(100))
    ly_do_kl        = Column(String(200))

    # Co tong hop
    co_hoc_bong     = Column(Boolean, default=False)
    bi_ky_luat      = Column(Boolean, default=False)

    ngay_load       = Column(DateTime, server_default=func.now())
    nguon_du_lieu   = Column(String(50), default="csv_ctsv")

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
# FACT_TAI_CHINH - Nguon 3: API Portal
# =============================================
class FactTaiChinh(WarehouseBase):
    __tablename__ = "fact_tai_chinh"

    tai_chinh_key       = Column(Integer, primary_key=True, autoincrement=True)
    sinh_vien_key       = Column(Integer, ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    hoc_ky_key          = Column(Integer, ForeignKey("dim_hoc_ky.hoc_ky_key"), nullable=False)
    ma_sinh_vien        = Column(String(20), nullable=False)
    ma_hoc_ky           = Column(String(50), nullable=False)

    # Hoc phi
    hoc_phi_phai_dong   = Column(BigInteger, default=0)
    da_dong             = Column(BigInteger, default=0)
    con_no              = Column(BigInteger, default=0)

    # Mien giam
    duoc_mien_giam      = Column(Boolean, default=False)
    ly_do_mien_giam     = Column(String(100))
    so_tien_mien_giam   = Column(BigInteger, default=0)
    ngay_dong_cuoi      = Column(Date)

    ngay_load           = Column(DateTime, server_default=func.now())
    nguon_du_lieu       = Column(String(50), default="api_portal")

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
# AGG_STUDENT_SUMMARY - Tong hop 3 nguon
# =============================================
class AggStudentSummary(WarehouseBase):
    __tablename__ = "agg_student_summary"

    agg_key             = Column(Integer, primary_key=True, autoincrement=True)
    sinh_vien_key       = Column(Integer, ForeignKey("dim_sinh_vien.sinh_vien_key"), nullable=False)
    ma_sinh_vien        = Column(String(20), unique=True, nullable=False)

    # Hoc tap (Nguon 1)
    gpa_he_10           = Column(Numeric(4, 2))
    gpa_he_4            = Column(Numeric(3, 2))
    xep_loai_hoc_luc    = Column(String(30))
    tong_tin_chi_dang_ky= Column(Integer, default=0)
    tin_chi_dat         = Column(Integer, default=0)
    tin_chi_khong_dat   = Column(Integer, default=0)
    ty_le_dat           = Column(Numeric(5, 2))
    tong_mon_dang_ky    = Column(Integer, default=0)
    so_mon_dat          = Column(Integer, default=0)
    so_mon_khong_dat    = Column(Integer, default=0)
    so_mon_hoc_lai      = Column(Integer, default=0)

    # Ren luyen (Nguon 2)
    diem_rl_trung_binh  = Column(Numeric(4, 1))
    xep_loai_rl_gan_nhat= Column(String(20))

    # Tai chinh (Nguon 3)
    tong_no_hoc_phi     = Column(BigInteger, default=0)
    co_no_hoc_phi       = Column(Boolean, default=False)
    duoc_mien_giam      = Column(Boolean, default=False)

    # Danh gia rui ro tu 3 nguon
    muc_do_rui_ro       = Column(String(20))
    canh_bao_hoc_vu     = Column(Boolean, default=False)
    co_the_tot_nghiep   = Column(Boolean, default=False)

    hoc_ky_key_gan_nhat = Column(Integer, ForeignKey("dim_hoc_ky.hoc_ky_key"))
    ngay_cap_nhat       = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_agg_ss_ma",       "ma_sinh_vien"),
        Index("idx_agg_ss_rui_ro",   "muc_do_rui_ro"),
        Index("idx_agg_ss_gpa4",     "gpa_he_4"),
        Index("idx_agg_ss_canh_bao", "canh_bao_hoc_vu"),
    )

    def __repr__(self):
        return f"<AggStudentSummary {self.ma_sinh_vien}: GPA={self.gpa_he_4}>"