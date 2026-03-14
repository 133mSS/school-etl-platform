"""
source_models.py  —  SQLAlchemy ORM Models cho Source Database
==============================================================
Khop 100% voi 01_create_tables.sql v2.0

Hierarchy:
  CoSo → Khoa → Nganh → LopHanhChinh → SinhVien
                ↘ GiangVien
  HocPhan ← DangKyHocPhan → DiemHocPhan
  TongHopKetQua (1-1 voi SinhVien)

Thay doi so voi v1.0:
  + Them bang Nganh (nam giua Khoa va LopHanhChinh)
  + LopHanhChinh: ma_khoa → ma_nganh
  + SinhVien: them ma_nganh, bo cccd/sdt/dia_chi/thanh_pho/
              he_dao_tao/ngay_nhap_hoc/hoc_ky_hien_tai/ma_co_van/ma_khoa
  + CoSo: bo thanh_pho
  + HocKyNamHoc: ma_hoc_ky VARCHAR(50) thay vi VARCHAR(20)
  + SinhVien: bo trigger ngay_cap_nhat (giang_vien van con)
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import (
    String, Integer, Numeric, Boolean, Date, DateTime,
    Text, ForeignKey, UniqueConstraint, Index, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.config.database import SourceBase


# =============================================
# 1. CO_SO (Campus)
# Vi du: CS-HN (Ha Noi), CS-HCM (TP.HCM)
# =============================================
class CoSo(SourceBase):
    __tablename__ = "co_so"

    ma_co_so:  Mapped[str]           = mapped_column(String(10),  primary_key=True)
    ten_co_so: Mapped[str]           = mapped_column(String(200), nullable=False)
    dia_chi:   Mapped[Optional[str]] = mapped_column(Text)
    # Khong co thanh_pho (da bo o v2.0)
    ngay_tao:  Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    # Relationships
    khoa_list:       Mapped[List["Khoa"]]      = relationship(back_populates="co_so")
    giang_vien_list: Mapped[List["GiangVien"]] = relationship(back_populates="co_so")

    def __repr__(self):
        return f"<CoSo {self.ma_co_so}: {self.ten_co_so}>"


# =============================================
# 2. KHOA (Faculty)
# Vi du: CNTT1, ATTT, DTVT
# =============================================
class Khoa(SourceBase):
    __tablename__ = "khoa"

    ma_khoa:  Mapped[str]           = mapped_column(String(10),  primary_key=True)
    ten_khoa: Mapped[str]           = mapped_column(String(200), nullable=False)
    ma_co_so: Mapped[Optional[str]] = mapped_column(ForeignKey("co_so.ma_co_so"))
    ngay_tao: Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    # Relationships
    co_so:           Mapped[Optional["CoSo"]]    = relationship(back_populates="khoa_list")
    nganh_list:      Mapped[List["Nganh"]]       = relationship(back_populates="khoa")
    giang_vien_list: Mapped[List["GiangVien"]]   = relationship(back_populates="khoa")
    hoc_phan_list:   Mapped[List["HocPhan"]]     = relationship(back_populates="khoa")

    def __repr__(self):
        return f"<Khoa {self.ma_khoa}: {self.ten_khoa}>"


# =============================================
# 3. NGANH (Major)  <-- MOI so voi v1.0
# Vi du: CNTT (Cong nghe thong tin), KTDL (Ky thuat du lieu)
# Nam giua Khoa va LopHanhChinh
# =============================================
class Nganh(SourceBase):
    __tablename__ = "nganh"

    ma_nganh:  Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ten_nganh: Mapped[str]           = mapped_column(String(200), nullable=False)
    ma_khoa:   Mapped[Optional[str]] = mapped_column(ForeignKey("khoa.ma_khoa"))
    ngay_tao:  Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    # Relationships
    khoa:           Mapped[Optional["Khoa"]]     = relationship(back_populates="nganh_list")
    lop_list:       Mapped[List["LopHanhChinh"]] = relationship(back_populates="nganh")
    sinh_vien_list: Mapped[List["SinhVien"]]     = relationship(back_populates="nganh")

    def __repr__(self):
        return f"<Nganh {self.ma_nganh}: {self.ten_nganh}>"


# =============================================
# 4. GIANG_VIEN (Instructor)
# Co: so_dien_thoai, ngay_tuyen_dung, ngay_cap_nhat (giu lai)
# =============================================
class GiangVien(SourceBase):
    __tablename__ = "giang_vien"

    ma_giang_vien:       Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ho:                  Mapped[str]           = mapped_column(String(50),  nullable=False)
    ten:                 Mapped[str]           = mapped_column(String(50),  nullable=False)
    email:               Mapped[str]           = mapped_column(String(100), unique=True, nullable=False)
    so_dien_thoai:       Mapped[Optional[str]] = mapped_column(String(15))
    chuc_danh:           Mapped[Optional[str]] = mapped_column(String(50))
    trang_thai_cong_tac: Mapped[str]           = mapped_column(String(20),  default="Dang cong tac")
    ngay_tuyen_dung:     Mapped[Optional[date]]= mapped_column(Date)
    ma_khoa:             Mapped[Optional[str]] = mapped_column(ForeignKey("khoa.ma_khoa"))
    ma_co_so:            Mapped[Optional[str]] = mapped_column(ForeignKey("co_so.ma_co_so"))
    ngay_tao:            Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())
    ngay_cap_nhat:       Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    # Relationships
    khoa:         Mapped[Optional["Khoa"]]      = relationship(back_populates="giang_vien_list")
    co_so:        Mapped[Optional["CoSo"]]      = relationship(back_populates="giang_vien_list")
    lop_co_van:   Mapped[List["LopHanhChinh"]]  = relationship(back_populates="co_van")
    dang_ky_list: Mapped[List["DangKyHocPhan"]] = relationship(back_populates="giang_vien")

    @property
    def ho_ten(self) -> str:
        return f"{self.ho} {self.ten}"

    def __repr__(self):
        return f"<GiangVien {self.ma_giang_vien}: {self.ho_ten} ({self.chuc_danh})>"


# =============================================
# 5. LOP_HANH_CHINH (Administrative Class)
# v2.0: ma_khoa → ma_nganh (lop thuoc nganh, khong phai khoa)
# Vi du: D21CQCN01-B
# =============================================
class LopHanhChinh(SourceBase):
    __tablename__ = "lop_hanh_chinh"

    ma_lop:    Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ten_lop:   Mapped[str]           = mapped_column(String(100), nullable=False)
    khoa_hoc:  Mapped[str]           = mapped_column(String(10),  nullable=False)
    ma_nganh:  Mapped[Optional[str]] = mapped_column(ForeignKey("nganh.ma_nganh"))  # v2.0: thay ma_khoa
    ma_co_van: Mapped[Optional[str]] = mapped_column(ForeignKey("giang_vien.ma_giang_vien"))
    ngay_tao:  Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    # Relationships
    nganh:          Mapped[Optional["Nganh"]]     = relationship(back_populates="lop_list")
    co_van:         Mapped[Optional["GiangVien"]] = relationship(back_populates="lop_co_van")
    sinh_vien_list: Mapped[List["SinhVien"]]      = relationship(back_populates="lop")

    __table_args__ = (
        Index("idx_lop_nganh",    "ma_nganh"),
        Index("idx_lop_khoa_hoc", "khoa_hoc"),
    )

    def __repr__(self):
        return f"<LopHanhChinh {self.ma_lop} khoa={self.khoa_hoc}>"


# =============================================
# 6. SINH_VIEN (Student)
#
# v2.0 — Chi giu nhung gi can thiet:
#   Co  : ho, ten, ngay_sinh, gioi_tinh, email,
#          ma_nganh, ma_lop, khoa_hoc, trang_thai_hoc_tap
#   Bo  : cccd, so_dien_thoai, dia_chi, thanh_pho, he_dao_tao,
#          ngay_nhap_hoc, hoc_ky_hien_tai, ma_co_van, ma_khoa,
#          ngay_cap_nhat (trigger da bo)
# =============================================
class SinhVien(SourceBase):
    __tablename__ = "sinh_vien"

    ma_sinh_vien:       Mapped[str]           = mapped_column(String(20),  primary_key=True)
    ho:                 Mapped[str]           = mapped_column(String(50),  nullable=False)
    ten:                Mapped[str]           = mapped_column(String(50),  nullable=False)
    ngay_sinh:          Mapped[date]          = mapped_column(Date,        nullable=False)
    gioi_tinh:          Mapped[Optional[str]] = mapped_column(String(10))
    email:              Mapped[str]           = mapped_column(String(100), unique=True, nullable=False)

    # Academic info
    ma_nganh:           Mapped[Optional[str]] = mapped_column(ForeignKey("nganh.ma_nganh"))
    ma_lop:             Mapped[Optional[str]] = mapped_column(ForeignKey("lop_hanh_chinh.ma_lop"))
    khoa_hoc:           Mapped[str]           = mapped_column(String(10),  nullable=False)
    trang_thai_hoc_tap: Mapped[str]           = mapped_column(String(30),  default="Dang hoc")

    ngay_tao:           Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    # Relationships
    nganh:        Mapped[Optional["Nganh"]]         = relationship(back_populates="sinh_vien_list")
    lop:          Mapped[Optional["LopHanhChinh"]]  = relationship(back_populates="sinh_vien_list")
    dang_ky_list: Mapped[List["DangKyHocPhan"]]     = relationship(back_populates="sinh_vien")
    tong_hop:     Mapped[Optional["TongHopKetQua"]] = relationship(back_populates="sinh_vien", uselist=False)

    __table_args__ = (
        Index("idx_sv_nganh",      "ma_nganh"),
        Index("idx_sv_lop",        "ma_lop"),
        Index("idx_sv_khoa_hoc",   "khoa_hoc"),
        Index("idx_sv_trang_thai", "trang_thai_hoc_tap"),
        Index("idx_sv_email",      "email"),
    )

    @property
    def ho_ten(self) -> str:
        return f"{self.ho} {self.ten}"

    def __repr__(self):
        return f"<SinhVien {self.ma_sinh_vien}: {self.ho_ten} [{self.khoa_hoc}]>"


# =============================================
# 7. HOC_PHAN (Course)
# so_tin_chi: max 12 (thay vi 6, vi HK9 co 12 TC thuc tap)
# =============================================
class HocPhan(SourceBase):
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
    ngay_tao:         Mapped[datetime]      = mapped_column(DateTime, server_default=func.now())

    # Relationships
    khoa:         Mapped[Optional["Khoa"]]      = relationship(back_populates="hoc_phan_list")
    dang_ky_list: Mapped[List["DangKyHocPhan"]] = relationship(back_populates="hoc_phan")

    __table_args__ = (
        Index("idx_hp_khoa",   "ma_khoa"),
        Index("idx_hp_hoc_ky", "hoc_ky_de_xuat"),
    )

    def __repr__(self):
        return f"<HocPhan {self.ma_hoc_phan}: {self.ten_mon} ({self.so_tin_chi} TC)>"


# =============================================
# 8. HOC_KY_NAM_HOC (Academic Term)
# v2.0: ma_hoc_ky VARCHAR(50) — vi "HK1-2021-22" dai hon VARCHAR(20)
# =============================================
class HocKyNamHoc(SourceBase):
    __tablename__ = "hoc_ky_nam_hoc"

    ma_hoc_ky:     Mapped[str]            = mapped_column(String(50), primary_key=True)  # VARCHAR(50)
    nam_hoc:       Mapped[str]            = mapped_column(String(50), nullable=False)
    hoc_ky:        Mapped[str]            = mapped_column(String(50), nullable=False)
    ngay_bat_dau:  Mapped[Optional[date]] = mapped_column(Date)
    ngay_ket_thuc: Mapped[Optional[date]] = mapped_column(Date)

    # Relationships
    dang_ky_list: Mapped[List["DangKyHocPhan"]] = relationship(back_populates="hoc_ky")

    def __repr__(self):
        return f"<HocKyNamHoc {self.ma_hoc_ky}>"


# =============================================
# 9. DANG_KY_HOC_PHAN (Enrollment)
# ma_hoc_ky: VARCHAR(50) dong bo voi HocKyNamHoc
# =============================================
class DangKyHocPhan(SourceBase):
    __tablename__ = "dang_ky_hoc_phan"

    ma_dang_ky:    Mapped[int]           = mapped_column(Integer, primary_key=True, autoincrement=True)
    ma_sinh_vien:  Mapped[str]           = mapped_column(ForeignKey("sinh_vien.ma_sinh_vien"),   nullable=False)
    ma_hoc_phan:   Mapped[str]           = mapped_column(ForeignKey("hoc_phan.ma_hoc_phan"),     nullable=False)
    ma_hoc_ky:     Mapped[str]           = mapped_column(ForeignKey("hoc_ky_nam_hoc.ma_hoc_ky"), nullable=False)
    ma_giang_vien: Mapped[Optional[str]] = mapped_column(ForeignKey("giang_vien.ma_giang_vien"))
    ngay_dang_ky:  Mapped[date]          = mapped_column(Date, server_default=func.current_date())
    trang_thai:    Mapped[str]           = mapped_column(String(30), default="Da dang ky")

    # Relationships
    sinh_vien:  Mapped["SinhVien"]              = relationship(back_populates="dang_ky_list")
    hoc_phan:   Mapped["HocPhan"]               = relationship(back_populates="dang_ky_list")
    hoc_ky:     Mapped["HocKyNamHoc"]           = relationship(back_populates="dang_ky_list")
    giang_vien: Mapped[Optional["GiangVien"]]   = relationship(back_populates="dang_ky_list")
    diem:       Mapped[Optional["DiemHocPhan"]] = relationship(back_populates="dang_ky", uselist=False)

    __table_args__ = (
        UniqueConstraint("ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky",
                         name="uq_dang_ky_sv_hp_hk"),
        Index("idx_dk_sinh_vien",  "ma_sinh_vien"),
        Index("idx_dk_hoc_phan",   "ma_hoc_phan"),
        Index("idx_dk_hoc_ky",     "ma_hoc_ky"),
        Index("idx_dk_trang_thai", "trang_thai"),
        Index("idx_dk_giang_vien", "ma_giang_vien"),
    )

    def __repr__(self):
        return f"<DangKyHocPhan {self.ma_dang_ky}: SV={self.ma_sinh_vien} HP={self.ma_hoc_phan}>"


# =============================================
# 10. DIEM_HOC_PHAN (Grades)
# =============================================
class DiemHocPhan(SourceBase):
    __tablename__ = "diem_hoc_phan"

    ma_diem:         Mapped[int]               = mapped_column(Integer, primary_key=True, autoincrement=True)
    ma_dang_ky:      Mapped[int]               = mapped_column(ForeignKey("dang_ky_hoc_phan.ma_dang_ky"), unique=True)

    diem_chuyen_can: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_bai_tap:    Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_giua_ky:    Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_cuoi_ky:    Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    diem_tong_ket:   Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))

    diem_chu:        Mapped[Optional[str]]     = mapped_column(String(2))
    diem_he_4:       Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    dat_mon:         Mapped[Optional[bool]]    = mapped_column(Boolean)
    hoc_lai:         Mapped[bool]              = mapped_column(Boolean, default=False)

    ngay_cham:       Mapped[Optional[datetime]]= mapped_column(DateTime)
    ngay_tao:        Mapped[datetime]          = mapped_column(DateTime, server_default=func.now())

    # Relationships
    dang_ky: Mapped["DangKyHocPhan"] = relationship(back_populates="diem")

    __table_args__ = (
        Index("idx_diem_dang_ky", "ma_dang_ky"),
        Index("idx_diem_dat_mon", "dat_mon"),
        Index("idx_diem_chu",     "diem_chu"),
        Index("idx_diem_hoc_lai", "hoc_lai"),
    )

    def __repr__(self):
        return f"<DiemHocPhan dk={self.ma_dang_ky}: {self.diem_tong_ket} ({self.diem_chu})>"


# =============================================
# 11. TONG_HOP_KET_QUA (Academic Summary)
# 1-1 voi SinhVien, duoc tinh lai moi ETL run
# =============================================
class TongHopKetQua(SourceBase):
    __tablename__ = "tong_hop_ket_qua"

    ma_sinh_vien:     Mapped[str]               = mapped_column(ForeignKey("sinh_vien.ma_sinh_vien"), primary_key=True)
    tong_tin_chi:     Mapped[int]               = mapped_column(Integer, default=0)
    tin_chi_tich_luy: Mapped[int]               = mapped_column(Integer, default=0)
    gpa_he_10:        Mapped[Optional[Decimal]]  = mapped_column(Numeric(4, 2))
    gpa_he_4:         Mapped[Optional[Decimal]]  = mapped_column(Numeric(3, 2))
    canh_bao_hoc_vu:  Mapped[bool]              = mapped_column(Boolean, default=False)
    ngay_cap_nhat:    Mapped[datetime]           = mapped_column(DateTime, server_default=func.now())

    # Relationships
    sinh_vien: Mapped["SinhVien"] = relationship(back_populates="tong_hop")

    def __repr__(self):
        return f"<TongHopKetQua {self.ma_sinh_vien}: GPA4={self.gpa_he_4}>"