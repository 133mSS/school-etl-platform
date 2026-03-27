
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import (
    Column, String, Integer, Numeric, Boolean, Date, DateTime,
    Text, ForeignKey, UniqueConstraint, Index, func
)
from sqlalchemy.orm import relationship

from src.config.database import SourceBase


# =============================================
# 2. KHOA (Faculty)
# =============================================
class Khoa(SourceBase):
    __tablename__ = "khoa"

    ma_khoa  = Column(String(10),  primary_key=True)
    ten_khoa = Column(String(200), nullable=False)
    ngay_tao = Column(DateTime,    server_default=func.now())

    # Relationships
    nganh_list      = relationship("Nganh",        back_populates="khoa")
    giang_vien_list = relationship("GiangVien",    back_populates="khoa")
    hoc_phan_list   = relationship("HocPhan",      back_populates="khoa")

    def __repr__(self):
        return f"<Khoa {self.ma_khoa}: {self.ten_khoa}>"


# =============================================
# 3. NGANH (Major)
# =============================================
class Nganh(SourceBase):
    __tablename__ = "nganh"

    ma_nganh  = Column(String(20),  primary_key=True)
    ten_nganh = Column(String(200), nullable=False)
    ma_khoa   = Column(String(10),  ForeignKey("khoa.ma_khoa"))
    ngay_tao  = Column(DateTime,    server_default=func.now())

    # Relationships
    khoa           = relationship("Khoa",           back_populates="nganh_list")
    lop_list       = relationship("LopHanhChinh",   back_populates="nganh")
    sinh_vien_list = relationship("SinhVien",       back_populates="nganh")

    def __repr__(self):
        return f"<Nganh {self.ma_nganh}: {self.ten_nganh}>"


# =============================================
# 4. GIANG_VIEN (Instructor)
# =============================================
class GiangVien(SourceBase):
    __tablename__ = "giang_vien"

    ma_giang_vien       = Column(String(20),  primary_key=True)
    ho                  = Column(String(50),  nullable=False)
    ten                 = Column(String(50),  nullable=False)
    email               = Column(String(100), unique=True, nullable=False)
    so_dien_thoai       = Column(String(15))
    chuc_danh           = Column(String(50))
    trang_thai_cong_tac = Column(String(20),  default="Dang cong tac")
    ngay_tuyen_dung     = Column(Date)
    ma_khoa             = Column(String(10),  ForeignKey("khoa.ma_khoa"))
    ngay_tao            = Column(DateTime,    server_default=func.now())
    ngay_cap_nhat       = Column(DateTime,    server_default=func.now())

    # Relationships
    khoa         = relationship("Khoa",           back_populates="giang_vien_list")
    lop_co_van   = relationship("LopHanhChinh",   back_populates="co_van")
    dang_ky_list = relationship("DangKyHocPhan",  back_populates="giang_vien")

    @property
    def ho_ten(self) -> str:
        return f"{self.ho} {self.ten}"

    def __repr__(self):
        return f"<GiangVien {self.ma_giang_vien}: {self.ho_ten} ({self.chuc_danh})>"


# =============================================
# 5. LOP_HANH_CHINH (Administrative Class)
# =============================================
class LopHanhChinh(SourceBase):
    __tablename__ = "lop_hanh_chinh"

    ma_lop    = Column(String(20),  primary_key=True)
    ten_lop   = Column(String(100), nullable=False)
    khoa_hoc  = Column(String(10),  nullable=False)
    ma_nganh  = Column(String(20),  ForeignKey("nganh.ma_nganh"))
    ma_co_van = Column(String(20),  ForeignKey("giang_vien.ma_giang_vien"))
    ngay_tao  = Column(DateTime,    server_default=func.now())

    # Relationships
    nganh          = relationship("Nganh",    back_populates="lop_list")
    co_van         = relationship("GiangVien", back_populates="lop_co_van")
    sinh_vien_list = relationship("SinhVien",  back_populates="lop")

    __table_args__ = (
        Index("idx_lop_nganh",    "ma_nganh"),
        Index("idx_lop_khoa_hoc", "khoa_hoc"),
    )

    def __repr__(self):
        return f"<LopHanhChinh {self.ma_lop} khoa={self.khoa_hoc}>"


# =============================================
# 6. SINH_VIEN (Student)
# =============================================
class SinhVien(SourceBase):
    __tablename__ = "sinh_vien"

    ma_sinh_vien       = Column(String(20),  primary_key=True)
    ho                 = Column(String(50),  nullable=False)
    ten                = Column(String(50),  nullable=False)
    ngay_sinh          = Column(Date,        nullable=False)
    gioi_tinh          = Column(String(10))
    email              = Column(String(100), unique=True, nullable=False)

    # Academic info
    ma_nganh           = Column(String(20),  ForeignKey("nganh.ma_nganh"))
    ma_lop             = Column(String(20),  ForeignKey("lop_hanh_chinh.ma_lop"))
    khoa_hoc           = Column(String(10),  nullable=False)
    trang_thai_hoc_tap = Column(String(30),  default="Dang hoc")

    ngay_tao           = Column(DateTime,    server_default=func.now())

    # Relationships
    nganh        = relationship("Nganh",           back_populates="sinh_vien_list")
    lop          = relationship("LopHanhChinh",    back_populates="sinh_vien_list")
    dang_ky_list = relationship("DangKyHocPhan",   back_populates="sinh_vien")
    tong_hop     = relationship("TongHopKetQua",   back_populates="sinh_vien", uselist=False)

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
# =============================================
class HocPhan(SourceBase):
    __tablename__ = "hoc_phan"

    ma_hoc_phan      = Column(String(20),  primary_key=True)
    ma_mon           = Column(String(10),  unique=True, nullable=False)
    ten_mon          = Column(String(200), nullable=False)
    so_tin_chi       = Column(Integer,     nullable=False)
    so_gio_ly_thuyet = Column(Integer,     default=0)
    so_gio_thuc_hanh = Column(Integer,     default=0)
    hoc_ky_de_xuat   = Column(Integer)
    bat_buoc         = Column(Boolean,     default=True)
    ma_khoa          = Column(String(10),  ForeignKey("khoa.ma_khoa"))
    ngay_tao         = Column(DateTime,    server_default=func.now())

    # Relationships
    khoa         = relationship("Khoa",          back_populates="hoc_phan_list")
    dang_ky_list = relationship("DangKyHocPhan", back_populates="hoc_phan")

    __table_args__ = (
        Index("idx_hp_khoa",   "ma_khoa"),
        Index("idx_hp_hoc_ky", "hoc_ky_de_xuat"),
    )

    def __repr__(self):
        return f"<HocPhan {self.ma_hoc_phan}: {self.ten_mon} ({self.so_tin_chi} TC)>"


# =============================================
# 8. HOC_KY_NAM_HOC (Academic Term)
# v2.0: ma_hoc_ky VARCHAR(50)
# =============================================
class HocKyNamHoc(SourceBase):
    __tablename__ = "hoc_ky_nam_hoc"

    ma_hoc_ky     = Column(String(50), primary_key=True)
    nam_hoc       = Column(String(50), nullable=False)
    hoc_ky        = Column(String(50), nullable=False)
    ngay_bat_dau  = Column(Date)
    ngay_ket_thuc = Column(Date)

    # Relationships
    dang_ky_list = relationship("DangKyHocPhan", back_populates="hoc_ky")

    def __repr__(self):
        return f"<HocKyNamHoc {self.ma_hoc_ky}>"


# =============================================
# 9. DANG_KY_HOC_PHAN (Enrollment)
# =============================================
class DangKyHocPhan(SourceBase):
    __tablename__ = "dang_ky_hoc_phan"

    ma_dang_ky    = Column(Integer,     primary_key=True, autoincrement=True)
    ma_sinh_vien  = Column(String(20),  ForeignKey("sinh_vien.ma_sinh_vien"),   nullable=False)
    ma_hoc_phan   = Column(String(20),  ForeignKey("hoc_phan.ma_hoc_phan"),     nullable=False)
    ma_hoc_ky     = Column(String(50),  ForeignKey("hoc_ky_nam_hoc.ma_hoc_ky"), nullable=False)
    ma_giang_vien = Column(String(20),  ForeignKey("giang_vien.ma_giang_vien"))
    ngay_dang_ky  = Column(Date,        server_default=func.current_date())
    trang_thai    = Column(String(30),  default="Da dang ky")

    # Relationships
    sinh_vien  = relationship("SinhVien",     back_populates="dang_ky_list")
    hoc_phan   = relationship("HocPhan",      back_populates="dang_ky_list")
    hoc_ky     = relationship("HocKyNamHoc",  back_populates="dang_ky_list")
    giang_vien = relationship("GiangVien",    back_populates="dang_ky_list")
    diem       = relationship("DiemHocPhan",  back_populates="dang_ky", uselist=False)

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

    ma_diem         = Column(Integer,     primary_key=True, autoincrement=True)
    ma_dang_ky      = Column(Integer,     ForeignKey("dang_ky_hoc_phan.ma_dang_ky"), unique=True)

    diem_chuyen_can = Column(Numeric(4, 2))
    diem_bai_tap    = Column(Numeric(4, 2))
    diem_giua_ky    = Column(Numeric(4, 2))
    diem_cuoi_ky    = Column(Numeric(4, 2))
    diem_tong_ket   = Column(Numeric(4, 2))

    diem_chu        = Column(String(2))
    diem_he_4       = Column(Numeric(3, 2))
    dat_mon         = Column(Boolean)
    hoc_lai         = Column(Boolean,     default=False)

    ngay_cham       = Column(DateTime)
    ngay_tao        = Column(DateTime,    server_default=func.now())

    # Relationships
    dang_ky = relationship("DangKyHocPhan", back_populates="diem")

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
# =============================================
class TongHopKetQua(SourceBase):
    __tablename__ = "tong_hop_ket_qua"

    ma_sinh_vien     = Column(String(20),  ForeignKey("sinh_vien.ma_sinh_vien"), primary_key=True)
    tong_tin_chi     = Column(Integer,     default=0)
    tin_chi_tich_luy = Column(Integer,     default=0)
    gpa_he_10        = Column(Numeric(4, 2))
    gpa_he_4         = Column(Numeric(3, 2))
    canh_bao_hoc_vu  = Column(Boolean,     default=False)
    ngay_cap_nhat    = Column(DateTime,    server_default=func.now())

    # Relationships
    sinh_vien = relationship("SinhVien", back_populates="tong_hop")

    def __repr__(self):
        return f"<TongHopKetQua {self.ma_sinh_vien}: GPA4={self.gpa_he_4}>"