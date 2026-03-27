""" 
generate_sample_data.py — PTIT School ETL Platform 
========================================================== 
Sinh dữ liệu mẫu cho 3 ngành: Kế toán, CNTT, Điện tử viễn thông - Nguồn 1: PostgreSQL (Phòng Đào tạo) - Nguồn 2: CSV (Phòng CTSV) - Nguồn 3: API JSON (Portal tài chính) 
MÔN ĐẠI CƯƠNG DÙNG CHUNG: 
DC1xx = chung cả 3 ngành (Triết, Tiếng Anh, Pháp luật, THCS1...) 
DC0xx = chung CNTT + ĐTVT (Giải tích, Đại số, Vật lý, XSTK...) 
KTxxx = riêng Kế toán 
VTxxx = riêng ĐTVT 
CNxxx = riêng CNTT 
QUAN TRỌNG: - tong_tin_chi = tổng TC chương trình (CỐ ĐỊNH theo ngành) - tin_chi_tich_luy = TC đã đạt (thay đổi theo SV) - SV "Tốt nghiệp" không có profile "yếu" 
""" 
import random 
import json 
import csv 
import os 
from datetime import date, datetime, timedelta 
from decimal import Decimal 
from typing import Optional 
from sqlalchemy import ( 
Boolean, Date, DateTime, ForeignKey, Integer, 
Numeric, String, Text, UniqueConstraint, create_engine, func, text 
) 
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column 
# 
═══════════════════════════════════════════════════════
════════════ 
# KẾT NỐI 
# 
═══════════════════════════════════════════════════════
════════════ 
DB_URL = 
"postgresql+psycopg2://school_user:school_pass@localhost:5434/school_source" 
engine = create_engine(DB_URL, echo=False) 
random.seed(42) 
OUTPUT_DIR = "generated_data" 
os.makedirs(f"{OUTPUT_DIR}/csv", exist_ok=True) 
os.makedirs(f"{OUTPUT_DIR}/api_json", exist_ok=True) 
# 
═══════════════════════════════════════════════════════
════════════ 
# ORM MODELS — khớp schema v2.0 (KHÔNG CÓ bảng co_so) 
# 
═══════════════════════════════════════════════════════
════════════ 
class Base(DeclarativeBase): 
pass 
class Khoa(Base): 
__tablename__ = "khoa" 
ma_khoa: Mapped[str] = mapped_column(String(10), primary_key=True) 
ten_khoa: Mapped[str] = mapped_column(String(200), nullable=False) 
ngay_tao: Mapped[datetime] = mapped_column(DateTime, server_default=func.now()) 
class Nganh(Base): 
__tablename__ = "nganh" 
ma_nganh: Mapped[str] = mapped_column(String(20), primary_key=True) 
ten_nganh: Mapped[str] = mapped_column(String(200), nullable=False) 
ma_khoa: Mapped[Optional[str]] = mapped_column(ForeignKey("khoa.ma_khoa")) 
ngay_tao: Mapped[datetime] = mapped_column(DateTime, server_default=func.now()) 
class GiangVien(Base): 
__tablename__ = "giang_vien" 
ma_giang_vien: Mapped[str] = mapped_column(String(20), primary_key=True) 
ho: Mapped[str] = mapped_column(String(50), nullable=False) 
ten: Mapped[str] = mapped_column(String(50), nullable=False) 
email: Mapped[str] = mapped_column(String(100), unique=True, nullable=False) 
so_dien_thoai: Mapped[Optional[str]] = mapped_column(String(15)) 
chuc_danh: Mapped[Optional[str]] = mapped_column(String(50)) 
trang_thai_cong_tac: Mapped[str] = mapped_column(String(20), default="Đang công 
tác") 
ma_khoa: Mapped[Optional[str]] = mapped_column(ForeignKey("khoa.ma_khoa")) 
ngay_tao: Mapped[datetime] = mapped_column(DateTime, server_default=func.now()) 
ngay_cap_nhat: Mapped[datetime] = mapped_column(DateTime, 
server_default=func.now()) 
class LopHanhChinh(Base): 
__tablename__ = "lop_hanh_chinh" 
ma_lop: Mapped[str] = mapped_column(String(20), primary_key=True) 
ten_lop: Mapped[str] = mapped_column(String(100), nullable=False) 
khoa_hoc: Mapped[str] = mapped_column(String(10), nullable=False) 
ma_nganh: Mapped[Optional[str]] = mapped_column(ForeignKey("nganh.ma_nganh")) 
ma_co_van: Mapped[Optional[str]] = 
mapped_column(ForeignKey("giang_vien.ma_giang_vien")) 
ngay_tao: Mapped[datetime] = mapped_column(DateTime, server_default=func.now()) 
class SinhVien(Base): 
__tablename__ = "sinh_vien" 
ma_sinh_vien: Mapped[str] = mapped_column(String(20), primary_key=True) 
ho: Mapped[str] = mapped_column(String(50), nullable=False) 
ten: Mapped[str] = mapped_column(String(50), nullable=False) 
ngay_sinh: Mapped[date] = mapped_column(Date, nullable=False) 
gioi_tinh: Mapped[Optional[str]] = mapped_column(String(10)) 
email: Mapped[str] = mapped_column(String(100), unique=True, nullable=False) 
ma_nganh: Mapped[Optional[str]] = mapped_column(ForeignKey("nganh.ma_nganh")) 
ma_lop: Mapped[Optional[str]] = 
mapped_column(ForeignKey("lop_hanh_chinh.ma_lop")) 
khoa_hoc: Mapped[str] = mapped_column(String(10), nullable=False) 
trang_thai_hoc_tap: Mapped[str] = mapped_column(String(30), default="Đang học") 
ngay_tao: Mapped[datetime] = mapped_column(DateTime, server_default=func.now()) 
class HocPhan(Base): 
__tablename__ = "hoc_phan" 
ma_hoc_phan: Mapped[str] = mapped_column(String(20), primary_key=True) 
ma_mon: Mapped[str] = mapped_column(String(10), unique=True, nullable=False) 
ten_mon: Mapped[str] = mapped_column(String(200), nullable=False) 
so_tin_chi: Mapped[int] = mapped_column(Integer, nullable=False) 
so_gio_ly_thuyet: Mapped[int] = mapped_column(Integer, default=0) 
so_gio_thuc_hanh: Mapped[int] = mapped_column(Integer, default=0) 
hoc_ky_de_xuat: Mapped[Optional[int]] = mapped_column(Integer) 
bat_buoc: Mapped[bool] = mapped_column(Boolean, default=True) 
ma_khoa: Mapped[Optional[str]] = mapped_column(ForeignKey("khoa.ma_khoa")) 
ngay_tao: Mapped[datetime] = mapped_column(DateTime, server_default=func.now()) 
class HocKyNamHoc(Base): 
__tablename__ = "hoc_ky_nam_hoc" 
ma_hoc_ky: Mapped[str] = mapped_column(String(50), primary_key=True) 
nam_hoc: Mapped[str] = mapped_column(String(50), nullable=False) 
hoc_ky: Mapped[str] = mapped_column(String(50), nullable=False) 
ngay_bat_dau: Mapped[Optional[date]] = mapped_column(Date) 
ngay_ket_thuc: Mapped[Optional[date]] = mapped_column(Date) 
class DangKyHocPhan(Base): 
__tablename__ = "dang_ky_hoc_phan" 
ma_dang_ky: Mapped[int] = mapped_column(Integer, primary_key=True, 
autoincrement=True) 
ma_sinh_vien: Mapped[str] = mapped_column(ForeignKey("sinh_vien.ma_sinh_vien"), 
nullable=False) 
ma_hoc_phan: Mapped[str] = mapped_column(ForeignKey("hoc_phan.ma_hoc_phan"), 
nullable=False) 
ma_hoc_ky: Mapped[str] = mapped_column(ForeignKey("hoc_ky_nam_hoc.ma_hoc_ky"), 
nullable=False) 
ma_giang_vien: Mapped[Optional[str]] = 
mapped_column(ForeignKey("giang_vien.ma_giang_vien")) 
ngay_dang_ky: Mapped[date] = mapped_column(Date, 
server_default=func.current_date()) 
trang_thai: Mapped[str] = mapped_column(String(30), default="Đã đăng ký") 
__table_args__ = ( 
UniqueConstraint("ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky", 
name="uq_dang_ky_sv_hp_hk"), 
) 
class DiemHocPhan(Base): 
__tablename__ = "diem_hoc_phan" 
ma_diem: Mapped[int] = mapped_column(Integer, primary_key=True, 
autoincrement=True) 
ma_dang_ky: Mapped[int] = 
mapped_column(ForeignKey("dang_ky_hoc_phan.ma_dang_ky"), unique=True) 
diem_chuyen_can: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2)) 
diem_bai_tap: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2)) 
diem_giua_ky: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2)) 
diem_cuoi_ky: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2)) 
diem_tong_ket: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2)) 
diem_chu: Mapped[Optional[str]] = mapped_column(String(2)) 
diem_he_4: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2)) 
dat_mon: Mapped[Optional[bool]] = mapped_column(Boolean) 
hoc_lai: Mapped[bool] = mapped_column(Boolean, default=False) 
ngay_cham: Mapped[Optional[datetime]] = mapped_column(DateTime) 
ngay_tao: Mapped[datetime] = mapped_column(DateTime, server_default=func.now()) 
class TongHopKetQua(Base): 
__tablename__ = "tong_hop_ket_qua" 
ma_sinh_vien: Mapped[str] = mapped_column(ForeignKey("sinh_vien.ma_sinh_vien"), 
primary_key=True) 
tong_tin_chi: Mapped[int] = mapped_column(Integer, default=0) 
tin_chi_tich_luy: Mapped[int] = mapped_column(Integer, default=0) 
gpa_he_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2)) 
gpa_he_4: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2)) 
canh_bao_hoc_vu: Mapped[bool] = mapped_column(Boolean, default=False) 
ngay_cap_nhat: Mapped[datetime] = mapped_column(DateTime, 
server_default=func.now()) 
# 
═══════════════════════════════════════════════════════
════════════ 
# DỮ LIỆU CỐ ĐỊNH 
# 
═══════════════════════════════════════════════════════
════════════ 
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


# 
═══════════════════════════════════════════════════════
════════════ 
# CHƯƠNG TRÌNH ĐÀO TẠO — MÔN ĐẠI CƯƠNG DÙNG CHUNG 
# DC100 = Triết (2TC thống nhất), DC101 = KTCT, DC102 = CNXH 
# DC103 = TT HCM, DC104 = LS Đảng, DC105 = Pháp luật 
# DC106 = THCS1, DC107 = TA C1, DC108 = TA C2, DC109 = TA C3 
# DC001 = GT1, DC002 = GT2, DC003 = Đại số, DC004 = XLTHS 
# DC005 = KTMT, DC006 = Vật lý, DC007 = XSTK, DC008 = THCS2 
# 
═══════════════════════════════════════════════════════
════════════ 

CHUONG_TRINH_KT = { 
1: { 
"bat_buoc": [ 
("DC100", "Triết học Mác-Lênin", 2, 30, 0, 0.0), 
("KT002", "Kinh tế vi mô 1", 3, 45, 0, 1.0), 
("DC106", "Tin học cơ sở 1", 2, 20, 20, 0.5), 
("KT004", "Toán cao cấp 1", 2, 30, 0, 2.0), 
("DC105", "Pháp luật đại cương", 2, 30, 0, 0.3), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
2: { 
"bat_buoc": [ 
("DC101", "Kinh tế chính trị Mác-Lênin", 3, 45, 0, 0.0), 
("DC107", "Tiếng Anh (Course 1)", 3, 45, 0, 0.8), 
("KT008", "Toán cao cấp 2", 2, 30, 0, 2.2), 
("KT009", "Lý thuyết xác suất và thống kê", 3, 45, 0, 2.0), 
("KT010", "Tin học cơ sở 3", 3, 30, 20, 0.8), 
("KT011", "Kinh tế vĩ mô 1", 3, 45, 0, 1.0), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
3: { 
"bat_buoc": [ 
("DC103", "Tư tưởng Hồ Chí Minh", 2, 30, 0, 0.0), 
("DC108", "Tiếng Anh (Course 2)", 4, 60, 0, 0.8), 
("KT014", "Toán kinh tế", 3, 45, 0, 1.5), 
("KT015", "Nguyên lý kế toán", 3, 45, 0, 1.3), 
("KT016", "Marketing căn bản", 3, 45, 0, 0.8), 
("KT017", "Quản trị học", 3, 45, 0, 0.8), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
4: { 
"bat_buoc": [ 
("DC104", "Lịch sử Đảng cộng sản VN", 3, 45, 0, 0.0), 
("DC109", "Tiếng Anh (Course 3)", 3, 45, 0, 1.0), 
("KT020", "Kế toán quản trị 1", 3, 30, 20, 1.3), 
("KT021", "Kế toán tài chính 1", 3, 30, 20, 1.5), 
("KT022", "Tài chính tiền tệ", 3, 45, 0, 1.2), 
("KT023", "Luật kinh doanh", 4, 60, 0, 0.5), 
("KT024", "Quản trị tài chính doanh nghiệp", 3, 45, 0, 1.3), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
5: { 
"bat_buoc": [ 
("KT025", "Tiếng Anh A22/B12", 4, 60, 0, 1.0), 
("KT026", "Thanh toán quốc tế", 2, 30, 0, 1.0), 
("KT027", "Kiểm toán căn bản", 3, 45, 0, 1.3), 
("KT028", "Kế toán tài chính 2", 3, 30, 20, 1.8), 
("KT029", "Kế toán quản trị 2", 3, 30, 20, 1.5), 
("KT030", "Hệ thống thông tin kế toán", 3, 30, 20, 1.2), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
6: { 
"bat_buoc": [ 
("KT031", "Nguyên lý thống kê kinh tế", 3, 45, 0, 1.2), 
("KT032", "ACCA", 3, 45, 0, 1.5), 
("KT033", "Thuế và kế toán thuế", 3, 45, 0, 1.3), 
("KT034", "Phân tích báo cáo tài chính DN", 2, 30, 0, 1.5), 
("KT035", "Kế toán tài chính 3", 3, 30, 20, 2.0), 
], 
"tu_chon": [ 
("KT036", "Kế toán ngân hàng", 2, 30, 0, 1.0), 
("KT037", "Kế toán hành chính sự nghiệp", 2, 30, 0, 1.0), 
("KT038", "Kế toán hợp nhất", 2, 30, 0, 1.2), 
], 
"so_chon": 3, 
}, 
7: { 
"bat_buoc": [ 
("KT039", "Phương pháp luận NCKH", 2, 30, 0, 0.5), 
("KT040", "Phân tích hoạt động kinh doanh", 2, 30, 0, 1.2), 
("KT041", "CFA", 3, 45, 0, 1.5), 
("KT042", "Kế toán máy", 2, 20, 20, 1.0), 
("KT043", "Kiểm toán tài chính", 3, 30, 20, 1.8), 
], 
"tu_chon": [ 
("KT044", "Kế toán quốc tế", 2, 30, 0, 1.0), 
("KT045", "Định giá tài sản", 2, 30, 0, 1.2), 
("KT046", "Kế toán môi trường", 2, 30, 0, 0.8), 
], 
"so_chon": 3, 
}, 
8: { 
"bat_buoc": [ 
("KT047", "Thực tập và tốt nghiệp", 10, 0, 150, 0.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
} 

CHUONG_TRINH_VT = { 
1: { 
"bat_buoc": [ 
("DC100", "Triết học Mác-Lênin", 2, 30, 0, 0.0), 
("DC106", "Tin học cơ sở 1", 2, 20, 20, 0.5), 
("DC001", "Giải tích 1", 3, 45, 0, 2.5), 
("DC003", "Đại số", 3, 45, 0, 2.0), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
2: { 
"bat_buoc": [ 
("DC101", "Kinh tế chính trị Mác-Lênin", 2, 30, 0, 0.0), 
("DC107", "Tiếng Anh (Course 1)", 4, 60, 0, 0.8), 
("DC008", "Tin học cơ sở 2", 2, 20, 20, 0.8), 
("DC002", "Giải tích 2", 3, 45, 0, 2.5), 
("DC006", "Vật lý đại cương", 4, 45, 15, 1.8), 
("DC007", "Xác suất thống kê", 2, 30, 0, 1.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
3: { 
"bat_buoc": [ 
("DC102", "Chủ nghĩa xã hội khoa học", 2, 30, 0, 0.0), 
("DC108", "Tiếng Anh (Course 2)", 4, 60, 0, 0.8), 
("VT013", "Tín hiệu và hệ thống", 3, 30, 20, 1.8), 
("VT014", "Vật lý và Thí nghiệm", 4, 45, 15, 1.5), 
("VT015", "Lý thuyết mạch", 3, 30, 20, 1.8), 
("VT016", "Linh kiện và mạch điện tử", 3, 30, 20, 1.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
4: { 
"bat_buoc": [ 
("DC103", "Tư tưởng Hồ Chí Minh", 2, 30, 0, 0.0), 
("DC109", "Tiếng Anh (Course 3)", 4, 60, 0, 1.0), 
("DC004", "Xử lý tín hiệu số", 3, 20, 20, 1.8), 
("VT020", "Kỹ thuật siêu cao tần", 3, 30, 20, 2.2), 
("VT021", "Điện tử số", 3, 20, 20, 1.5), 
("VT022", "Lý thuyết truyền tin", 3, 45, 0, 1.8), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
5: { 
"bat_buoc": [ 
("VT023", "Tiếng Anh (Course 3 Plus)", 2, 30, 0, 0.8), 
("DC104", "Lịch sử Đảng cộng sản VN", 2, 30, 0, 0.0), 
("VT025", "Truyền sóng và anten", 3, 30, 20, 1.8), 
("VT026", "Toán rời rạc", 3, 45, 0, 1.5), 
("VT027", "Kỹ thuật lập trình", 3, 30, 30, 1.3), 
("DC005", "Kiến trúc máy tính", 3, 30, 20, 1.3), 
("VT029", "Kỹ thuật vi xử lý", 3, 30, 20, 1.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
6: { 
"bat_buoc": [ 
("VT030", "Hệ điều hành", 2, 20, 20, 1.3), 
("VT031", "Cấu trúc dữ liệu và giải thuật", 3, 30, 30, 2.0), 
("VT032", "Kỹ thuật thông tin quang", 3, 30, 20, 1.5), 
("VT033", "Kỹ thuật mạng truyền thông", 3, 30, 20, 1.3), 
("VT034", "Kỹ thuật thông tin vô tuyến", 2, 20, 20, 1.5), 
("VT035", "Công nghệ phần mềm", 3, 30, 20, 1.0), 
("VT036", "Mô phỏng hệ thống truyền thông", 2, 10, 30, 1.3), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
7: { 
"bat_buoc": [ 
("VT037", "Phương pháp luận NCKH", 2, 30, 0, 0.5), 
("VT038", "Internet và các giao thức", 3, 30, 20, 1.3), 
("VT039", "Mạng truyền thông và quang", 3, 30, 20, 1.5), 
("VT040", "Thông tin di động", 3, 30, 20, 1.3), 
("VT041", "An toàn mạng thông tin", 3, 30, 20, 1.5), 
("VT042", "Cơ sở dữ liệu", 3, 30, 20, 1.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
8: { 
"bat_buoc": [ 
("VT043", "Điện toán và đám mây", 2, 20, 20, 1.2), 
("VT044", "Lập trình hướng đối tượng", 3, 30, 30, 1.3), 
("VT045", "Chuyên đề mạng và dịch vụ Internet", 1, 10, 10, 1.0), 
], 
"tu_chon": [ 
("VT046", "Thiết kế mạng viễn thông", 2, 20, 20, 1.2), 
("VT047", "Xử lý ảnh số", 2, 20, 20, 1.5), 
("VT048", "Mạng cảm biến", 3, 30, 20, 1.3), 
("VT049", "An ninh mạng", 3, 30, 20, 1.5), 
("VT050", "IoT cơ bản", 3, 20, 30, 1.2), 
], 
"so_chon": 5, 
}, 
9: { 
"bat_buoc": [ 
("VT051", "Thực tập và tốt nghiệp", 12, 0, 180, 0.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
} 

CHUONG_TRINH_CN = { 
1: { 
"bat_buoc": [ 
("DC100", "Triết học Mác-Lênin", 2, 30, 0, 0.0), 
("DC106", "Tin học cơ sở 1", 2, 20, 20, 0.5), 
("DC001", "Giải tích 1", 3, 45, 0, 2.5), 
("DC003", "Đại số", 3, 45, 0, 2.0), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
2: { 
"bat_buoc": [ 
("DC101", "Kinh tế chính trị Mác-Lênin", 2, 30, 0, 0.0), 
("DC107", "Tiếng Anh (Course 1)", 4, 60, 0, 0.8), 
("DC008", "Tin học cơ sở 2", 2, 20, 20, 0.8), 
("DC002", "Giải tích 2", 3, 45, 0, 2.5), 
("DC105", "Pháp luật đại cương", 2, 30, 0, 0.3), 
("DC006", "Vật lý đại cương", 4, 45, 15, 1.8), 
("CN011", "Kỹ thuật số", 2, 20, 10, 1.2), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
3: { 
"bat_buoc": [ 
("DC102", "Chủ nghĩa xã hội khoa học", 2, 30, 0, 0.0), 
("DC108", "Tiếng Anh (Course 2)", 4, 60, 0, 0.8), 
("CN014", "Ngôn ngữ lập trình C++", 3, 30, 30, 1.5), 
("CN015", "Toán rời rạc 1", 3, 45, 0, 1.8), 
("DC004", "Xử lý tín hiệu số", 2, 20, 10, 1.5), 
("DC007", "Xác suất thống kê", 3, 45, 0, 1.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
4: { 
"bat_buoc": [ 
("DC103", "Tư tưởng Hồ Chí Minh", 2, 30, 0, 0.0), 
("DC109", "Tiếng Anh (Course 3)", 4, 60, 0, 1.0), 
("DC005", "Kiến trúc máy tính", 3, 30, 20, 1.3), 
("CN021", "Toán rời rạc 2", 3, 45, 0, 2.0), 
("CN022", "Cấu trúc dữ liệu và giải thuật", 3, 30, 30, 2.0), 
("CN023", "Lý thuyết thông tin", 3, 45, 0, 1.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
5: { 
"bat_buoc": [ 
("DC104", "Lịch sử Đảng cộng sản VN", 2, 30, 0, 0.0), 
("CN025", "Tiếng Anh (Course 3 Plus)", 2, 30, 0, 0.8), 
("CN026", "Hệ điều hành", 3, 30, 20, 1.3), 
("CN027", "Lập trình hướng đối tượng", 3, 30, 30, 1.3), 
("CN028", "Cơ sở dữ liệu", 3, 30, 20, 1.5), 
("CN029", "Mạng máy tính", 3, 30, 20, 1.2), 
("CN030", "Lập trình Python", 3, 30, 30, 1.0), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
6: { 
"bat_buoc": [ 
("CN031", "Nhập môn công nghệ phần mềm", 3, 30, 20, 1.0), 
("CN032", "Nhập môn trí tuệ nhân tạo", 3, 30, 20, 1.5), 
("CN033", "An toàn và bảo mật HTTT", 3, 30, 20, 1.3), 
("CN034", "Lập trình web", 3, 20, 40, 1.2), 
("CN035", "Cơ sở dữ liệu phân tán", 3, 30, 20, 1.8), 
("CN036", "Thực tập cơ sở", 4, 0, 60, 0.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
7: { 
"bat_buoc": [ 
("CN037", "QLDA phần mềm", 3, 30, 20, 1.0), 
("CN038", "IoT và ứng dụng", 3, 20, 40, 1.3), 
("CN039", "Phân tích và thiết kế HTTT", 3, 30, 20, 1.5), 
("CN040", "Xử lý ảnh", 3, 20, 40, 1.8), 
], 
"tu_chon": [ 
("CN041", "Kiến trúc và thuật toán song song", 3, 30, 20, 1.2), 
("CN042", "Hệ CSDL đa phương tiện", 3, 30, 20, 1.2), 
("CN043", "Mạng viễn thông thế hệ mới", 3, 30, 20, 1.2), 
], 
"so_chon": 2, 
}, 
8: { 
"bat_buoc": [ 
("CN044", "Thiết kế mạng máy tính", 3, 30, 20, 1.3), 
("CN045", "Đánh giá hiệu năng mạng", 3, 30, 20, 1.5), 
("CN046", "Quản lý mạng máy tính", 3, 30, 20, 1.2), 
("CN047", "An ninh mạng", 3, 30, 20, 1.5), 
], 
"tu_chon": [ 
("CN048", "Điện toán đám mây", 3, 30, 20, 1.2), 
("CN049", "Nhập môn khoa học dữ liệu", 3, 30, 20, 1.2), 
("CN050", "Các hệ thống phân tán", 3, 30, 20, 1.3), 
("CN051", "Phương pháp luận NCKH", 2, 30, 0, 0.5), 
], 
"so_chon": 2, 
}, 
9: { 
"bat_buoc": [ 
("CN052", "Thực tập và tốt nghiệp", 12, 0, 180, 0.5), 
], 
"tu_chon": [], "so_chon": 0, 
}, 
} 

# 
═══════════════════════════════════════════════════════
════════════ 
# CẤU HÌNH 3 NGÀNH 
# 
═══════════════════════════════════════════════════════
════════════ 
NGANH_CONFIG = { 
"KT": { 
"ma_nganh": "KE_TOAN", "ten_nganh": "Kế toán", 
"ma_khoa": "KKT", "ma_viet_tat": "KT", 
"chuong_trinh": CHUONG_TRINH_KT, "max_hk": 8, 
"so_lop_per_khoa": 4, "ty_le_nam": 0.35, 
}, 
"VT": { 
"ma_nganh": "DTVT", "ten_nganh": "Kỹ thuật Điện tử viễn thông", 
"ma_khoa": "KVT", "ma_viet_tat": "VT", 
"chuong_trinh": CHUONG_TRINH_VT, "max_hk": 9, 
"so_lop_per_khoa": 4, "ty_le_nam": 0.80, 
}, 
"CN": { 
"ma_nganh": "CNTT", "ten_nganh": "Công nghệ thông tin", 
"ma_khoa": "CNTT1", "ma_viet_tat": "CN", 
"chuong_trinh": CHUONG_TRINH_CN, "max_hk": 9, 
"so_lop_per_khoa": 4, "ty_le_nam": 0.75, 
}, 
} 

SV_PER_KHOA = {"B21": 130, "B22": 140, "B23": 145, "B24": 150} 

COHORT_CONFIG = { 
"B21": (2021, 9, 8), 
"B22": (2022, 7, 6), 
"B23": (2023, 5, 4), 
"B24": (2024, 3, 2), 
} 

TRANG_THAI_BY_COHORT = { 
"B21": (["Tốt nghiệp", "Đang học", "Bảo lưu", "Thôi học"], [50, 30, 10, 10]), 
"B22": (["Đang học", "Bảo lưu", "Thôi học"], [82, 8, 10]), 
"B23": (["Đang học", "Bảo lưu", "Thôi học"], [85, 8, 7]), 
"B24": (["Đang học", "Bảo lưu", "Thôi học"], [81, 11, 8]), 
} 

HK_MASTER = { 
"HK1-2021-22": ("2021-2022", "Học kỳ 1", date(2021, 9, 6), date(2022, 1, 15)), 
"HK2-2021-22": ("2021-2022", "Học kỳ 2", date(2022, 2, 14), date(2022, 6, 30)), 
"HK1-2022-23": ("2022-2023", "Học kỳ 1", date(2022, 9, 5), date(2023, 1, 14)), 
"HK2-2022-23": ("2022-2023", "Học kỳ 2", date(2023, 2, 13), date(2023, 6, 30)), 
"HK1-2023-24": ("2023-2024", "Học kỳ 1", date(2023, 9, 4), date(2024, 1, 13)), 
"HK2-2023-24": ("2023-2024", "Học kỳ 2", date(2024, 2, 12), date(2024, 6, 28)), 
"HK1-2024-25": ("2024-2025", "Học kỳ 1", date(2024, 9, 2), date(2025, 1, 11)), 
"HK2-2024-25": ("2024-2025", "Học kỳ 2", date(2025, 2, 10), date(2025, 6, 27)), 
"HK1-2025-26": ("2025-2026", "Học kỳ 1", date(2025, 9, 1), date(2026, 3, 14)), 
"HK2-2025-26": ("2025-2026", "Học kỳ 2", date(2026, 2, 9), date(2026, 6, 30)), 
} 

HK_SEQ_BY_COHORT = { 
"B21": ["HK1-2021-22", "HK2-2021-22", "HK1-2022-23", "HK2-2022-23", 
"HK1-2023-24", "HK2-2023-24", "HK1-2024-25", "HK2-2024-25", 
"HK1-2025-26"], 
"B22": ["HK1-2022-23", "HK2-2022-23", "HK1-2023-24", "HK2-2023-24", 
"HK1-2024-25", "HK2-2024-25", "HK1-2025-26"], 
"B23": ["HK1-2023-24", "HK2-2023-24", "HK1-2024-25", "HK2-2024-25", 
"HK1-2025-26"], 
"B24": ["HK1-2024-25", "HK2-2024-25", "HK1-2025-26"], 
} 

GV_DATA = { 
"KKT": [ 
("Nguyễn", "Văn Thành"), ("Trần", "Thị Hương"), ("Lê", "Quốc Bình"), 
("Phạm", "Minh Tuấn"), ("Hoàng", "Thị Lan"), ("Vũ", "Đức Mạnh"), 
("Đỗ", "Thị Ngọc"), ("Bùi", "Văn Hải"), ("Ngô", "Thị Mai"), ("Đặng", "Quốc Hùng"), 
], 
"KVT": [ 
("Nguyễn", "Hữu Đức"), ("Trần", "Quang Vinh"), ("Lê", "Minh Sơn"), 
("Phạm", "Thị Hoa"), ("Hoàng", "Văn Long"), ("Vũ", "Thị Hiền"), 
("Đỗ", "Anh Tuấn"), ("Bùi", "Quốc Đạt"), ("Ngô", "Văn Phong"), ("Đặng", "Thị Thu"), 
], 
"CNTT1": [ 
("Nguyễn", "Minh Hiếu"), ("Trần", "Văn Cường"), ("Lê", "Thị Phương"), 
("Phạm", "Đức Anh"), ("Hoàng", "Quốc Trung"), ("Vũ", "Văn Nam"), 
("Đỗ", "Thị Linh"), ("Bùi", "Minh Khoa"), ("Ngô", "Đức Thịnh"), ("Đặng", "Văn Huy"), 
], 
"KCB": [ 
("Nguyễn", "Thị Hạnh"), ("Trần", "Đức Thắng"), ("Lê", "Văn Khánh"), 
("Phạm", "Thị Dung"), ("Hoàng", "Minh Trí"), 
("Vũ", "Quốc Phương"), ("Đỗ", "Thị Hằng"), 
("Bùi", "Văn Tâm"), ("Ngô", "Thị Thuỷ"), ("Đặng", "Minh Đức"), 
("Lê", "Thuỷ Tiên"), ("Phạm", "Anh Thư"), ("Hoàng", "Bích Ngọc"), 
("Trần", "Quốc Việt"), ("Nguyễn", "Thị Huyền"), 
], 
} 
# 
═══════════════════════════════════════════════════════
════════════ 
# HELPERS 
# 
═══════════════════════════════════════════════════════
════════════ 
def _gen_diem_raw(profile: str) -> float: 
r = random.random() 
if profile == "giỏi": 
tbl = [(0.40, 8.5, 10.0), (0.75, 7.5, 8.49), (0.90, 6.5, 7.49), (0.97, 5.5, 6.49), (1.00, 4.0, 
5.49)] 
elif profile == "yếu": 
tbl = [(0.05, 8.0, 10.0), (0.12, 6.5, 7.99), (0.30, 5.0, 6.49), (0.60, 3.5, 4.99), (1.00, 0.0, 
3.49)] 
else: 
tbl = [(0.15, 8.5, 10.0), (0.45, 7.0, 8.49), (0.75, 5.5, 6.99), (0.92, 4.0, 5.49), (1.00, 0.0, 
3.99)] 
for thr, lo, hi in tbl: 
if r < thr: 
return round(random.uniform(lo, hi), 2) 
return 5.0 
def _gen_diem_mon(profile: str, do_kho: float, hk_idx: int, la_hoc_lai: bool) -> float: 
p = "trung bình" if (la_hoc_lai and profile == "yếu") else profile 
base = _gen_diem_raw(p) 
base -= do_kho * 0.25 
base += min((hk_idx - 1) * 0.10, 0.7) 
if la_hoc_lai: 
base += 1.2 
return round(max(0.0, min(10.0, base)), 2) 
def _diem_sang_chu(d: float) -> tuple: 
if d >= 9.0: return "A+", 4.0 
if d >= 8.5: return "A", 3.7 
if d >= 8.0: return "B+", 3.5 
if d >= 7.0: return "B", 3.0 
if d >= 6.5: return "C+", 2.5 
if d >= 5.5: return "C", 2.0 
if d >= 5.0: return "D+", 1.5 
if d >= 4.0: return "D", 1.0 
return "F", 0.0 
def _tao_diem_record(ma_dk, do_kho, profile, hk_idx, la_hoc_lai, ngay_ket_thuc_hk): 
if profile == "giỏi": 
cc = round(random.uniform(8.5, 10.0), 2) 
elif profile == "yếu": 
cc = round(random.uniform(5.0, 8.5), 2) 
cc = round(max(0.0, cc - do_kho * 0.2), 2) 
else: 
cc = round(random.uniform(6.5, 9.5), 2) 
cc = round(max(0.0, cc - do_kho * 0.1), 2) 
bt = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai) 
gk = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai) 
ck = _gen_diem_mon(profile, do_kho, hk_idx, la_hoc_lai) 
dtk = round(max(0.0, min(10.0, 0.1 * cc + 0.1 * bt + 0.2 * gk + 0.6 * ck)), 2) 
chu, he4 = _diem_sang_chu(dtk) 
return { 
"ma_dang_ky": ma_dk, 
"diem_chuyen_can": cc, "diem_bai_tap": bt, 
"diem_giua_ky": gk, "diem_cuoi_ky": ck, 
"diem_tong_ket": dtk, "diem_chu": chu, 
"diem_he_4": float(he4), "dat_mon": dtk >= 4.0, 
"hoc_lai": la_hoc_lai, 
"ngay_cham": datetime.combine( 
ngay_ket_thuc_hk + timedelta(days=random.randint(7, 21)), 
datetime.min.time() 
), 
} 


def _gen_sdt() -> str: 
p = random.choice(["032", "033", "034", "035", "036", "038", "086", "096", "097", "098"]) 
return p + str(random.randint(1000000, 9999999)) 

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
# 
═══════════════════════════════════════════════════════
════════════ 
# MAIN 
# 
═══════════════════════════════════════════════════════
════════════ 
def main(): 
COHORTS = list(COHORT_CONFIG.keys()) 
total_sv_est = sum(SV_PER_KHOA.values()) * len(NGANH_CONFIG) 

print("=" * 70) 
print(f"  3 ngành: Kế toán, CNTT, Điện tử viễn thông") 
print(f"  Tổng dự kiến: ~{total_sv_est} SV | 4 khóa B21-B24") 
print("=" * 70) 

for nk, cfg in NGANH_CONFIG.items(): 
ct = cfg["chuong_trinh"] 
total_tc = _tinh_tong_tc_den_hk(ct, cfg["max_hk"]) 
tc_per_hk = [] 
for hk_idx in range(1, cfg["max_hk"] + 1): 
mons = _build_mon_list_for_hk(ct[hk_idx]) 
tc_per_hk.append(f"HK{hk_idx}={sum(m[2] for m in mons)}") 
print(f"  {cfg['ten_nganh']}: {total_tc} TC tổng | {' | '.join(tc_per_hk)}") 
print() 

print("[0] Rebuild schema...") 
Base.metadata.drop_all(engine) 
Base.metadata.create_all(engine) 
print("    OK") 

all_csv_records = [] 
all_api_records = [] 

# Track môn đã insert vào DB (tránh trùng ma_mon UNIQUE giữa các ngành) 
global_hp_inserted = set() 

with Session(engine) as s: 
# ── 1. Khoa ── 
khoas = [Khoa(ma_khoa=ma, ten_khoa=ten) for ma, ten in KHOA_DATA] 
s.add_all(khoas) 
s.flush() 
print(f"\n[1] Khoa         : {len(khoas)}") 

# ── 2. Ngành ── 
nganh_objs = [] 
for cfg in NGANH_CONFIG.values(): 
n = Nganh(ma_nganh=cfg["ma_nganh"], ten_nganh=cfg["ten_nganh"], 
ma_khoa=cfg["ma_khoa"]) 
nganh_objs.append(n) 
s.add_all(nganh_objs) 
s.flush() 
print(f"[2] Ngành        : {len(nganh_objs)}") 

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
all_gv_ids = [g.ma_giang_vien for g in gvs] 
print(f"[3] Giảng viên   : {len(gvs)}") 

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
print(f"[4] Học kỳ       : {len(hoc_kys)}") 

hk_lookup = {} 
for cohort, seq in HK_SEQ_BY_COHORT.items(): 
for idx, ma_hk in enumerate(seq, start=1): 
hk_lookup[(cohort, idx)] = hk_by_key[ma_hk] 

# ═══════════ VÒNG LẶP TỪNG NGÀNH ═══════════ 
total_sv_created = 0 
total_dk_created = 0 
total_diem_created = 0 

# 
═══════════════════════════════════════════════════════
════ 
# PHÂN CÔNG GV - MÔN (GLOBAL, trước vòng lặp ngành) 
# Môn DC* → chỉ GV Khoa Cơ bản (KCB) 
# Môn riêng → chỉ GV khoa tương ứng, 2-3 GV/môn 
# 
═══════════════════════════════════════════════════════
════ 
all_mon_by_khoa = {} 
all_mon_dc = [] 
for nk, cfg_tmp in NGANH_CONFIG.items(): 
ct = cfg_tmp["chuong_trinh"] 
for hk_idx in range(1, cfg_tmp["max_hk"] + 1): 
mons = _build_mon_list_for_hk(ct[hk_idx]) 
for ma_mon, ten_mon, tc, lt, th, do_kho in mons: 
if ma_mon.startswith("DC"): 
if ma_mon not in all_mon_dc: 
    all_mon_dc.append(ma_mon) 
else: 
ma_khoa = cfg_tmp["ma_khoa"] 
if ma_khoa not in all_mon_by_khoa: 
    all_mon_by_khoa[ma_khoa] = [] 
if ma_mon not in all_mon_by_khoa[ma_khoa]: 
    all_mon_by_khoa[ma_khoa].append(ma_mon) 

global_gv_per_mon = {} 

# Môn riêng: 2-3 GV từ khoa tương ứng 
for ma_khoa, mon_list in all_mon_by_khoa.items(): 
gv_list = gv_ids_by_khoa[ma_khoa] 
gv_mon_assign = {gv: [] for gv in gv_list} 
for ma_mon in mon_list: 
so_gv_mon = min(3, len(gv_list)) 
sorted_gvs = sorted(gv_list, key=lambda g: len(gv_mon_assign[g])) 
chosen = sorted_gvs[:so_gv_mon] 
global_gv_per_mon[ma_mon] = chosen 
for gv in chosen: 
gv_mon_assign[gv].append(ma_mon) 
mon_per_gv = [len(v) for v in gv_mon_assign.values()] 
print(f"    Khoa {ma_khoa}: {len(mon_list)} môn, GV dạy {min(mon_per_gv)}
{max(mon_per_gv)} môn/người") 

# Môn DC*: chỉ GV Khoa Cơ bản, 3-4 GV/môn 
gv_kcb = gv_ids_by_khoa.get("KCB", []) 
gv_dc_count = {gv: 0 for gv in gv_kcb} 
for ma_mon in all_mon_dc: 
so_gv_mon = min(4, len(gv_kcb)) 
sorted_gvs = sorted(gv_kcb, key=lambda g: gv_dc_count[g]) 
chosen = sorted_gvs[:so_gv_mon] 
global_gv_per_mon[ma_mon] = chosen 
for gv in chosen: 
gv_dc_count[gv] += 1 
dc_per_gv = [v for v in gv_dc_count.values() if v > 0] 
if dc_per_gv: 
print(f"    Khoa KCB: {len(all_mon_dc)} môn DC, GV dạy {min(dc_per_gv)}
{max(dc_per_gv)} môn/người") 
print(f"    Tổng: {len(global_gv_per_mon)} môn đã phân công GV") 


# ═══════════ VÒNG LẶP TỪNG NGÀNH ═══════════ 
for nganh_key, cfg in NGANH_CONFIG.items(): 
print(f"\n{'─' * 50}") 
print(f"  NGÀNH: {cfg['ten_nganh']} ({cfg['ma_nganh']})") 
print(f"{'─' * 50}") 

vt = cfg["ma_viet_tat"] 
chuong_trinh = cfg["chuong_trinh"] 
max_hk = cfg["max_hk"] 

# ── Pre-compute danh sách môn CỐ ĐỊNH cho mỗi HK ── 
mon_by_hk = {} 
for hk_idx in range(1, max_hk + 1): 
mon_by_hk[hk_idx] = _build_mon_list_for_hk(chuong_trinh[hk_idx]) 

for hk_idx in range(1, max_hk + 1): 
tc = sum(m[2] for m in mon_by_hk[hk_idx]) 
print(f"    HK{hk_idx}: {len(mon_by_hk[hk_idx])} môn, {tc} TC") 

# ── 5. Lớp hành chính ── 
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
print(f"  [5] Lớp hành chính : {len(lops)}") 

# ── 6. Học phần (MÔN CHUNG: skip nếu đã insert) ── 
hoc_phans = [] 
hp_lookup = {} 
for hk_idx in range(1, max_hk + 1): 
for ma_mon, ten_mon, tc, lt, th, do_kho in mon_by_hk[hk_idx]: 
# Luôn thêm vào hp_lookup để dùng khi tạo đăng ký 
hp_lookup[ma_mon] = (ma_mon, do_kho) 
# Chỉ insert vào DB nếu chưa có 
if ma_mon in global_hp_inserted: 
continue 
global_hp_inserted.add(ma_mon) 
if ma_mon.startswith("DC"): 
hp_ma_khoa = "KCB" 
else: 
hp_ma_khoa = cfg["ma_khoa"] 
hoc_phans.append(HocPhan( 
ma_hoc_phan=ma_mon, ma_mon=ma_mon, ten_mon=ten_mon, 
so_tin_chi=tc, so_gio_ly_thuyet=lt, so_gio_thuc_hanh=th, 
hoc_ky_de_xuat=hk_idx, bat_buoc=True, 
ma_khoa=hp_ma_khoa, 
)) 
s.add_all(hoc_phans) 
s.flush() 
print(f"  [6] Học phần       : {len(hoc_phans)} mới (tổng DB: {len(global_hp_inserted)})") 


# ── 7. Sinh viên ── 
svs = [] 
hoc_luc_sv = {} 
for cohort in COHORTS: 
nam_nhap = COHORT_CONFIG[cohort][0] 
tt_options, tt_weights = TRANG_THAI_BY_COHORT[cohort] 
so_sv = SV_PER_KHOA[cohort] 

for stt in range(1, so_sv + 1): 
is_nam = random.random() < cfg["ty_le_nam"] 
gioi = "Nam" if is_nam else "Nữ" 
ma_sv = f"{cohort}DC{vt}{stt:03d}" 
email = f"{cohort.lower()}dc{vt.lower()}{stt:03d}@student.ptit.edu.vn" 
trang_thai = random.choices(tt_options, weights=tt_weights)[0] 
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
gioi_tinh=gioi, email=email, 
ma_nganh=cfg["ma_nganh"], 
ma_lop=random.choice(lop_by_cohort[cohort]), 
khoa_hoc=cohort, trang_thai_hoc_tap=trang_thai, 
)) 

if trang_thai == "Tốt nghiệp": 
hl_w = [40, 60, 0] 
elif trang_thai == "Thôi học": 
hl_w = [5, 25, 70] 
elif trang_thai == "Bảo lưu": 
hl_w = [10, 55, 35] 
else: 
hl_w = [30, 58, 12] 
hoc_luc_sv[ma_sv] = random.choices(["giỏi", "trung bình", "yếu"], 
weights=hl_w)[0] 

s.add_all(svs) 
s.flush() 
total_sv_created += len(svs) 

tt_count = {} 
for sv in svs: 
tt_count[sv.trang_thai_hoc_tap] = tt_count.get(sv.trang_thai_hoc_tap, 0) + 1 


print(f"  [7] Sinh viên      : {len(svs)} ({' | '.join(f'{k}:{v}' for k, v in 
sorted(tt_count.items()))})") 

# ── 8. Đăng ký + Điểm ── 
print(f"  [8] Tạo đăng ký + điểm...") 
dk_buf = [] 
dk_set = set() 

for sv in svs: 
_, hk_ht, hk_da_diem = COHORT_CONFIG[sv.khoa_hoc] 
profile = hoc_luc_sv[sv.ma_sinh_vien] 

if sv.trang_thai_hoc_tap == "Tốt nghiệp": 
hk_co_diem = max_hk 
elif sv.trang_thai_hoc_tap == "Đang học": 
hk_co_diem = hk_da_diem 
elif sv.trang_thai_hoc_tap == "Bảo lưu": 
hk_co_diem = max(1, hk_da_diem - 1) 
else: 
hk_co_diem = max(1, hk_da_diem - 2) 

hk_co_diem = min(hk_co_diem, max_hk) 

for hk_idx in range(1, hk_co_diem + 1): 
hk_obj = hk_lookup.get((sv.khoa_hoc, hk_idx)) 
if hk_obj is None: 
continue 
if hk_idx == max_hk and sv.trang_thai_hoc_tap != "Tốt nghiệp": 
continue 

for ma_mon, ten_mon, tc, lt, th, do_kho in mon_by_hk[hk_idx]: 
ma_hp = ma_mon 
key = (sv.ma_sinh_vien, ma_hp, hk_obj.ma_hoc_ky) 
if key in dk_set: 
    continue 
dk_set.add(key) 

                        # Chọn GV từ danh sách cố định của môn đó 
gv_mon_list = global_gv_per_mon.get(ma_hp, gv_ids_by_khoa[cfg["ma_khoa"]]) 
dk_buf.append({ 
    "ma_sinh_vien": sv.ma_sinh_vien, 
    "ma_hoc_phan": ma_hp, 
    "ma_hoc_ky": hk_obj.ma_hoc_ky, 
    "ma_giang_vien": random.choice(gv_mon_list), 
    "ngay_dang_ky": hk_obj.ngay_bat_dau + timedelta(days=random.randint(1, 
10)), 
    "trang_thai": "Đã đăng ký", 
    "_hk_idx": hk_idx, "_do_kho": do_kho, 
    "_profile": profile, "_kt_hk": hk_obj.ngay_ket_thuc, 
}) 

# Insert đăng ký 
dk_clean = [{k: v for k, v in d.items() if not k.startswith("_")} for d in dk_buf] 
inserted_dk = 0 
for i in range(0, len(dk_clean), 500): 
batch = dk_clean[i:i + 500] 
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
total_dk_created += inserted_dk 
print(f"      -> {inserted_dk:,} đăng ký đã insert") 

# Lấy ma_dang_ky 
like_patterns = " OR ".join( 
[f"ma_sinh_vien LIKE '{c}DC{vt}%'" for c in COHORTS] 
) 
all_dk = s.execute(text( 
f"SELECT ma_dang_ky, ma_sinh_vien, ma_hoc_phan, ma_hoc_ky " 
f"FROM dang_ky_hoc_phan WHERE {like_patterns}" 
)).fetchall() 
dk_to_id = {(r.ma_sinh_vien, r.ma_hoc_phan, r.ma_hoc_ky): r.ma_dang_ky for r in 
all_dk} 

# Sinh điểm 
diem_buf = [] 
for d in dk_buf: 
key = (d["ma_sinh_vien"], d["ma_hoc_phan"], d["ma_hoc_ky"]) 
ma_dk = dk_to_id.get(key) 
if ma_dk is None: 
continue 
rec = _tao_diem_record( 
ma_dk, d["_do_kho"], d["_profile"], 
d["_hk_idx"], False, d["_kt_hk"] 
) 
diem_buf.append(rec) 

inserted_diem = 0 
for i in range(0, len(diem_buf), 500): 
batch = diem_buf[i:i + 500] 
result = s.execute(text(""" 
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
inserted_diem += result.rowcount 
s.flush() 
total_diem_created += inserted_diem 
print(f"      -> {inserted_diem:,} điểm đã insert") 

# ── NGUỒN 2: CSV (Phòng CTSV) ── 
for sv in svs: 
profile = hoc_luc_sv[sv.ma_sinh_vien] 
_, _, hk_da_diem = COHORT_CONFIG[sv.khoa_hoc] 

if sv.trang_thai_hoc_tap == "Tốt nghiệp": 
hk_count = max_hk 
elif sv.trang_thai_hoc_tap == "Đang học": 
hk_count = hk_da_diem 
elif sv.trang_thai_hoc_tap == "Bảo lưu": 
hk_count = max(1, hk_da_diem - 1) 
else: 
hk_count = max(1, hk_da_diem - 2) 
hk_count = min(hk_count, max_hk) 

for hk_idx in range(1, hk_count + 1): 
hk_obj = hk_lookup.get((sv.khoa_hoc, hk_idx)) 
if hk_obj is None: 
continue 
if hk_idx == max_hk and sv.trang_thai_hoc_tap != "Tốt nghiệp": 
continue 

if profile == "giỏi": 
drl = int(max(0, min(100, random.gauss(88, 6)))) 
elif profile == "yếu": 
drl = int(max(0, min(100, random.gauss(42, 15)))) 
else: 
drl = int(max(0, min(100, random.gauss(68, 12)))) 

if drl >= 90: xep_loai = "Xuất sắc" 
elif drl >= 80: xep_loai = "Tốt" 
elif drl >= 65: xep_loai = "Khá" 
elif drl >= 50: xep_loai = "Trung bình" 
elif drl >= 35: xep_loai = "Yếu" 
else: xep_loai = "Kém" 

loai_hb, muc_tien = "", 0 
if profile == "giỏi" and drl >= 90: 
loai_hb, muc_tien = "KKHT loại 1", 3600000 
elif profile == "giỏi" and drl >= 80: 
loai_hb, muc_tien = "KKHT loại 2", 2400000 
elif profile == "trung bình" and drl >= 75 and random.random() < 0.15: 
loai_hb, muc_tien = "KKHT loại 3", 1200000 

hinh_thuc_kl, ly_do_kl = "", "" 
if profile == "yếu" and random.random() < 0.15: 
hinh_thuc_kl = random.choice(["Cảnh cáo lần 1", "Cảnh cáo lần 2", "Khiển 
trách"]) 
ly_do_kl = random.choice(["Thi hộ", "Vi phạm quy chế thi", "Gian lận bài tập", 
                            "Nghỉ học quá nhiều", "Vi phạm nội quy KTX"]) 

all_csv_records.append({ 
"ma_sinh_vien": sv.ma_sinh_vien, 
"hoc_ky": hk_obj.ma_hoc_ky, 
"diem_ren_luyen": drl, 
"xep_loai_rl": xep_loai, 
"loai_hoc_bong": loai_hb, 
"muc_tien_hb": muc_tien, 
"hinh_thuc_ky_luat": hinh_thuc_kl, 
"ly_do_ky_luat": ly_do_kl, 
}) 

# ── NGUỒN 3: API JSON (Tài chính) ── 
for sv in svs: 
profile = hoc_luc_sv[sv.ma_sinh_vien] 
_, _, hk_da_diem = COHORT_CONFIG[sv.khoa_hoc] 

if sv.trang_thai_hoc_tap == "Tốt nghiệp": 
hk_count = max_hk 
elif sv.trang_thai_hoc_tap == "Đang học": 
hk_count = hk_da_diem 
elif sv.trang_thai_hoc_tap == "Bảo lưu": 
hk_count = max(1, hk_da_diem - 1) 
else: 
hk_count = max(1, hk_da_diem - 2) 
hk_count = min(hk_count, max_hk) 

for hk_idx in range(1, hk_count + 1): 
hk_obj = hk_lookup.get((sv.khoa_hoc, hk_idx)) 
if hk_obj is None: 
continue 
if hk_idx == max_hk and sv.trang_thai_hoc_tap != "Tốt nghiệp": 
continue 

mons_hk = mon_by_hk[hk_idx] 
tc_hk = sum(m[2] for m in mons_hk) 
gia_tc = random.choice([440000, 460000, 480000, 500000]) 
hoc_phi = tc_hk * gia_tc 

duoc_mien_giam, ly_do_mg, so_tien_mg = False, "", 0 
if random.random() < 0.08: 
duoc_mien_giam = True 
ly_do_mg = random.choice(["Chính sách", "Hộ nghèo", "Cận hộ nghèo", 
                            "Dân tộc thiểu số", "Mồ côi", "Con thương binh"]) 
so_tien_mg = int(hoc_phi * random.choice([0.3, 0.5, 0.7, 1.0])) 

hoc_phi_phai_dong = hoc_phi - so_tien_mg 

if profile == "giỏi": 
da_dong, con_no = hoc_phi_phai_dong, 0 
elif profile == "trung bình": 
if random.random() < 0.85: 
    da_dong, con_no = hoc_phi_phai_dong, 0 
else: 
    da_dong = int(hoc_phi_phai_dong * random.uniform(0.5, 0.9)) 
    con_no = hoc_phi_phai_dong - da_dong 
else: 
if random.random() < 0.50: 
    da_dong, con_no = hoc_phi_phai_dong, 0 
else: 
    da_dong = int(hoc_phi_phai_dong * random.uniform(0.1, 0.6)) 
    con_no = hoc_phi_phai_dong - da_dong 

all_api_records.append({ 
"ma_sinh_vien": sv.ma_sinh_vien, 
"hoc_ky": hk_obj.ma_hoc_ky, 
"hoc_phi_phai_dong": hoc_phi_phai_dong, 
"da_dong": da_dong, "con_no": con_no, 
"duoc_mien_giam": duoc_mien_giam, 
"ly_do_mien_giam": ly_do_mg, 
"so_tien_mien_giam": so_tien_mg, 
"ngay_dong_cuoi": str(hk_obj.ngay_bat_dau + timedelta(days=35)), 
}) 

# ── Pre-compute tổng TC chương trình CỐ ĐỊNH cho mỗi ngành ── 
tong_tc_chuong_trinh = {} 
for nk, cfg in NGANH_CONFIG.items(): 
total = _tinh_tong_tc_den_hk(cfg["chuong_trinh"], cfg["max_hk"]) 
tong_tc_chuong_trinh[cfg["ma_nganh"]] = total 
print(f"    {cfg['ten_nganh']}: {total} TC chương trình") 

# ── GPA ── 
print(f"\n[9] Tính GPA...") 
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

sv_nganh_map = {} 
for row in s.execute(text("SELECT ma_sinh_vien, ma_nganh FROM 
sinh_vien")).fetchall(): 
sv_nganh_map[row.ma_sinh_vien] = row.ma_nganh 

th_buf = [] 
for r in rows_gpa: 
if not r.tc_da_hoc: 
continue 
gpa4 = round(float(r.tong_cl) / float(r.tc_da_hoc), 2) 
ma_nganh = sv_nganh_map.get(r.ma_sinh_vien, "CNTT") 
tong_tc_ct = tong_tc_chuong_trinh.get(ma_nganh, 151) 
th_buf.append({ 
"ma_sinh_vien": r.ma_sinh_vien, 
"tong_tin_chi": tong_tc_ct, 
"tin_chi_tich_luy": int(r.tc_dat), 
"gpa_he_10": round(min(gpa4 * 2.5, 10.0), 2), 
"gpa_he_4": gpa4, 
"canh_bao_hoc_vu": gpa4 < 2.0, 
}) 
s.bulk_insert_mappings(TongHopKetQua, th_buf) 
s.commit() 
print(f"     -> {len(th_buf)} SV có GPA") 

# 
═══════════════════════════════════════════════════════
════ 
# XUẤT CSV + JSON 
# 
═══════════════════════════════════════════════════════
════ 
print(f"\n[10] Xuất CSV (Nguồn 2)...") 
hk_groups_csv = {} 
for rec in all_csv_records: 
hk_groups_csv.setdefault(rec["hoc_ky"], []).append(rec) 
csv_fields = ["ma_sinh_vien", "hoc_ky", "diem_ren_luyen", "xep_loai_rl", 
"loai_hoc_bong", "muc_tien_hb", "hinh_thuc_ky_luat", "ly_do_ky_luat"] 
for hk, records in sorted(hk_groups_csv.items()): 
with open(f"{OUTPUT_DIR}/csv/ctsv_{hk.replace('-', '_')}.csv", "w", newline="", 
encoding="utf-8") as f: 
w = csv.DictWriter(f, fieldnames=csv_fields); w.writeheader(); w.writerows(records) 
with open(f"{OUTPUT_DIR}/csv/ctsv_all.csv", "w", newline="", encoding="utf-8") as f: 
w = csv.DictWriter(f, fieldnames=csv_fields); w.writeheader() 
for hk in sorted(hk_groups_csv.keys()): 
w.writerows(hk_groups_csv[hk]) 
print(f"     -> {len(hk_groups_csv)} files + 1 tổng | {len(all_csv_records):,} records") 
print(f"[11] Xuất JSON (Nguồn 3)...") 
hk_groups_api = {} 
for rec in all_api_records: 
hk_groups_api.setdefault(rec["hoc_ky"], []).append(rec) 
for hk, records in sorted(hk_groups_api.items()): 
with open(f"{OUTPUT_DIR}/api_json/taichinh_{hk.replace('-', '_')}.json", "w", 
encoding="utf-8") as f: 
json.dump(records, f, ensure_ascii=False, indent=2) 
with open(f"{OUTPUT_DIR}/api_json/taichinh_all.json", "w", encoding="utf-8") as f: 
json.dump(all_api_records, f, ensure_ascii=False, indent=2) 
print(f"     
# -> {len(hk_groups_api)} files + 1 tổng | {len(all_api_records):,} records") 
═══════════════════════════════════════════════════════
════ 
# TỔNG KẾT 
# 
═══════════════════════════════════════════════════════
════ 
print() 
print("=" * 70) 
print("  HOÀN THÀNH — 3 NGÀNH PTIT (MÔN ĐẠI CƯƠNG DÙNG CHUNG)") 
print(f"  Sinh viên    : {total_sv_created:,}") 
print(f"  Đăng ký HP   : {total_dk_created:,}") 
print(f"  Điểm        
: {total_diem_created:,}") 
print(f"  GPA          
: {len(th_buf):,} SV") 
print(f"  Học phần     : {len(global_hp_inserted)} (có môn dùng chung)") 
print(f"  CSV records  : {len(all_csv_records):,}") 
print(f"  API records  : {len(all_api_records):,}") 
print() 
print("  MÔN DÙNG CHUNG:") 
dc_count = sum(1 for m in global_hp_inserted if m.startswith("DC")) 
print(f"    {dc_count} môn đại cương DC* dùng chung giữa các ngành") 
print() 
print("  VERIFY môn chung:") 
print("  SELECT hp.ma_hoc_phan, hp.ten_mon, COUNT(DISTINCT s.ma_nganh) AS 
so_nganh") 
print("  FROM dang_ky_hoc_phan dk") 
print("  JOIN sinh_vien s ON dk.ma_sinh_vien = s.ma_sinh_vien") 
print("  JOIN hoc_phan hp ON dk.ma_hoc_phan = hp.ma_hoc_phan") 
print("  WHERE hp.ma_hoc_phan LIKE 'DC%'") 
print("  GROUP BY 1,2 HAVING COUNT(DISTINCT s.ma_nganh) > 1") 
print("  ORDER BY so_nganh DESC, 1;") 
print() 
print("  VERIFY tổng TC:") 
print("  SELECT s.ma_nganh, s.khoa_hoc, s.trang_thai_hoc_tap, t.tong_tin_chi, 
COUNT(*)") 
print("  FROM tong_hop_ket_qua t JOIN sinh_vien s ON t.ma_sinh_vien = s.ma_sinh_vien") 
print("  GROUP BY 1,2,3,4 ORDER BY 1,2,3;") 
print("=" * 70) 
if __name__ == "__main__": 
main() 