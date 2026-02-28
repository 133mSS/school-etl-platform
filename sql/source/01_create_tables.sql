-- =============================================
-- SCHOOL SOURCE DATABASE SCHEMA (SIMPLIFIED)
-- Operational database (OLTP)
-- Version: 1.0 - Production Ready (7 tuần)
-- =============================================
-- =============================================
-- 1. CƠ SỞ (CAMPUS)
-- =============================================
CREATE TABLE co_so (
    ma_co_so VARCHAR(10) PRIMARY KEY,
    ten_co_so VARCHAR(200) NOT NULL,
    dia_chi TEXT,
    thanh_pho VARCHAR(100) NOT NULL,
    ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE co_so IS 'Campus/Branch - Cơ sở đào tạo';

-- =============================================
-- 2. KHOA (DEPARTMENT)
-- =============================================
CREATE TABLE khoa (
    ma_khoa VARCHAR(10) PRIMARY KEY,
    ten_khoa VARCHAR(200) NOT NULL,
    ma_co_so VARCHAR(10) REFERENCES co_so(ma_co_so),
    ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE khoa IS 'Department/Faculty - Khoa đào tạo';

-- =============================================
-- 3. GIẢNG VIÊN (INSTRUCTOR)
-- =============================================
CREATE TABLE giang_vien (
    ma_giang_vien VARCHAR(20) PRIMARY KEY,
    ho VARCHAR(50) NOT NULL,
    ten VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    so_dien_thoai VARCHAR(15),
    chuc_danh VARCHAR(50),
    trang_thai_cong_tac VARCHAR(20) DEFAULT 'Đang công tác',
    ngay_tuyen_dung DATE,
    ma_khoa VARCHAR(10) REFERENCES khoa(ma_khoa),
    ma_co_so VARCHAR(10) REFERENCES co_so(ma_co_so),
    ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ngay_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE giang_vien IS 'Instructors - Giảng viên';

-- =============================================
-- 4. LỚP HÀNH CHÍNH (ADMINISTRATIVE CLASS)
-- =============================================
CREATE TABLE lop_hanh_chinh (
    ma_lop VARCHAR(20) PRIMARY KEY,
    ten_lop VARCHAR(50) NOT NULL,
    khoa_hoc VARCHAR(10) NOT NULL,
    ma_khoa VARCHAR(10) REFERENCES khoa(ma_khoa),
    ma_co_van VARCHAR(20) REFERENCES giang_vien(ma_giang_vien),
    ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE lop_hanh_chinh IS 'Administrative class - Lớp hành chính';

-- =============================================
-- 5. SINH VIÊN (STUDENT)
-- =============================================
CREATE TABLE sinh_vien (
    ma_sinh_vien VARCHAR(20) PRIMARY KEY,
    cccd VARCHAR(12) UNIQUE,
    ho VARCHAR(50) NOT NULL,
    ten VARCHAR(50) NOT NULL,
    ngay_sinh DATE NOT NULL,
    gioi_tinh VARCHAR(10) CHECK (gioi_tinh IN ('Nam', 'Nữ', 'Khác')),
    email VARCHAR(100) UNIQUE NOT NULL,
    so_dien_thoai VARCHAR(15),
    dia_chi TEXT,
    thanh_pho VARCHAR(100),

    -- Academic information
    he_dao_tao VARCHAR(50),
    ma_khoa VARCHAR(10) REFERENCES khoa(ma_khoa),
    ma_lop VARCHAR(20) REFERENCES lop_hanh_chinh(ma_lop),
    khoa_hoc VARCHAR(10) NOT NULL,
    ngay_nhap_hoc DATE NOT NULL,
    hoc_ky_hien_tai INT CHECK (hoc_ky_hien_tai BETWEEN 1 AND 12),
    trang_thai_hoc_tap VARCHAR(30) DEFAULT 'Đang học',

    ma_co_van VARCHAR(20) REFERENCES giang_vien(ma_giang_vien),

    ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ngay_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_sinh_vien_tuoi CHECK (ngay_sinh <= CURRENT_DATE - INTERVAL '15 years')
);

COMMENT ON TABLE sinh_vien IS 'Students - Sinh viên';
COMMENT ON COLUMN sinh_vien.trang_thai_hoc_tap IS 'Status: Đang học/Bảo lưu/Thôi học/Tốt nghiệp';

-- =============================================
-- 6. HỌC PHẦN (COURSE)
-- =============================================
CREATE TABLE hoc_phan (
    ma_hoc_phan VARCHAR(20) PRIMARY KEY,
    ma_mon VARCHAR(10) UNIQUE NOT NULL,
    ten_mon VARCHAR(200) NOT NULL,
    so_tin_chi INT NOT NULL CHECK (so_tin_chi > 0 AND so_tin_chi <= 6),
    so_gio_ly_thuyet INT DEFAULT 0,
    so_gio_thuc_hanh INT DEFAULT 0,
    hoc_ky_de_xuat INT CHECK (hoc_ky_de_xuat BETWEEN 1 AND 12),
    bat_buoc BOOLEAN DEFAULT TRUE,
    ma_khoa VARCHAR(10) REFERENCES khoa(ma_khoa),
    ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE hoc_phan IS 'Courses - Học phần/Môn học';

-- =============================================
-- 7. HỌC KỲ NĂM HỌC (ACADEMIC TERM)
-- =============================================
CREATE TABLE hoc_ky_nam_hoc (
    ma_hoc_ky VARCHAR(20) PRIMARY KEY,
    nam_hoc VARCHAR(10) NOT NULL,
    hoc_ky VARCHAR(10) NOT NULL,
    ngay_bat_dau DATE,
    ngay_ket_thuc DATE
);

COMMENT ON TABLE hoc_ky_nam_hoc IS 'Academic terms - Học kỳ năm học';

-- =============================================
-- 8. ĐĂNG KÝ HỌC PHẦN (ENROLLMENT)
-- =============================================
CREATE TABLE dang_ky_hoc_phan (
    ma_dang_ky SERIAL PRIMARY KEY,
    ma_sinh_vien VARCHAR(20) REFERENCES sinh_vien(ma_sinh_vien),
    ma_hoc_phan VARCHAR(20) REFERENCES hoc_phan(ma_hoc_phan),
    ma_hoc_ky VARCHAR(20) REFERENCES hoc_ky_nam_hoc(ma_hoc_ky),
    ma_giang_vien VARCHAR(20) REFERENCES giang_vien(ma_giang_vien),
    ngay_dang_ky DATE DEFAULT CURRENT_DATE,
    trang_thai VARCHAR(30) DEFAULT 'Đã đăng ký',

    UNIQUE(ma_sinh_vien, ma_hoc_phan, ma_hoc_ky)
);

COMMENT ON TABLE dang_ky_hoc_phan IS 'Course enrollments - Đăng ký học phần';

-- =============================================
-- 9. ĐIỂM HỌC PHẦN (GRADES)
-- =============================================
CREATE TABLE diem_hoc_phan (
    ma_diem SERIAL PRIMARY KEY,
    ma_dang_ky INT REFERENCES dang_ky_hoc_phan(ma_dang_ky),

    diem_chuyen_can DECIMAL(4,2) CHECK (diem_chuyen_can BETWEEN 0 AND 10),
    diem_bai_tap DECIMAL(4,2) CHECK (diem_bai_tap BETWEEN 0 AND 10),
    diem_giua_ky DECIMAL(4,2) CHECK (diem_giua_ky BETWEEN 0 AND 10),
    diem_cuoi_ky DECIMAL(4,2) CHECK (diem_cuoi_ky BETWEEN 0 AND 10),
    diem_tong_ket DECIMAL(4,2) CHECK (diem_tong_ket BETWEEN 0 AND 10),

    diem_chu VARCHAR(2),
    diem_he_4 DECIMAL(3,2),
    dat_mon BOOLEAN,
    hoc_lai BOOLEAN DEFAULT FALSE,

    ngay_cham TIMESTAMP,
    ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(ma_dang_ky)
);

COMMENT ON TABLE diem_hoc_phan IS 'Course grades - Điểm học phần';
COMMENT ON COLUMN diem_hoc_phan.diem_chu IS 'Letter grade: A+/A/B+/B/C+/C/D+/D/F';
COMMENT ON COLUMN diem_hoc_phan.diem_chuyen_can IS 'Attendance score (fixed 8-10 random)';

-- =============================================
-- 10. TỔNG HỢP KẾT QUẢ (ACADEMIC SUMMARY)
-- =============================================
CREATE TABLE tong_hop_ket_qua (
    ma_sinh_vien VARCHAR(20) PRIMARY KEY REFERENCES sinh_vien(ma_sinh_vien),
    tong_tin_chi INT DEFAULT 0,
    tin_chi_tich_luy INT DEFAULT 0,
    gpa_he_10 DECIMAL(4,2),
    gpa_he_4 DECIMAL(3,2),
    canh_bao_hoc_vu BOOLEAN DEFAULT FALSE,
    ngay_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE tong_hop_ket_qua IS 'Academic summary - Tổng hợp kết quả';

-- =============================================
-- INDEXES FOR PERFORMANCE
-- =============================================

-- sinh_vien
CREATE INDEX idx_sinh_vien_khoa_hoc ON sinh_vien(khoa_hoc);
CREATE INDEX idx_sinh_vien_ma_khoa ON sinh_vien(ma_khoa);
CREATE INDEX idx_sinh_vien_ma_lop ON sinh_vien(ma_lop);
CREATE INDEX idx_sinh_vien_trang_thai ON sinh_vien(trang_thai_hoc_tap);
CREATE INDEX idx_sinh_vien_co_van ON sinh_vien(ma_co_van);
CREATE INDEX idx_sinh_vien_email ON sinh_vien(email);

-- dang_ky_hoc_phan
CREATE INDEX idx_dang_ky_sinh_vien ON dang_ky_hoc_phan(ma_sinh_vien);
CREATE INDEX idx_dang_ky_hoc_phan ON dang_ky_hoc_phan(ma_hoc_phan);
CREATE INDEX idx_dang_ky_hoc_ky ON dang_ky_hoc_phan(ma_hoc_ky);
CREATE INDEX idx_dang_ky_trang_thai ON dang_ky_hoc_phan(trang_thai);
CREATE INDEX idx_dang_ky_giang_vien ON dang_ky_hoc_phan(ma_giang_vien);

-- diem_hoc_phan
CREATE INDEX idx_diem_dang_ky ON diem_hoc_phan(ma_dang_ky);
CREATE INDEX idx_diem_dat_mon ON diem_hoc_phan(dat_mon);
CREATE INDEX idx_diem_chu ON diem_hoc_phan(diem_chu);

-- giang_vien
CREATE INDEX idx_giang_vien_khoa ON giang_vien(ma_khoa);
CREATE INDEX idx_giang_vien_co_so ON giang_vien(ma_co_so);

-- hoc_phan
CREATE INDEX idx_hoc_phan_khoa ON hoc_phan(ma_khoa);
CREATE INDEX idx_hoc_phan_hoc_ky ON hoc_phan(hoc_ky_de_xuat);

-- lop_hanh_chinh
CREATE INDEX idx_lop_khoa_hoc ON lop_hanh_chinh(khoa_hoc);
CREATE INDEX idx_lop_khoa ON lop_hanh_chinh(ma_khoa);

-- =============================================
-- TRIGGERS FOR AUTO-UPDATE
-- =============================================

CREATE OR REPLACE FUNCTION cap_nhat_ngay_sua_doi()
RETURNS TRIGGER AS $$
BEGIN
    NEW.ngay_cap_nhat = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sinh_vien_cap_nhat 
BEFORE UPDATE ON sinh_vien
FOR EACH ROW 
EXECUTE FUNCTION cap_nhat_ngay_sua_doi();

CREATE TRIGGER trg_giang_vien_cap_nhat 
BEFORE UPDATE ON giang_vien
FOR EACH ROW 
EXECUTE FUNCTION cap_nhat_ngay_sua_doi();

CREATE TRIGGER trg_tong_hop_ket_qua_cap_nhat 
BEFORE UPDATE ON tong_hop_ket_qua
FOR EACH ROW 
EXECUTE FUNCTION cap_nhat_ngay_sua_doi();

-- =============================================
-- SUCCESS MESSAGE
-- =============================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ Source database schema created!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  1. co_so (Campus)';
    RAISE NOTICE '  2. khoa (Department)';
    RAISE NOTICE '  3. giang_vien (Instructor)';
    RAISE NOTICE '  4. lop_hanh_chinh (Admin Class)';
    RAISE NOTICE '  5. sinh_vien (Student)';
    RAISE NOTICE '  6. hoc_phan (Course)';
    RAISE NOTICE '  7. hoc_ky_nam_hoc (Academic Term)';
    RAISE NOTICE '  8. dang_ky_hoc_phan (Enrollment)';
    RAISE NOTICE '  9. diem_hoc_phan (Grades)';
    RAISE NOTICE '  10. tong_hop_ket_qua (Summary)';
    RAISE NOTICE '';
    RAISE NOTICE 'Performance optimizations:';
    RAISE NOTICE '  - 20 indexes created';
    RAISE NOTICE '  - 3 auto-update triggers';
    RAISE NOTICE '';
    RAISE NOTICE 'Ready for data generation! 🚀';
    RAISE NOTICE '========================================';
END $$;