-- =============================================
-- SCHOOL SOURCE DATABASE SCHEMA
-- Operational database (OLTP)
-- Version: 2.0
-- Hierarchy:  Khoa -> Nganh -> Lop -> SinhVien
-- =============================================
-- =============================================
-- 2. KHOA (FACULTY)
-- =============================================
CREATE TABLE khoa (
    ma_khoa  VARCHAR(10)  PRIMARY KEY,
    ten_khoa VARCHAR(200) NOT NULL,
    ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE khoa IS 'Faculty/Department - Khoa dao tao';

-- =============================================
-- 3. NGANH (MAJOR)  <-- MOI so voi v1.0
-- Nam giua Khoa va Lop hanh chinh
-- Vi du: Cong nghe thong tin, Ky thuat du lieu
-- =============================================
CREATE TABLE nganh (
    ma_nganh  VARCHAR(20)  PRIMARY KEY,
    ten_nganh VARCHAR(200) NOT NULL,
    ma_khoa   VARCHAR(10)  REFERENCES khoa(ma_khoa),
    ngay_tao  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE nganh IS 'Major - Nganh dao tao';

-- =============================================
-- 4. GIANG_VIEN (INSTRUCTOR)
-- =============================================
CREATE TABLE giang_vien (
    ma_giang_vien       VARCHAR(20)  PRIMARY KEY,
    ho                  VARCHAR(50)  NOT NULL,
    ten                 VARCHAR(50)  NOT NULL,
    email               VARCHAR(100) UNIQUE NOT NULL,
    so_dien_thoai       VARCHAR(15),
    chuc_danh           VARCHAR(50),
    trang_thai_cong_tac VARCHAR(20)  DEFAULT 'Dang cong tac',
    ngay_tuyen_dung     DATE,
    ma_khoa             VARCHAR(10)  REFERENCES khoa(ma_khoa),
    ngay_tao            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ngay_cap_nhat       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE giang_vien IS 'Instructors - Giang vien';

-- =============================================
-- 5. LOP_HANH_CHINH (ADMINISTRATIVE CLASS)
-- v2.0: FK vao NGANH thay vi KHOA
-- Vi du: D21CQCN01-B
-- =============================================
CREATE TABLE lop_hanh_chinh (
    ma_lop    VARCHAR(20)  PRIMARY KEY,
    ten_lop   VARCHAR(100) NOT NULL,
    khoa_hoc  VARCHAR(10)  NOT NULL,
    ma_nganh  VARCHAR(20)  REFERENCES nganh(ma_nganh),   -- thay ma_khoa
    ma_co_van VARCHAR(20)  REFERENCES giang_vien(ma_giang_vien),
    ngay_tao  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE lop_hanh_chinh IS 'Administrative class - Lop hanh chinh';

-- =============================================
-- 6. SINH_VIEN (STUDENT)
-- v2.0: Don gian hoa, giu lai nhung gi can thiet
--   Co  : ho, ten, ngay_sinh, gioi_tinh, email,
--          ma_nganh, ma_lop, khoa_hoc, trang_thai_hoc_tap
--   Bo  : cccd, so_dien_thoai, dia_chi, thanh_pho, he_dao_tao,
--          ngay_nhap_hoc, hoc_ky_hien_tai, ma_co_van, ma_khoa
-- =============================================
CREATE TABLE sinh_vien (
    ma_sinh_vien       VARCHAR(20)  PRIMARY KEY,
    ho                 VARCHAR(50)  NOT NULL,
    ten                VARCHAR(50)  NOT NULL,
    ngay_sinh          DATE         NOT NULL,
    gioi_tinh          VARCHAR(10)  CHECK (gioi_tinh IN ('Nam', 'Nu', 'Khac')),
    email              VARCHAR(100) UNIQUE NOT NULL,

    -- Academic info
    ma_nganh           VARCHAR(20)  REFERENCES nganh(ma_nganh),
    ma_lop             VARCHAR(20)  REFERENCES lop_hanh_chinh(ma_lop),
    khoa_hoc           VARCHAR(10)  NOT NULL,
    trang_thai_hoc_tap VARCHAR(30)  DEFAULT 'Dang hoc',

    ngay_tao           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_sinh_vien_tuoi CHECK (ngay_sinh <= CURRENT_DATE - INTERVAL '15 years')
);

COMMENT ON TABLE sinh_vien IS 'Students - Sinh vien';
COMMENT ON COLUMN sinh_vien.khoa_hoc IS 'Khoa nhap hoc: B21, B22, B23, B24';
COMMENT ON COLUMN sinh_vien.trang_thai_hoc_tap IS 'Dang hoc / Bao luu / Thoi hoc / Tot nghiep';

-- =============================================
-- 7. HOC_PHAN (COURSE)
-- =============================================
CREATE TABLE hoc_phan (
    ma_hoc_phan      VARCHAR(20)  PRIMARY KEY,
    ma_mon           VARCHAR(10)  UNIQUE NOT NULL,
    ten_mon          VARCHAR(200) NOT NULL,
    so_tin_chi       INT NOT NULL CHECK (so_tin_chi > 0 AND so_tin_chi <= 12),
    so_gio_ly_thuyet INT DEFAULT 0,
    so_gio_thuc_hanh INT DEFAULT 0,
    hoc_ky_de_xuat   INT CHECK (hoc_ky_de_xuat BETWEEN 1 AND 12),
    bat_buoc         BOOLEAN DEFAULT TRUE,
    ma_khoa          VARCHAR(10)  REFERENCES khoa(ma_khoa),
    ngay_tao         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE hoc_phan IS 'Courses - Hoc phan/Mon hoc';

-- =============================================
-- 8. HOC_KY_NAM_HOC (ACADEMIC TERM)
-- v2.0: ma_hoc_ky VARCHAR(50) thay vi VARCHAR(20)
--       Vi du: "HK1-2021-22", "HK2-2024-25"
-- =============================================
CREATE TABLE hoc_ky_nam_hoc (
    ma_hoc_ky     VARCHAR(50) PRIMARY KEY,   -- tang len 50
    nam_hoc       VARCHAR(50) NOT NULL,
    hoc_ky        VARCHAR(50) NOT NULL,
    ngay_bat_dau  DATE,
    ngay_ket_thuc DATE
);

COMMENT ON TABLE hoc_ky_nam_hoc IS 'Academic terms - Hoc ky nam hoc';

-- =============================================
-- 9. DANG_KY_HOC_PHAN (ENROLLMENT)
-- =============================================
CREATE TABLE dang_ky_hoc_phan (
    ma_dang_ky    SERIAL      PRIMARY KEY,
    ma_sinh_vien  VARCHAR(20) REFERENCES sinh_vien(ma_sinh_vien),
    ma_hoc_phan   VARCHAR(20) REFERENCES hoc_phan(ma_hoc_phan),
    ma_hoc_ky     VARCHAR(50) REFERENCES hoc_ky_nam_hoc(ma_hoc_ky),  -- VARCHAR(50)
    ma_giang_vien VARCHAR(20) REFERENCES giang_vien(ma_giang_vien),
    ngay_dang_ky  DATE        DEFAULT CURRENT_DATE,
    trang_thai    VARCHAR(30) DEFAULT 'Da dang ky',

    UNIQUE (ma_sinh_vien, ma_hoc_phan, ma_hoc_ky)
);

COMMENT ON TABLE dang_ky_hoc_phan IS 'Course enrollments - Dang ky hoc phan';

-- =============================================
-- 10. DIEM_HOC_PHAN (GRADES)
-- =============================================
CREATE TABLE diem_hoc_phan (
    ma_diem         SERIAL PRIMARY KEY,
    ma_dang_ky      INT REFERENCES dang_ky_hoc_phan(ma_dang_ky),

    diem_chuyen_can DECIMAL(4,2) CHECK (diem_chuyen_can BETWEEN 0 AND 10),
    diem_bai_tap    DECIMAL(4,2) CHECK (diem_bai_tap    BETWEEN 0 AND 10),
    diem_giua_ky    DECIMAL(4,2) CHECK (diem_giua_ky    BETWEEN 0 AND 10),
    diem_cuoi_ky    DECIMAL(4,2) CHECK (diem_cuoi_ky    BETWEEN 0 AND 10),
    diem_tong_ket   DECIMAL(4,2) CHECK (diem_tong_ket   BETWEEN 0 AND 10),

    diem_chu        VARCHAR(2),
    diem_he_4       DECIMAL(3,2),
    dat_mon         BOOLEAN,
    hoc_lai         BOOLEAN DEFAULT FALSE,

    ngay_cham       TIMESTAMP,
    ngay_tao        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (ma_dang_ky)
);

COMMENT ON TABLE diem_hoc_phan IS 'Course grades - Diem hoc phan';
COMMENT ON COLUMN diem_hoc_phan.diem_chu IS 'Letter grade: A+/A/B+/B/C+/C/D+/D/F';

-- =============================================
-- 11. TONG_HOP_KET_QUA (ACADEMIC SUMMARY)
-- =============================================
CREATE TABLE tong_hop_ket_qua (
    ma_sinh_vien     VARCHAR(20) PRIMARY KEY REFERENCES sinh_vien(ma_sinh_vien),
    tong_tin_chi     INT DEFAULT 0,
    tin_chi_tich_luy INT DEFAULT 0,
    gpa_he_10        DECIMAL(4,2),
    gpa_he_4         DECIMAL(3,2),
    canh_bao_hoc_vu  BOOLEAN DEFAULT FALSE,
    ngay_cap_nhat    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE tong_hop_ket_qua IS 'Academic summary - Tong hop ket qua';

-- =============================================
-- INDEXES FOR PERFORMANCE
-- =============================================

-- nganh
CREATE INDEX idx_nganh_khoa      ON nganh(ma_khoa);

-- lop_hanh_chinh
CREATE INDEX idx_lop_nganh       ON lop_hanh_chinh(ma_nganh);
CREATE INDEX idx_lop_khoa_hoc    ON lop_hanh_chinh(khoa_hoc);

-- sinh_vien
CREATE INDEX idx_sv_nganh        ON sinh_vien(ma_nganh);
CREATE INDEX idx_sv_lop          ON sinh_vien(ma_lop);
CREATE INDEX idx_sv_khoa_hoc     ON sinh_vien(khoa_hoc);
CREATE INDEX idx_sv_trang_thai   ON sinh_vien(trang_thai_hoc_tap);
CREATE INDEX idx_sv_email        ON sinh_vien(email);

-- giang_vien
CREATE INDEX idx_gv_khoa         ON giang_vien(ma_khoa);


-- hoc_phan
CREATE INDEX idx_hp_khoa         ON hoc_phan(ma_khoa);
CREATE INDEX idx_hp_hoc_ky       ON hoc_phan(hoc_ky_de_xuat);

-- dang_ky_hoc_phan
CREATE INDEX idx_dk_sinh_vien    ON dang_ky_hoc_phan(ma_sinh_vien);
CREATE INDEX idx_dk_hoc_phan     ON dang_ky_hoc_phan(ma_hoc_phan);
CREATE INDEX idx_dk_hoc_ky       ON dang_ky_hoc_phan(ma_hoc_ky);
CREATE INDEX idx_dk_trang_thai   ON dang_ky_hoc_phan(trang_thai);
CREATE INDEX idx_dk_giang_vien   ON dang_ky_hoc_phan(ma_giang_vien);

-- diem_hoc_phan
CREATE INDEX idx_diem_dang_ky    ON diem_hoc_phan(ma_dang_ky);
CREATE INDEX idx_diem_dat_mon    ON diem_hoc_phan(dat_mon);
CREATE INDEX idx_diem_chu        ON diem_hoc_phan(diem_chu);
CREATE INDEX idx_diem_hoc_lai    ON diem_hoc_phan(hoc_lai);

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

CREATE TRIGGER trg_giang_vien_cap_nhat
BEFORE UPDATE ON giang_vien
FOR EACH ROW
EXECUTE FUNCTION cap_nhat_ngay_sua_doi();

CREATE TRIGGER trg_tong_hop_cap_nhat
BEFORE UPDATE ON tong_hop_ket_qua
FOR EACH ROW
EXECUTE FUNCTION cap_nhat_ngay_sua_doi();

-- =============================================
-- SUCCESS
-- =============================================
DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Schema v2.0 - DONE!';
    RAISE NOTICE 'Hierarchy:  Khoa -> Nganh -> Lop -> SV';
    RAISE NOTICE 'Tables:';
    RAISE NOTICE '  2.  khoa';
    RAISE NOTICE '  3.  nganh         (MOI)';
    RAISE NOTICE '  4.  giang_vien';
    RAISE NOTICE '  5.  lop_hanh_chinh (FK -> nganh)';
    RAISE NOTICE '  6.  sinh_vien      (ma_nganh, don gian hoa)';
    RAISE NOTICE '  7.  hoc_phan';
    RAISE NOTICE '  8.  hoc_ky_nam_hoc (VARCHAR(50))';
    RAISE NOTICE '  9.  dang_ky_hoc_phan';
    RAISE NOTICE '  10. diem_hoc_phan';
    RAISE NOTICE '  11. tong_hop_ket_qua';
    RAISE NOTICE '20 indexes | 2 triggers';
    RAISE NOTICE '============================================';
END $$;