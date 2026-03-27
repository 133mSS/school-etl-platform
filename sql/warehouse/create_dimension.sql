-- =============================================
-- DATA WAREHOUSE - DIMENSION TABLES
-- Version: 2.0 | Đồng bộ source_models.py v2.0
-- + Thêm: fact_ctsv, fact_tai_chinh (nguồn 2 & 3)
-- =============================================

DROP TABLE IF EXISTS fact_tai_chinh       CASCADE;
DROP TABLE IF EXISTS fact_ctsv            CASCADE;
DROP TABLE IF EXISTS fact_hoc_tap         CASCADE;
DROP TABLE IF EXISTS fact_dang_ky         CASCADE;
DROP TABLE IF EXISTS agg_student_summary  CASCADE;
DROP TABLE IF EXISTS dim_sinh_vien        CASCADE;
DROP TABLE IF EXISTS dim_giang_vien       CASCADE;
DROP TABLE IF EXISTS dim_hoc_phan         CASCADE;
DROP TABLE IF EXISTS dim_hoc_ky           CASCADE;
DROP TABLE IF EXISTS dim_date             CASCADE;

-- =============================================
-- 1. DIM_DATE
-- =============================================
CREATE TABLE dim_date (
    date_key      INT PRIMARY KEY,
    full_date     DATE NOT NULL UNIQUE,
    day_of_week   INT NOT NULL,
    day_name      VARCHAR(20) NOT NULL,
    day_of_month  INT NOT NULL,
    day_of_year   INT NOT NULL,
    week_of_year  INT NOT NULL,
    month_num     INT NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    quarter       INT NOT NULL,
    year          INT NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    academic_year VARCHAR(50),
    academic_term VARCHAR(20)
);
COMMENT ON TABLE dim_date IS 'Chiều thời gian';

-- =============================================
-- 2. DIM_SINH_VIEN (SCD Type 2)
-- v2.0: bỏ so_dien_thoai, dia_chi, he_dao_tao,
--       ngay_nhap_hoc, hoc_ky_hien_tai
-- Giữ: ma_nganh, ten_nganh, ma_co_van lấy từ lop
-- =============================================
CREATE TABLE dim_sinh_vien (
    sinh_vien_key      SERIAL PRIMARY KEY,
    ma_sinh_vien       VARCHAR(20) NOT NULL,

    -- Thông tin cá nhân
    ho                 VARCHAR(50),
    ten                VARCHAR(50),
    ho_ten             VARCHAR(100) NOT NULL,
    ngay_sinh          DATE,
    gioi_tinh          VARCHAR(10),
    email              VARCHAR(100),

    -- Học tập
    khoa_hoc           VARCHAR(10),
    trang_thai_hoc_tap VARCHAR(30),

    -- Ngành (v2.0)
    ma_nganh           VARCHAR(20),
    ten_nganh          VARCHAR(200),

    -- Khoa
    ma_khoa            VARCHAR(10),
    ten_khoa           VARCHAR(200),

    -- Lớp
    ma_lop             VARCHAR(20),
    ten_lop            VARCHAR(100),

    -- Cố vấn học tập (lấy từ lop_hanh_chinh)
    ma_co_van          VARCHAR(20),
    ten_co_van         VARCHAR(100),

    -- SCD Type 2
    ngay_hieu_luc      DATE NOT NULL DEFAULT CURRENT_DATE,
    ngay_het_hieu_luc  DATE,
    la_ban_hien_tai    BOOLEAN NOT NULL DEFAULT TRUE,
    phien_ban          INT NOT NULL DEFAULT 1,
    ngay_tao           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE dim_sinh_vien IS 'Student dimension (SCD Type 2)';

CREATE INDEX idx_dim_sv_ma         ON dim_sinh_vien(ma_sinh_vien);
CREATE INDEX idx_dim_sv_hien_tai   ON dim_sinh_vien(la_ban_hien_tai);
CREATE INDEX idx_dim_sv_nganh      ON dim_sinh_vien(ma_nganh);
CREATE INDEX idx_dim_sv_lop        ON dim_sinh_vien(ma_lop);
CREATE INDEX idx_dim_sv_khoa_hoc   ON dim_sinh_vien(khoa_hoc);
CREATE INDEX idx_dim_sv_trang_thai ON dim_sinh_vien(trang_thai_hoc_tap);

-- =============================================
-- 3. DIM_HOC_PHAN
-- =============================================
CREATE TABLE dim_hoc_phan (
    hoc_phan_key     SERIAL PRIMARY KEY,
    ma_hoc_phan      VARCHAR(20) NOT NULL UNIQUE,
    ma_mon           VARCHAR(10),
    ten_mon          VARCHAR(200) NOT NULL,
    so_tin_chi       INT,
    so_gio_ly_thuyet INT DEFAULT 0,
    so_gio_thuc_hanh INT DEFAULT 0,
    hoc_ky_de_xuat   INT,
    bat_buoc         BOOLEAN DEFAULT TRUE,
    loai_hoc_phan    VARCHAR(50),
    ma_khoa          VARCHAR(10),
    ten_khoa         VARCHAR(200),
    ngay_tao         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE dim_hoc_phan IS 'Course dimension';

CREATE INDEX idx_dim_hp_ma     ON dim_hoc_phan(ma_hoc_phan);
CREATE INDEX idx_dim_hp_loai   ON dim_hoc_phan(loai_hoc_phan);
CREATE INDEX idx_dim_hp_hoc_ky ON dim_hoc_phan(hoc_ky_de_xuat);
CREATE INDEX idx_dim_hp_khoa   ON dim_hoc_phan(ma_khoa);

-- =============================================
-- 4. DIM_GIANG_VIEN
-- =============================================
CREATE TABLE dim_giang_vien (
    giang_vien_key      SERIAL PRIMARY KEY,
    ma_giang_vien       VARCHAR(20) NOT NULL UNIQUE,
    ho                  VARCHAR(50),
    ten                 VARCHAR(50),
    ho_ten              VARCHAR(100) NOT NULL,
    email               VARCHAR(100),
    so_dien_thoai       VARCHAR(15),
    chuc_danh           VARCHAR(50),
    trang_thai_cong_tac VARCHAR(20),
    ma_khoa             VARCHAR(10),
    ten_khoa            VARCHAR(200),
    ngay_tao            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE dim_giang_vien IS 'Instructor dimension';

CREATE INDEX idx_dim_gv_ma         ON dim_giang_vien(ma_giang_vien);
CREATE INDEX idx_dim_gv_khoa       ON dim_giang_vien(ma_khoa);
CREATE INDEX idx_dim_gv_trang_thai ON dim_giang_vien(trang_thai_cong_tac);

-- =============================================
-- 5. DIM_HOC_KY
-- =============================================
CREATE TABLE dim_hoc_ky (
    hoc_ky_key    SERIAL PRIMARY KEY,
    ma_hoc_ky     VARCHAR(50) NOT NULL UNIQUE,
    nam_hoc       VARCHAR(50) NOT NULL,
    hoc_ky        VARCHAR(50) NOT NULL,
    ngay_bat_dau  DATE,
    ngay_ket_thuc DATE,
    nam_bat_dau   INT,
    nam_ket_thuc  INT,
    ngay_tao      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE dim_hoc_ky IS 'Academic term dimension';

CREATE INDEX idx_dim_hk_ma      ON dim_hoc_ky(ma_hoc_ky);
CREATE INDEX idx_dim_hk_nam_hoc ON dim_hoc_ky(nam_hoc);

DO $$
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE '✅ create_dimension.sql v2.0 DONE';
    RAISE NOTICE '   5 dimensions tạo xong';
    RAISE NOTICE '   dim_sinh_vien: bỏ he_dao_tao, hoc_ky_hien_tai';
    RAISE NOTICE '   dim_hoc_ky   : dùng hoc_ky (không phải hoc_ky_ten)';
    RAISE NOTICE '==========================================';
END $$;