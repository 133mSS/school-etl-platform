-- =============================================
-- DATA WAREHOUSE - DIMENSION TABLES
-- Star Schema Design | Version: 1.0
-- Auto-run by postgres-warehouse container init
-- User: warehouse_user | DB: school_warehouse | Port: 5435
-- =============================================

-- =============================================
-- 1. DIM_DATE
-- Pre-populated trong populate_dim_date.sql
-- =============================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key          INT PRIMARY KEY,         -- Format: YYYYMMDD
    full_date         DATE NOT NULL UNIQUE,
    day_of_week       INT NOT NULL,            -- 1=Monday, 7=Sunday
    day_name          VARCHAR(20) NOT NULL,
    day_of_month      INT NOT NULL,
    day_of_year       INT NOT NULL,
    week_of_year      INT NOT NULL,
    month_num         INT NOT NULL,
    month_name        VARCHAR(20) NOT NULL,
    quarter           INT NOT NULL,
    year              INT NOT NULL,
    is_weekend        BOOLEAN NOT NULL,
    academic_year     VARCHAR(10),             -- e.g. '2023-2024'
    academic_term     VARCHAR(20)              -- e.g. 'HK1 2023-2024'
);

COMMENT ON TABLE dim_date IS 'Date dimension - Chiều thời gian (2020-2029)';

-- =============================================
-- 2. DIM_SINH_VIEN (SCD Type 2)
-- Source: sinh_vien JOIN lop_hanh_chinh JOIN khoa JOIN giang_vien
-- =============================================
CREATE TABLE IF NOT EXISTS dim_sinh_vien (
    sinh_vien_key         SERIAL PRIMARY KEY,
    ma_sinh_vien          VARCHAR(20) NOT NULL,   -- Natural key từ source

    -- Thông tin cá nhân
    ho                    VARCHAR(50),
    ten                   VARCHAR(50),
    ho_ten                VARCHAR(100) NOT NULL,
    ngay_sinh             DATE,
    gioi_tinh             VARCHAR(10),
    email                 VARCHAR(100),

    -- Thông tin học tập
    khoa_hoc              VARCHAR(10),             -- B21, B22, B23, B24
    he_dao_tao            VARCHAR(50),
    ma_khoa               VARCHAR(10),
    ten_khoa              VARCHAR(200),
    ma_lop                VARCHAR(20),
    ten_lop               VARCHAR(50),
    trang_thai_hoc_tap    VARCHAR(30),
    hoc_ky_hien_tai       INT,

    -- Cố vấn học tập
    ma_co_van             VARCHAR(20),
    ten_co_van            VARCHAR(100),

    -- SCD Type 2
    ngay_hieu_luc         DATE NOT NULL DEFAULT CURRENT_DATE,
    ngay_het_hieu_luc     DATE,                    -- NULL = đang hiệu lực
    la_ban_hien_tai       BOOLEAN NOT NULL DEFAULT TRUE,
    phien_ban             INT NOT NULL DEFAULT 1,

    ngay_tao              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_sinh_vien IS 'Student dimension (SCD Type 2) - lưu lịch sử thay đổi trạng thái';
COMMENT ON COLUMN dim_sinh_vien.la_ban_hien_tai IS 'TRUE = bản ghi hiện tại, FALSE = lịch sử';

CREATE INDEX IF NOT EXISTS idx_dim_sv_ma         ON dim_sinh_vien(ma_sinh_vien);
CREATE INDEX IF NOT EXISTS idx_dim_sv_hien_tai   ON dim_sinh_vien(la_ban_hien_tai);
CREATE INDEX IF NOT EXISTS idx_dim_sv_khoa_hoc   ON dim_sinh_vien(khoa_hoc);
CREATE INDEX IF NOT EXISTS idx_dim_sv_trang_thai ON dim_sinh_vien(trang_thai_hoc_tap);

-- =============================================
-- 3. DIM_HOC_PHAN
-- Source: hoc_phan JOIN khoa
-- =============================================
CREATE TABLE IF NOT EXISTS dim_hoc_phan (
    hoc_phan_key      SERIAL PRIMARY KEY,
    ma_hoc_phan       VARCHAR(20) NOT NULL UNIQUE,
    ma_mon            VARCHAR(10),
    ten_mon           VARCHAR(200) NOT NULL,
    so_tin_chi        INT,
    so_gio_ly_thuyet  INT,
    so_gio_thuc_hanh  INT,
    hoc_ky_de_xuat    INT,
    bat_buoc          BOOLEAN,
    loai_mon          VARCHAR(50),                   -- 'Lý thuyết' / 'Thực hành' / 'Kết hợp'
    ma_khoa           VARCHAR(10),
    ten_khoa          VARCHAR(200),
    ngay_tao          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_hoc_phan IS 'Course dimension - Chiều học phần';

CREATE INDEX IF NOT EXISTS idx_dim_hp_ma   ON dim_hoc_phan(ma_hoc_phan);
CREATE INDEX IF NOT EXISTS idx_dim_hp_khoa ON dim_hoc_phan(ma_khoa);
CREATE INDEX IF NOT EXISTS idx_dim_hp_hk   ON dim_hoc_phan(hoc_ky_de_xuat);

-- =============================================
-- 4. DIM_GIANG_VIEN
-- Source: giang_vien JOIN khoa JOIN co_so
-- =============================================
CREATE TABLE IF NOT EXISTS dim_giang_vien (
    giang_vien_key        SERIAL PRIMARY KEY,
    ma_giang_vien         VARCHAR(20) NOT NULL UNIQUE,
    ho                    VARCHAR(50),
    ten                   VARCHAR(50),
    ho_ten                VARCHAR(100) NOT NULL,
    email                 VARCHAR(100),
    chuc_danh             VARCHAR(50),                   -- ThS, TS, PGS.TS, GS.TS
    trang_thai_cong_tac   VARCHAR(20),
    ma_khoa               VARCHAR(10),
    ten_khoa              VARCHAR(200),
    ma_co_so              VARCHAR(10),
    ten_co_so             VARCHAR(200),
    ngay_tao              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_giang_vien IS 'Instructor dimension - Chiều giảng viên';

CREATE INDEX IF NOT EXISTS idx_dim_gv_ma   ON dim_giang_vien(ma_giang_vien);
CREATE INDEX IF NOT EXISTS idx_dim_gv_khoa ON dim_giang_vien(ma_khoa);

-- =============================================
-- 5. DIM_HOC_KY
-- Source: hoc_ky_nam_hoc
-- =============================================
CREATE TABLE IF NOT EXISTS dim_hoc_ky (
    hoc_ky_key    SERIAL PRIMARY KEY,
    ma_hoc_ky     VARCHAR(20) NOT NULL UNIQUE,   -- e.g. 'HK1-2023-2024'
    nam_hoc       VARCHAR(10) NOT NULL,           -- '2023-2024'
    hoc_ky        VARCHAR(10) NOT NULL,           -- 'HK1', 'HK2', 'HK3'
    hoc_ky_so     INT,                            -- 1, 2, 3
    ngay_bat_dau  DATE,
    ngay_ket_thuc DATE,
    nam_bat_dau   INT,
    nam_ket_thuc  INT,
    ngay_tao      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_hoc_ky IS 'Academic term dimension - Chiều học kỳ năm học';

CREATE INDEX IF NOT EXISTS idx_dim_hk_ma      ON dim_hoc_ky(ma_hoc_ky);
CREATE INDEX IF NOT EXISTS idx_dim_hk_nam_hoc ON dim_hoc_ky(nam_hoc);

-- =============================================
-- SUCCESS
-- =============================================
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ create_dimensions.sql DONE';
    RAISE NOTICE '   Tables: dim_date, dim_sinh_vien,';
    RAISE NOTICE '   dim_hoc_phan, dim_giang_vien, dim_hoc_ky';
    RAISE NOTICE '========================================';
END $$;