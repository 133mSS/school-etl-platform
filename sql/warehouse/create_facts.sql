-- =============================================
-- DATA WAREHOUSE - FACT & AGGREGATE TABLES
-- Star Schema Design | Version: 1.0
-- Chạy SAU create_dimensions.sql (thứ tự alphabet đảm bảo)
-- User: warehouse_user | DB: school_warehouse | Port: 5435
-- =============================================

-- =============================================
-- FACT 1: FACT_HOC_TAP (Student Performance)
-- Grain: 1 row per student × course × term
-- Source: diem_hoc_phan JOIN dang_ky_hoc_phan
-- =============================================
CREATE TABLE IF NOT EXISTS fact_hoc_tap (
    hoc_tap_key       SERIAL PRIMARY KEY,

    -- Foreign Keys → Dimensions
    sinh_vien_key     INT NOT NULL REFERENCES dim_sinh_vien(sinh_vien_key),
    hoc_phan_key      INT NOT NULL REFERENCES dim_hoc_phan(hoc_phan_key),
    giang_vien_key    INT NOT NULL REFERENCES dim_giang_vien(giang_vien_key),
    hoc_ky_key        INT NOT NULL REFERENCES dim_hoc_ky(hoc_ky_key),
    date_key          INT REFERENCES dim_date(date_key),   -- ngày chấm điểm

    -- Natural keys (trace về source DB)
    ma_sinh_vien      VARCHAR(20) NOT NULL,
    ma_hoc_phan       VARCHAR(20) NOT NULL,
    ma_dang_ky        INT,

    -- Measures: Điểm thành phần
    diem_chuyen_can   DECIMAL(4,2),
    diem_bai_tap      DECIMAL(4,2),
    diem_giua_ky      DECIMAL(4,2),
    diem_cuoi_ky      DECIMAL(4,2),
    diem_tong_ket     DECIMAL(4,2),

    -- Measures: Xếp loại
    diem_chu          VARCHAR(2),                          -- A+/A/B+/B/C+/C/D+/D/F
    diem_he_4         DECIMAL(3,2),

    -- Measures: Flags
    dat_mon           BOOLEAN,
    hoc_lai           BOOLEAN,

    -- Measures: Tính toán
    so_tin_chi        INT,
    diem_chat_luong   DECIMAL(5,2),                       -- diem_he_4 × so_tin_chi

    -- ETL metadata
    ngay_load         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nguon_du_lieu     VARCHAR(50) DEFAULT 'postgresql'
);

COMMENT ON TABLE fact_hoc_tap IS 'Student performance fact - kết quả học tập từng môn';
COMMENT ON COLUMN fact_hoc_tap.diem_chat_luong IS 'Quality points = GPA4 × credits, dùng tính GPA tích lũy';

CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_ht_sv_hp_hk
    ON fact_hoc_tap(ma_sinh_vien, ma_hoc_phan, hoc_ky_key);

CREATE INDEX IF NOT EXISTS idx_fact_ht_sv       ON fact_hoc_tap(sinh_vien_key);
CREATE INDEX IF NOT EXISTS idx_fact_ht_hp       ON fact_hoc_tap(hoc_phan_key);
CREATE INDEX IF NOT EXISTS idx_fact_ht_gv       ON fact_hoc_tap(giang_vien_key);
CREATE INDEX IF NOT EXISTS idx_fact_ht_hk       ON fact_hoc_tap(hoc_ky_key);
CREATE INDEX IF NOT EXISTS idx_fact_ht_date     ON fact_hoc_tap(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_ht_ma_sv    ON fact_hoc_tap(ma_sinh_vien);
CREATE INDEX IF NOT EXISTS idx_fact_ht_dat_mon  ON fact_hoc_tap(dat_mon);
CREATE INDEX IF NOT EXISTS idx_fact_ht_diem_chu ON fact_hoc_tap(diem_chu);

-- =============================================
-- FACT 2: FACT_DANG_KY (Enrollment)
-- Grain: 1 row per enrollment transaction
-- Source: dang_ky_hoc_phan
-- =============================================
CREATE TABLE IF NOT EXISTS fact_dang_ky (
    dang_ky_key       SERIAL PRIMARY KEY,

    -- Foreign Keys → Dimensions
    sinh_vien_key     INT NOT NULL REFERENCES dim_sinh_vien(sinh_vien_key),
    hoc_phan_key      INT NOT NULL REFERENCES dim_hoc_phan(hoc_phan_key),
    giang_vien_key    INT NOT NULL REFERENCES dim_giang_vien(giang_vien_key),
    hoc_ky_key        INT NOT NULL REFERENCES dim_hoc_ky(hoc_ky_key),
    date_key          INT REFERENCES dim_date(date_key),   -- ngày đăng ký

    -- Natural keys
    ma_sinh_vien      VARCHAR(20) NOT NULL,
    ma_hoc_phan       VARCHAR(20) NOT NULL,
    ma_dang_ky        INT,

    -- Measures
    trang_thai        VARCHAR(30),                         -- Đã đăng ký / Đã hủy / Hoàn thành
    so_tin_chi        INT,
    ngay_dang_ky      DATE,

    -- ETL metadata
    ngay_load         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nguon_du_lieu     VARCHAR(50) DEFAULT 'postgresql'
);

COMMENT ON TABLE fact_dang_ky IS 'Enrollment fact - sự kiện đăng ký học phần';

CREATE INDEX IF NOT EXISTS idx_fact_dk_sv    ON fact_dang_ky(sinh_vien_key);
CREATE INDEX IF NOT EXISTS idx_fact_dk_hp    ON fact_dang_ky(hoc_phan_key);
CREATE INDEX IF NOT EXISTS idx_fact_dk_hk    ON fact_dang_ky(hoc_ky_key);
CREATE INDEX IF NOT EXISTS idx_fact_dk_date  ON fact_dang_ky(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_dk_ma_sv ON fact_dang_ky(ma_sinh_vien);
CREATE INDEX IF NOT EXISTS idx_fact_dk_tt    ON fact_dang_ky(trang_thai);

-- =============================================
-- AGGREGATE: AGG_STUDENT_SUMMARY
-- Pre-calculated per student — tối ưu tốc độ Grafana
-- ETL cập nhật sau mỗi pipeline run
-- =============================================
CREATE TABLE IF NOT EXISTS agg_student_summary (
    agg_key               SERIAL PRIMARY KEY,
    sinh_vien_key         INT NOT NULL REFERENCES dim_sinh_vien(sinh_vien_key),
    ma_sinh_vien          VARCHAR(20) NOT NULL UNIQUE,

    -- GPA
    gpa_he_10             DECIMAL(4,2),
    gpa_he_4              DECIMAL(3,2),
    xep_loai_hoc_luc      VARCHAR(30),
    -- Xuất sắc: GPA4 >= 3.6
    -- Giỏi:     GPA4 >= 3.2
    -- Khá:      GPA4 >= 2.5
    -- Trung bình: GPA4 >= 2.0
    -- Yếu:      GPA4 < 2.0

    -- Tín chỉ
    tong_tin_chi_dang_ky  INT DEFAULT 0,
    tin_chi_dat           INT DEFAULT 0,
    tin_chi_khong_dat     INT DEFAULT 0,
    ty_le_dat             DECIMAL(5,2),

    -- Môn học
    tong_mon_dang_ky      INT DEFAULT 0,
    so_mon_dat            INT DEFAULT 0,
    so_mon_khong_dat      INT DEFAULT 0,
    so_mon_hoc_lai        INT DEFAULT 0,

    -- Cảnh báo học vụ
    canh_bao_hoc_vu       BOOLEAN DEFAULT FALSE,
    muc_do_rui_ro         VARCHAR(20),
    -- 'Cao':        GPA4 < 2.0 HOẶC so_mon_khong_dat >= 3
    -- 'Trung bình': GPA4 2.0 – 2.5
    -- 'Thấp':       GPA4 > 2.5

    -- Tiến độ tốt nghiệp
    co_the_tot_nghiep     BOOLEAN DEFAULT FALSE,           -- tin_chi_dat >= 130

    hoc_ky_key_gan_nhat   INT REFERENCES dim_hoc_ky(hoc_ky_key),
    ngay_cap_nhat         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE agg_student_summary IS 'Pre-aggregated per-student summary — dùng cho Grafana dashboard';
COMMENT ON COLUMN agg_student_summary.muc_do_rui_ro IS 'Cao=GPA4<2.0, Trung bình=2.0-2.5, Thấp=>2.5';

CREATE INDEX IF NOT EXISTS idx_agg_ss_ma       ON agg_student_summary(ma_sinh_vien);
CREATE INDEX IF NOT EXISTS idx_agg_ss_rui_ro   ON agg_student_summary(muc_do_rui_ro);
CREATE INDEX IF NOT EXISTS idx_agg_ss_canh_bao ON agg_student_summary(canh_bao_hoc_vu);
CREATE INDEX IF NOT EXISTS idx_agg_ss_xep_loai ON agg_student_summary(xep_loai_hoc_luc);
CREATE INDEX IF NOT EXISTS idx_agg_ss_gpa4     ON agg_student_summary(gpa_he_4);

-- =============================================
-- SUCCESS
-- =============================================
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ create_facts.sql DONE';
    RAISE NOTICE '   Tables: fact_hoc_tap, fact_dang_ky,';
    RAISE NOTICE '   agg_student_summary';
    RAISE NOTICE '========================================';
END $$;