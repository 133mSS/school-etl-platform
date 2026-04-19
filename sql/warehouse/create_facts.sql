-- ============================================================
-- DATA WAREHOUSE - FACT & AGGREGATE TABLES  v2.1
-- ============================================================

-- FACT 1: FACT_HOC_TAP
CREATE TABLE IF NOT EXISTS fact_hoc_tap (
    hoc_tap_key      SERIAL PRIMARY KEY,

    -- Foreign Keys
    sinh_vien_key    INT NOT NULL REFERENCES dim_sinh_vien(sinh_vien_key),
    hoc_phan_key     INT NOT NULL REFERENCES dim_hoc_phan(hoc_phan_key),
    giang_vien_key   INT          REFERENCES dim_giang_vien(giang_vien_key),
    hoc_ky_key       INT NOT NULL REFERENCES dim_hoc_ky(hoc_ky_key),
    -- Natural keys
    ma_sinh_vien     VARCHAR(20) NOT NULL,
    ma_hoc_phan      VARCHAR(20) NOT NULL,
    ma_dang_ky       INT,

    -- Measures
    diem_chuyen_can  DECIMAL(4,2),
    diem_bai_tap     DECIMAL(4,2),
    diem_giua_ky     DECIMAL(4,2),
    diem_cuoi_ky     DECIMAL(4,2),
    diem_tong_ket    DECIMAL(4,2),
    diem_chu         VARCHAR(2),
    diem_he_4        DECIMAL(3,2),
    dat_mon          BOOLEAN,
    hoc_lai          BOOLEAN,
    so_tin_chi       INT,
    diem_chat_luong  DECIMAL(5,2),

    -- ETL metadata
    ngay_load        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nguon_du_lieu    VARCHAR(50) DEFAULT 'postgresql'
);

COMMENT ON TABLE fact_hoc_tap IS
    'Kết quả học tập - Grain: SinhVien × HocPhan × HocKy';

-- FIX: Đổi tên index nhất quán với constraint name trong Python
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_ht_sv_hp_hk
    ON fact_hoc_tap(ma_sinh_vien, ma_hoc_phan, hoc_ky_key);

CREATE INDEX IF NOT EXISTS idx_fact_ht_sv      ON fact_hoc_tap(sinh_vien_key);
CREATE INDEX IF NOT EXISTS idx_fact_ht_hk      ON fact_hoc_tap(hoc_ky_key);
CREATE INDEX IF NOT EXISTS idx_fact_ht_ma_sv   ON fact_hoc_tap(ma_sinh_vien);
CREATE INDEX IF NOT EXISTS idx_fact_ht_dat_mon ON fact_hoc_tap(dat_mon);


-- ============================================================
-- FACT 2: FACT_DANG_KY
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_dang_ky (
    dang_ky_key      SERIAL PRIMARY KEY,

    -- Foreign Keys
    sinh_vien_key    INT NOT NULL REFERENCES dim_sinh_vien(sinh_vien_key),
    hoc_phan_key     INT NOT NULL REFERENCES dim_hoc_phan(hoc_phan_key),
    giang_vien_key   INT          REFERENCES dim_giang_vien(giang_vien_key),
    hoc_ky_key       INT NOT NULL REFERENCES dim_hoc_ky(hoc_ky_key),


    -- Natural keys
    ma_sinh_vien     VARCHAR(20) NOT NULL,
    ma_hoc_phan      VARCHAR(20) NOT NULL,
    ma_dang_ky       INT,

    -- Measures
    trang_thai       VARCHAR(30),
    so_tin_chi       INT,
    ngay_dang_ky     DATE,

    -- ETL metadata
    ngay_load        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nguon_du_lieu    VARCHAR(50) DEFAULT 'postgresql'
);

COMMENT ON TABLE fact_dang_ky IS
    'Đăng ký học phần - Grain: SinhVien × HocPhan × HocKy (lần đăng ký mới nhất)';

-- FIX: THÊM UNIQUE INDEX — bắt buộc cho on_conflict_do_update
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_dk_sv_hp_hk
    ON fact_dang_ky(ma_sinh_vien, ma_hoc_phan, hoc_ky_key);

CREATE INDEX IF NOT EXISTS idx_fact_dk_sv ON fact_dang_ky(sinh_vien_key);
CREATE INDEX IF NOT EXISTS idx_fact_dk_hk ON fact_dang_ky(hoc_ky_key);
CREATE INDEX IF NOT EXISTS idx_fact_dk_ma ON fact_dang_ky(ma_sinh_vien);


-- ============================================================
-- FACT 3: FACT_CTSV
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_ctsv (
    ctsv_key         SERIAL PRIMARY KEY,

    -- Foreign Keys
    sinh_vien_key    INT NOT NULL REFERENCES dim_sinh_vien(sinh_vien_key),
    hoc_ky_key       INT NOT NULL REFERENCES dim_hoc_ky(hoc_ky_key),

    -- Natural keys
    ma_sinh_vien     VARCHAR(20) NOT NULL,
    ma_hoc_ky        VARCHAR(50) NOT NULL,

    -- Measures
    diem_rl          INT,
    xep_loai_rl      VARCHAR(20),
    loai_hoc_bong    VARCHAR(100),
    muc_tien_hb      BIGINT DEFAULT 0,
    hinh_thuc_kl     VARCHAR(100),
    ly_do_kl         VARCHAR(200),
    co_hoc_bong      BOOLEAN DEFAULT FALSE,
    bi_ky_luat       BOOLEAN DEFAULT FALSE,

    -- ETL metadata
    ngay_load        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nguon_du_lieu    VARCHAR(50) DEFAULT 'csv_ctsv'
);

COMMENT ON TABLE fact_ctsv IS
    'Phòng Công tác SV - Grain: SinhVien × HocKy | điểm RL + học bổng + kỷ luật';

-- OK: đã có unique index
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_ctsv_sv_hk
    ON fact_ctsv(ma_sinh_vien, hoc_ky_key);

CREATE INDEX IF NOT EXISTS idx_fact_ctsv_sv ON fact_ctsv(sinh_vien_key);
CREATE INDEX IF NOT EXISTS idx_fact_ctsv_hk ON fact_ctsv(hoc_ky_key);
CREATE INDEX IF NOT EXISTS idx_fact_ctsv_ma ON fact_ctsv(ma_sinh_vien);
CREATE INDEX IF NOT EXISTS idx_fact_ctsv_rl ON fact_ctsv(diem_rl);
CREATE INDEX IF NOT EXISTS idx_fact_ctsv_hb ON fact_ctsv(co_hoc_bong);
CREATE INDEX IF NOT EXISTS idx_fact_ctsv_kl ON fact_ctsv(bi_ky_luat);


-- ============================================================
-- FACT 4: FACT_TAI_CHINH
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_tai_chinh (
    tai_chinh_key     SERIAL PRIMARY KEY,

    -- Foreign Keys
    sinh_vien_key     INT NOT NULL REFERENCES dim_sinh_vien(sinh_vien_key),
    hoc_ky_key        INT NOT NULL REFERENCES dim_hoc_ky(hoc_ky_key),

    -- Natural keys
    ma_sinh_vien      VARCHAR(20) NOT NULL,
    ma_hoc_ky         VARCHAR(50) NOT NULL,

    -- Measures
    hoc_phi_phai_dong BIGINT DEFAULT 0,
    da_dong           BIGINT DEFAULT 0,
    con_no            BIGINT DEFAULT 0,
    duoc_mien_giam    BOOLEAN DEFAULT FALSE,
    ly_do_mien_giam   VARCHAR(100),
    so_tien_mien_giam BIGINT DEFAULT 0,
    ngay_dong_cuoi    DATE,
    con_no_flag       BOOLEAN GENERATED ALWAYS AS (con_no > 0) STORED,

    -- ETL metadata
    ngay_load         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nguon_du_lieu     VARCHAR(50) DEFAULT 'api_portal'
);

COMMENT ON TABLE fact_tai_chinh IS
    'Portal Tài chính - Grain: SinhVien × HocKy | học phí + miễn giảm';
COMMENT ON COLUMN fact_tai_chinh.con_no_flag IS
    'GENERATED ALWAYS — TRUE nếu con_no > 0, không INSERT trực tiếp';

-- OK: đã có unique index
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_tc_sv_hk
    ON fact_tai_chinh(ma_sinh_vien, hoc_ky_key);

CREATE INDEX IF NOT EXISTS idx_fact_tc_sv   ON fact_tai_chinh(sinh_vien_key);
CREATE INDEX IF NOT EXISTS idx_fact_tc_hk   ON fact_tai_chinh(hoc_ky_key);
CREATE INDEX IF NOT EXISTS idx_fact_tc_ma   ON fact_tai_chinh(ma_sinh_vien);
CREATE INDEX IF NOT EXISTS idx_fact_tc_no   ON fact_tai_chinh(con_no_flag);
CREATE INDEX IF NOT EXISTS idx_fact_tc_mien ON fact_tai_chinh(duoc_mien_giam);


-- ============================================================
-- AGGREGATE: AGG_STUDENT_SUMMARY 
-- ============================================================
CREATE TABLE IF NOT EXISTS agg_student_summary (
    agg_key               SERIAL PRIMARY KEY,
    sinh_vien_key         INT NOT NULL REFERENCES dim_sinh_vien(sinh_vien_key),
    ma_sinh_vien          VARCHAR(20) NOT NULL UNIQUE,

    -- Kết quả học tập
    gpa_he_10             DECIMAL(4,2),
    gpa_he_4              DECIMAL(3,2),
    xep_loai_hoc_luc      VARCHAR(30),
    tong_tin_chi_dang_ky  INT DEFAULT 0,
    tin_chi_dat           INT DEFAULT 0,
    tin_chi_khong_dat     INT DEFAULT 0,
    ty_le_dat             DECIMAL(5,2),
    tong_mon_dang_ky      INT DEFAULT 0,
    so_mon_dat            INT DEFAULT 0,
    so_mon_khong_dat      INT DEFAULT 0,
    so_mon_hoc_lai        INT DEFAULT 0,

    -- Rèn luyện
    diem_rl_trung_binh    DECIMAL(4,1),
    xep_loai_rl_gan_nhat  VARCHAR(20),

    -- Tài chính
    tong_no_hoc_phi       BIGINT DEFAULT 0,
    co_no_hoc_phi         BOOLEAN DEFAULT FALSE,
    duoc_mien_giam        BOOLEAN DEFAULT FALSE,

    -- Đánh giá rủi ro
    muc_do_rui_ro         VARCHAR(20),
    canh_bao_hoc_vu       BOOLEAN DEFAULT FALSE,
    co_the_tot_nghiep     BOOLEAN DEFAULT FALSE,

    hoc_ky_key_gan_nhat   INT REFERENCES dim_hoc_ky(hoc_ky_key),
    ngay_cap_nhat         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE agg_student_summary IS
    'Tổng hợp sinh viên từ 3 nguồn - dùng cho Grafana dashboard';

CREATE INDEX IF NOT EXISTS idx_agg_ss_ma       ON agg_student_summary(ma_sinh_vien);
CREATE INDEX IF NOT EXISTS idx_agg_ss_rui_ro   ON agg_student_summary(muc_do_rui_ro);
CREATE INDEX IF NOT EXISTS idx_agg_ss_gpa4     ON agg_student_summary(gpa_he_4);
CREATE INDEX IF NOT EXISTS idx_agg_ss_canh_bao ON agg_student_summary(canh_bao_hoc_vu);


-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE '   create_facts.sql v2.1 DONE';
    RAISE NOTICE '   Thay doi so voi v2.0:';
    RAISE NOTICE '   + fact_dang_ky: them uq_fact_dk_sv_hp_hk';
    RAISE NOTICE '   + fact_tai_chinh: ghi chu GENERATED COLUMN';
    RAISE NOTICE '==========================================';
END $$;