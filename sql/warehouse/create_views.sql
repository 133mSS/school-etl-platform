-- =============================================
-- DATA WAREHOUSE - ANALYTICAL VIEWS
-- Dùng cho Grafana Dashboards & Báo cáo
-- Chạy SAU create_facts.sql (thứ tự alphabet đảm bảo)
-- User: warehouse_user | DB: school_warehouse | Port: 5435
-- =============================================

-- =============================================
-- VIEW 1: v_student_performance
-- Full join — cho phân tích chi tiết
-- =============================================
CREATE OR REPLACE VIEW v_student_performance AS
SELECT
    sv.ma_sinh_vien,
    sv.ho_ten,
    sv.khoa_hoc,
    sv.ten_khoa,
    sv.ten_lop,
    sv.trang_thai_hoc_tap,

    hp.ma_hoc_phan,
    hp.ten_mon,
    hp.so_tin_chi,
    hp.bat_buoc,
    hp.loai_mon,

    gv.ho_ten        AS ten_giang_vien,
    gv.chuc_danh,

    hk.ma_hoc_ky,
    hk.nam_hoc,
    hk.hoc_ky,

    ht.diem_chuyen_can,
    ht.diem_bai_tap,
    ht.diem_giua_ky,
    ht.diem_cuoi_ky,
    ht.diem_tong_ket,
    ht.diem_chu,
    ht.diem_he_4,
    ht.dat_mon,
    ht.hoc_lai,
    ht.so_tin_chi    AS tin_chi_mon,
    ht.diem_chat_luong

FROM fact_hoc_tap ht
JOIN dim_sinh_vien  sv ON ht.sinh_vien_key  = sv.sinh_vien_key  AND sv.la_ban_hien_tai = TRUE
JOIN dim_hoc_phan   hp ON ht.hoc_phan_key   = hp.hoc_phan_key
JOIN dim_giang_vien gv ON ht.giang_vien_key = gv.giang_vien_key
JOIN dim_hoc_ky     hk ON ht.hoc_ky_key     = hk.hoc_ky_key;

COMMENT ON VIEW v_student_performance IS 'Full student performance view — cho phân tích chi tiết';

-- =============================================
-- VIEW 2: v_at_risk_students
-- Sinh viên nguy cơ học tập kém — hỗ trợ kịp thời
-- =============================================
CREATE OR REPLACE VIEW v_at_risk_students AS
SELECT
    agg.ma_sinh_vien,
    sv.ho_ten,
    sv.ten_khoa,
    sv.ten_lop,
    sv.khoa_hoc,
    sv.trang_thai_hoc_tap,
    sv.ten_co_van,

    agg.gpa_he_4,
    agg.gpa_he_10,
    agg.xep_loai_hoc_luc,
    agg.muc_do_rui_ro,
    agg.so_mon_khong_dat,
    agg.so_mon_hoc_lai,
    agg.ty_le_dat,
    agg.canh_bao_hoc_vu,
    agg.ngay_cap_nhat

FROM agg_student_summary agg
JOIN dim_sinh_vien sv
  ON agg.sinh_vien_key = sv.sinh_vien_key
 AND sv.la_ban_hien_tai = TRUE
WHERE agg.canh_bao_hoc_vu = TRUE
   OR agg.muc_do_rui_ro = 'Cao'
ORDER BY agg.gpa_he_4 ASC NULLS LAST;

COMMENT ON VIEW v_at_risk_students IS 'At-risk students — GPA thấp hoặc nhiều môn không đạt';

-- =============================================
-- VIEW 3: v_course_statistics
-- Thống kê kết quả theo môn học
-- =============================================
CREATE OR REPLACE VIEW v_course_statistics AS
SELECT
    hp.ma_hoc_phan,
    hp.ten_mon,
    hp.so_tin_chi,
    hp.bat_buoc,
    hp.ten_khoa,

    COUNT(ht.hoc_tap_key)                                        AS tong_luot_hoc,
    COUNT(DISTINCT ht.ma_sinh_vien)                              AS tong_sinh_vien,
    ROUND(AVG(ht.diem_tong_ket), 2)                              AS diem_trung_binh,
    ROUND(AVG(ht.diem_he_4), 2)                                  AS gpa_trung_binh,
    ROUND(MIN(ht.diem_tong_ket), 2)                              AS diem_thap_nhat,
    ROUND(MAX(ht.diem_tong_ket), 2)                              AS diem_cao_nhat,

    SUM(CASE WHEN ht.dat_mon = TRUE  THEN 1 ELSE 0 END)          AS so_dat,
    SUM(CASE WHEN ht.dat_mon = FALSE THEN 1 ELSE 0 END)          AS so_khong_dat,
    ROUND(
        100.0 * SUM(CASE WHEN ht.dat_mon = TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(ht.hoc_tap_key), 0)
    , 2)                                                         AS ty_le_dat_pct,

    COUNT(DISTINCT ht.giang_vien_key)                            AS so_giang_vien,
    COUNT(DISTINCT ht.hoc_ky_key)                                AS so_hoc_ky_giang_day

FROM fact_hoc_tap ht
JOIN dim_hoc_phan hp ON ht.hoc_phan_key = hp.hoc_phan_key
WHERE ht.diem_tong_ket IS NOT NULL
GROUP BY hp.ma_hoc_phan, hp.ten_mon, hp.so_tin_chi, hp.bat_buoc, hp.ten_khoa;

COMMENT ON VIEW v_course_statistics IS 'Course-level stats — tỷ lệ đạt, điểm TB theo môn';

-- =============================================
-- VIEW 4: v_instructor_statistics
-- Thống kê theo giảng viên
-- =============================================
CREATE OR REPLACE VIEW v_instructor_statistics AS
SELECT
    gv.ma_giang_vien,
    gv.ho_ten        AS ten_giang_vien,
    gv.chuc_danh,
    gv.ten_khoa,

    COUNT(DISTINCT ht.ma_sinh_vien)         AS tong_sinh_vien,
    COUNT(DISTINCT ht.hoc_phan_key)         AS so_mon_day,
    COUNT(ht.hoc_tap_key)                   AS tong_luot_cham_diem,
    ROUND(AVG(ht.diem_tong_ket), 2)         AS diem_trung_binh,
    ROUND(
        100.0 * SUM(CASE WHEN ht.dat_mon = TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(ht.hoc_tap_key), 0)
    , 2)                                    AS ty_le_dat_pct

FROM fact_hoc_tap ht
JOIN dim_giang_vien gv ON ht.giang_vien_key = gv.giang_vien_key
WHERE ht.diem_tong_ket IS NOT NULL
GROUP BY gv.ma_giang_vien, gv.ho_ten, gv.chuc_danh, gv.ten_khoa;

COMMENT ON VIEW v_instructor_statistics IS 'Instructor-level stats — hiệu quả giảng dạy';

-- =============================================
-- VIEW 5: v_cohort_summary
-- Tổng hợp theo khóa (B21, B22, B23, B24)
-- =============================================
CREATE OR REPLACE VIEW v_cohort_summary AS
SELECT
    sv.khoa_hoc,
    sv.ten_khoa,

    COUNT(DISTINCT agg.ma_sinh_vien)                                        AS tong_sinh_vien,
    ROUND(AVG(agg.gpa_he_4), 2)                                             AS gpa_trung_binh,

    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Xuất sắc'   THEN 1 ELSE 0 END)   AS xuat_sac,
    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Giỏi'       THEN 1 ELSE 0 END)   AS gioi,
    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Khá'        THEN 1 ELSE 0 END)   AS kha,
    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Trung bình' THEN 1 ELSE 0 END)   AS trung_binh,
    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Yếu'        THEN 1 ELSE 0 END)   AS yeu,
    SUM(CASE WHEN agg.canh_bao_hoc_vu  = TRUE          THEN 1 ELSE 0 END)   AS co_canh_bao,
    ROUND(
        100.0 * SUM(CASE WHEN agg.canh_bao_hoc_vu = TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(agg.ma_sinh_vien), 0)
    , 2)                                                                    AS ty_le_canh_bao_pct

FROM agg_student_summary agg
JOIN dim_sinh_vien sv
  ON agg.sinh_vien_key = sv.sinh_vien_key
 AND sv.la_ban_hien_tai = TRUE
GROUP BY sv.khoa_hoc, sv.ten_khoa
ORDER BY sv.khoa_hoc;

COMMENT ON VIEW v_cohort_summary IS 'Cohort summary (B21-B24) — xếp loại và cảnh báo theo khóa';

-- =============================================
-- VIEW 6: v_term_performance
-- Xu hướng kết quả theo học kỳ — Grafana timeline
-- =============================================
CREATE OR REPLACE VIEW v_term_performance AS
SELECT
    hk.ma_hoc_ky,
    hk.nam_hoc,
    hk.hoc_ky,
    hk.ngay_bat_dau,

    COUNT(DISTINCT ht.ma_sinh_vien)         AS tong_sinh_vien,
    COUNT(ht.hoc_tap_key)                   AS tong_luot_hoc,
    ROUND(AVG(ht.diem_tong_ket), 2)         AS diem_trung_binh,
    ROUND(AVG(ht.diem_he_4), 2)             AS gpa_trung_binh,

    SUM(CASE WHEN ht.dat_mon = TRUE  THEN 1 ELSE 0 END)  AS tong_dat,
    SUM(CASE WHEN ht.dat_mon = FALSE THEN 1 ELSE 0 END)  AS tong_khong_dat,
    ROUND(
        100.0 * SUM(CASE WHEN ht.dat_mon = TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(ht.hoc_tap_key), 0)
    , 2)                                    AS ty_le_dat_pct

FROM fact_hoc_tap ht
JOIN dim_hoc_ky hk ON ht.hoc_ky_key = hk.hoc_ky_key
WHERE ht.diem_tong_ket IS NOT NULL
GROUP BY hk.ma_hoc_ky, hk.nam_hoc, hk.hoc_ky, hk.ngay_bat_dau
ORDER BY hk.ngay_bat_dau;

COMMENT ON VIEW v_term_performance IS 'Term-over-term trend — Grafana timeline chart';

-- =============================================
-- SUCCESS
-- =============================================
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ create_views.sql DONE';
    RAISE NOTICE '   Views: v_student_performance,';
    RAISE NOTICE '   v_at_risk_students, v_course_statistics,';
    RAISE NOTICE '   v_instructor_statistics, v_cohort_summary,';
    RAISE NOTICE '   v_term_performance';
    RAISE NOTICE '========================================';
END $$;