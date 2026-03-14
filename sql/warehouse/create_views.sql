-- =============================================
-- DATA WAREHOUSE - ANALYTICAL VIEWS
-- Version: 2.0
-- Sửa: hoc_ky_ten → hoc_ky, INNER→LEFT JOIN gv
-- Thêm: v_at_risk_combined, v_xet_hoc_bong, v_tai_chinh_hoc_tap
-- =============================================

-- =============================================
-- VIEW 1: v_student_performance
-- Kết quả học tập chi tiết — Nguồn: PostgreSQL
-- =============================================
CREATE OR REPLACE VIEW v_student_performance AS
SELECT
    sv.ma_sinh_vien,
    sv.ho_ten,
    sv.khoa_hoc,
    sv.ten_khoa,
    sv.ma_nganh,
    sv.ten_nganh,
    sv.ten_lop,
    sv.trang_thai_hoc_tap,

    hp.ma_hoc_phan,
    hp.ten_mon,
    hp.so_tin_chi,
    hp.bat_buoc,
    hp.loai_hoc_phan,

    gv.ho_ten           AS ten_giang_vien,
    gv.chuc_danh,

    hk.ma_hoc_ky,
    hk.nam_hoc,
    hk.hoc_ky,          -- v2.0: đúng tên cột

    ht.diem_chuyen_can,
    ht.diem_bai_tap,
    ht.diem_giua_ky,
    ht.diem_cuoi_ky,
    ht.diem_tong_ket,
    ht.diem_chu,
    ht.diem_he_4,
    ht.dat_mon,
    ht.hoc_lai,
    ht.so_tin_chi       AS tin_chi_mon,
    ht.diem_chat_luong

FROM fact_hoc_tap ht
JOIN  dim_sinh_vien  sv ON ht.sinh_vien_key  = sv.sinh_vien_key AND sv.la_ban_hien_tai = TRUE
JOIN  dim_hoc_phan   hp ON ht.hoc_phan_key   = hp.hoc_phan_key
LEFT JOIN dim_giang_vien gv ON ht.giang_vien_key = gv.giang_vien_key  -- v2.0: LEFT JOIN
JOIN  dim_hoc_ky     hk ON ht.hoc_ky_key     = hk.hoc_ky_key;

COMMENT ON VIEW v_student_performance IS 'Kết quả học tập chi tiết — Nguồn 1: PostgreSQL';

-- =============================================
-- VIEW 2: v_at_risk_students
-- Cảnh báo học vụ — Nguồn: PostgreSQL
-- =============================================
CREATE OR REPLACE VIEW v_at_risk_students AS
SELECT
    agg.ma_sinh_vien,
    sv.ho_ten,
    sv.ten_khoa,
    sv.ten_nganh,
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
  ON agg.sinh_vien_key  = sv.sinh_vien_key
 AND sv.la_ban_hien_tai = TRUE
WHERE agg.canh_bao_hoc_vu = TRUE
   OR agg.muc_do_rui_ro   = 'Cao'
ORDER BY agg.gpa_he_4 ASC NULLS LAST;

COMMENT ON VIEW v_at_risk_students IS 'SV cần hỗ trợ học vụ (GPA thấp/nhiều môn nợ)';

-- =============================================
-- VIEW 3: v_course_statistics
-- Thống kê môn học — Nguồn: PostgreSQL
-- =============================================
CREATE OR REPLACE VIEW v_course_statistics AS
SELECT
    hp.ma_hoc_phan,
    hp.ten_mon,
    hp.so_tin_chi,
    hp.bat_buoc,
    hp.ten_khoa,
    hp.loai_hoc_phan,

    COUNT(ht.hoc_tap_key)                                               AS tong_luot_hoc,
    COUNT(DISTINCT ht.ma_sinh_vien)                                     AS tong_sinh_vien,
    ROUND(AVG(ht.diem_tong_ket), 2)                                     AS diem_trung_binh,
    ROUND(AVG(ht.diem_he_4), 2)                                         AS gpa_trung_binh,
    SUM(CASE WHEN ht.dat_mon = TRUE  THEN 1 ELSE 0 END)                 AS so_dat,
    SUM(CASE WHEN ht.dat_mon = FALSE THEN 1 ELSE 0 END)                 AS so_khong_dat,
    ROUND(
        100.0 * SUM(CASE WHEN ht.dat_mon = TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(ht.hoc_tap_key), 0)
    , 2)                                                                AS ty_le_dat_pct

FROM fact_hoc_tap ht
JOIN dim_hoc_phan hp ON ht.hoc_phan_key = hp.hoc_phan_key
WHERE ht.diem_tong_ket IS NOT NULL
GROUP BY hp.ma_hoc_phan, hp.ten_mon, hp.so_tin_chi,
         hp.bat_buoc, hp.ten_khoa, hp.loai_hoc_phan;

COMMENT ON VIEW v_course_statistics IS 'Thống kê kết quả theo môn học';

-- =============================================
-- VIEW 4: v_cohort_summary
-- Tổng hợp theo khóa — Nguồn: PostgreSQL
-- =============================================
CREATE OR REPLACE VIEW v_cohort_summary AS
SELECT
    sv.khoa_hoc,
    sv.ten_khoa,
    sv.ten_nganh,

    COUNT(DISTINCT agg.ma_sinh_vien)                                        AS tong_sinh_vien,
    ROUND(AVG(agg.gpa_he_4), 2)                                             AS gpa_trung_binh,

    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Xuất sắc'   THEN 1 ELSE 0 END)   AS xuat_sac,
    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Giỏi'       THEN 1 ELSE 0 END)   AS gioi,
    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Khá'        THEN 1 ELSE 0 END)   AS kha,
    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Trung bình' THEN 1 ELSE 0 END)   AS trung_binh,
    SUM(CASE WHEN agg.xep_loai_hoc_luc = 'Yếu'        THEN 1 ELSE 0 END)   AS yeu,
    SUM(CASE WHEN agg.canh_bao_hoc_vu  = TRUE          THEN 1 ELSE 0 END)   AS co_canh_bao

FROM agg_student_summary agg
JOIN dim_sinh_vien sv
  ON agg.sinh_vien_key    = sv.sinh_vien_key
 AND sv.la_ban_hien_tai   = TRUE
GROUP BY sv.khoa_hoc, sv.ten_khoa, sv.ten_nganh
ORDER BY sv.khoa_hoc;

COMMENT ON VIEW v_cohort_summary IS 'Tổng hợp theo Khóa và Ngành';

-- =============================================
-- VIEW 5: v_term_performance
-- Xu hướng theo học kỳ — Nguồn: PostgreSQL
-- =============================================
CREATE OR REPLACE VIEW v_term_performance AS
SELECT
    hk.ma_hoc_ky,
    hk.nam_hoc,
    hk.hoc_ky,          -- v2.0: đúng tên cột
    hk.ngay_bat_dau,

    COUNT(DISTINCT ht.ma_sinh_vien)        AS tong_sinh_vien,
    COUNT(ht.hoc_tap_key)                  AS tong_luot_hoc,
    ROUND(AVG(ht.diem_tong_ket), 2)        AS diem_trung_binh,
    ROUND(AVG(ht.diem_he_4), 2)            AS gpa_trung_binh,
    SUM(CASE WHEN ht.dat_mon = TRUE  THEN 1 ELSE 0 END) AS tong_dat,
    SUM(CASE WHEN ht.dat_mon = FALSE THEN 1 ELSE 0 END) AS tong_khong_dat,
    ROUND(
        100.0 * SUM(CASE WHEN ht.dat_mon = TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(ht.hoc_tap_key), 0)
    , 2)                                   AS ty_le_dat_pct

FROM fact_hoc_tap ht
JOIN dim_hoc_ky hk ON ht.hoc_ky_key = hk.hoc_ky_key
WHERE ht.diem_tong_ket IS NOT NULL
GROUP BY hk.ma_hoc_ky, hk.nam_hoc, hk.hoc_ky, hk.ngay_bat_dau
ORDER BY hk.ngay_bat_dau;

COMMENT ON VIEW v_term_performance IS 'Xu hướng kết quả học tập theo học kỳ';

-- =============================================
-- VIEW 6: v_at_risk_combined  ← MỚI
-- Phát hiện SV nguy cơ bỏ học — JOIN 3 nguồn
-- Bài toán: GPA thấp + RL kém + nợ học phí
-- =============================================
CREATE OR REPLACE VIEW v_at_risk_combined AS
SELECT
    sv.ma_sinh_vien,
    sv.ho_ten,
    sv.khoa_hoc,
    sv.ten_nganh,
    sv.ten_lop,
    sv.ten_co_van,

    -- Học tập (Nguồn 1)
    agg.gpa_he_4,
    agg.xep_loai_hoc_luc,
    agg.so_mon_khong_dat,
    agg.canh_bao_hoc_vu,

    -- Rèn luyện (Nguồn 2 - CSV)
    agg.diem_rl_trung_binh,
    agg.xep_loai_rl_gan_nhat,

    -- Tài chính (Nguồn 3 - API)
    agg.tong_no_hoc_phi,
    agg.co_no_hoc_phi,

    -- Đánh giá tổng hợp
    agg.muc_do_rui_ro,

    -- Phân loại chi tiết từng chiều
    CASE WHEN agg.gpa_he_4 < 2.0                THEN TRUE ELSE FALSE END AS hoc_tap_yeu,
    CASE WHEN agg.diem_rl_trung_binh < 50        THEN TRUE ELSE FALSE END AS rl_yeu,
    CASE WHEN agg.co_no_hoc_phi = TRUE            THEN TRUE ELSE FALSE END AS no_hoc_phi,

    -- Điểm rủi ro tổng hợp (0-3)
    (CASE WHEN agg.gpa_he_4 < 2.0             THEN 1 ELSE 0 END +
     CASE WHEN agg.diem_rl_trung_binh < 50    THEN 1 ELSE 0 END +
     CASE WHEN agg.co_no_hoc_phi = TRUE        THEN 1 ELSE 0 END
    )                                              AS diem_rui_ro,

    agg.ngay_cap_nhat

FROM agg_student_summary agg
JOIN dim_sinh_vien sv
  ON agg.sinh_vien_key  = sv.sinh_vien_key
 AND sv.la_ban_hien_tai = TRUE
WHERE agg.muc_do_rui_ro IN ('Cao', 'Trung bình')
ORDER BY diem_rui_ro DESC, agg.gpa_he_4 ASC;

COMMENT ON VIEW v_at_risk_combined IS 'SV nguy cơ bỏ học — JOIN 3 nguồn: GPA + RL + học phí';

-- =============================================
-- VIEW 7: v_xet_hoc_bong  ← MỚI
-- Danh sách đủ điều kiện học bổng — JOIN 3 nguồn
-- Tiêu chí: GPA≥3.2 + RL≥80 + không kỷ luật + không nợ HP
-- =============================================
CREATE OR REPLACE VIEW v_xet_hoc_bong AS
SELECT
    sv.ma_sinh_vien,
    sv.ho_ten,
    sv.khoa_hoc,
    sv.ten_nganh,
    sv.ten_lop,

    -- Học tập (Nguồn 1)
    agg.gpa_he_4,
    agg.xep_loai_hoc_luc,

    -- Rèn luyện (Nguồn 2)
    agg.diem_rl_trung_binh,
    agg.xep_loai_rl_gan_nhat,

    -- Tài chính (Nguồn 3)
    agg.co_no_hoc_phi,
    agg.duoc_mien_giam,

    -- Xét từng tiêu chí
    CASE WHEN agg.gpa_he_4 >= 3.6           THEN 'KKHT loai 1'
         WHEN agg.gpa_he_4 >= 3.2           THEN 'KKHT loai 2'
         ELSE NULL END                              AS loai_hoc_bong_de_xuat,

    -- Đủ điều kiện hay không
    CASE
        WHEN agg.gpa_he_4 >= 3.2
         AND agg.diem_rl_trung_binh >= 80
         AND agg.co_no_hoc_phi = FALSE
         AND NOT EXISTS (
             SELECT 1 FROM fact_ctsv fc
             WHERE fc.sinh_vien_key = agg.sinh_vien_key
               AND fc.bi_ky_luat   = TRUE
         )
        THEN TRUE ELSE FALSE
    END                                             AS du_dieu_kien

FROM agg_student_summary agg
JOIN dim_sinh_vien sv
  ON agg.sinh_vien_key  = sv.sinh_vien_key
 AND sv.la_ban_hien_tai = TRUE
WHERE agg.gpa_he_4 >= 3.2
ORDER BY agg.gpa_he_4 DESC, agg.diem_rl_trung_binh DESC;

COMMENT ON VIEW v_xet_hoc_bong IS 'Xét học bổng tự động — JOIN 3 nguồn: GPA + RL + học phí';

-- =============================================
-- VIEW 8: v_tai_chinh_hoc_tap  ← MỚI
-- Tác động nợ học phí đến kết quả thi — JOIN Nguồn 1+3
-- =============================================
CREATE OR REPLACE VIEW v_tai_chinh_hoc_tap AS
SELECT
    sv.ma_sinh_vien,
    sv.ho_ten,
    sv.khoa_hoc,
    sv.ten_nganh,

    -- Học tập (Nguồn 1)
    agg.gpa_he_4,
    agg.xep_loai_hoc_luc,
    agg.ty_le_dat,

    -- Tài chính (Nguồn 3)
    tc_sum.tong_hoc_phi,
    tc_sum.tong_da_dong,
    tc_sum.tong_con_no,
    tc_sum.so_ky_no,
    tc_sum.duoc_mien_giam,

    -- Phân nhóm tài chính
    CASE
        WHEN tc_sum.tong_con_no = 0       THEN 'Đã đóng đủ'
        WHEN tc_sum.so_ky_no   <= 1       THEN 'Nợ nhẹ (1 HK)'
        ELSE                                   'Nợ nặng (≥2 HK)'
    END                                         AS nhom_tai_chinh

FROM agg_student_summary agg
JOIN dim_sinh_vien sv
  ON agg.sinh_vien_key  = sv.sinh_vien_key
 AND sv.la_ban_hien_tai = TRUE
LEFT JOIN (
    SELECT
        sinh_vien_key,
        SUM(hoc_phi_phai_dong)              AS tong_hoc_phi,
        SUM(da_dong)                        AS tong_da_dong,
        SUM(con_no)                         AS tong_con_no,
        COUNT(*) FILTER (WHERE con_no > 0)  AS so_ky_no,
        BOOL_OR(duoc_mien_giam)             AS duoc_mien_giam
    FROM fact_tai_chinh
    GROUP BY sinh_vien_key
) tc_sum ON tc_sum.sinh_vien_key = agg.sinh_vien_key;

COMMENT ON VIEW v_tai_chinh_hoc_tap IS 'Tác động nợ học phí đến GPA — JOIN Nguồn 1 + Nguồn 3';

DO $$
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE '✅ create_views.sql v2.0 DONE';
    RAISE NOTICE '   Views cũ (sửa lỗi):';
    RAISE NOTICE '   v_student_performance, v_at_risk_students';
    RAISE NOTICE '   v_course_statistics, v_cohort_summary';
    RAISE NOTICE '   v_term_performance';
    RAISE NOTICE '   Views mới (3 nguồn):';
    RAISE NOTICE '   v_at_risk_combined    — GPA+RL+HP';
    RAISE NOTICE '   v_xet_hoc_bong        — GPA+RL+HP';
    RAISE NOTICE '   v_tai_chinh_hoc_tap   — GPA+HP';
    RAISE NOTICE '==========================================';
END $$;