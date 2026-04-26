-- =============================================
-- DATA WAREHOUSE - ANALYTICAL VIEWS
-- Version: 3.0
-- FIX: so_mon_hoc_lai semantics corrected
-- NEW: v_canh_bao_theo_hoc_ky (per-semester warning trend)
-- NEW: v_ha_bac_bang_tot_nghiep (panel 3.1 fix)
-- =============================================
-- =============================================
-- VIEW 1: v_student_performance (giữ nguyên)
-- =============================================
CREATE OR REPLACE VIEW v_student_performance AS
SELECT sv.ma_sinh_vien,
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
    gv.ho_ten AS ten_giang_vien,
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
    ht.so_tin_chi AS tin_chi_mon,
    ht.diem_chat_luong
FROM fact_hoc_tap ht
    JOIN dim_sinh_vien sv ON ht.sinh_vien_key = sv.sinh_vien_key
    AND sv.la_ban_hien_tai = TRUE
    JOIN dim_hoc_phan hp ON ht.hoc_phan_key = hp.hoc_phan_key
    LEFT JOIN dim_giang_vien gv ON ht.giang_vien_key = gv.giang_vien_key
    JOIN dim_hoc_ky hk ON ht.hoc_ky_key = hk.hoc_ky_key;
COMMENT ON VIEW v_student_performance IS 'Kết quả học tập chi tiết — Nguồn 1: PostgreSQL';
-- =============================================
-- VIEW 2: v_at_risk_students (giữ nguyên)
-- =============================================
CREATE OR REPLACE VIEW v_at_risk_students AS
SELECT agg.ma_sinh_vien,
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
    JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
    AND sv.la_ban_hien_tai = TRUE
WHERE agg.canh_bao_hoc_vu = TRUE
    OR agg.muc_do_rui_ro = 'Cao'
ORDER BY agg.gpa_he_4 ASC NULLS LAST;
COMMENT ON VIEW v_at_risk_students IS 'SV cần hỗ trợ học vụ (GPA thấp/nhiều môn nợ)';
-- =============================================
-- VIEW 3-5: giữ nguyên (course_statistics, cohort_summary, term_performance)
-- =============================================
CREATE OR REPLACE VIEW v_course_statistics AS
SELECT hp.ma_hoc_phan,
    hp.ten_mon,
    hp.so_tin_chi,
    hp.bat_buoc,
    hp.ten_khoa,
    hp.loai_hoc_phan,
    COUNT(ht.hoc_tap_key) AS tong_luot_hoc,
    COUNT(DISTINCT ht.ma_sinh_vien) AS tong_sinh_vien,
    ROUND(AVG(ht.diem_tong_ket), 2) AS diem_trung_binh,
    ROUND(AVG(ht.diem_he_4), 2) AS gpa_trung_binh,
    SUM(
        CASE
            WHEN ht.dat_mon = TRUE THEN 1
            ELSE 0
        END
    ) AS so_dat,
    SUM(
        CASE
            WHEN ht.dat_mon = FALSE THEN 1
            ELSE 0
        END
    ) AS so_khong_dat,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN ht.dat_mon = TRUE THEN 1
                ELSE 0
            END
        ) / NULLIF(COUNT(ht.hoc_tap_key), 0),
        2
    ) AS ty_le_dat_pct
FROM fact_hoc_tap ht
    JOIN dim_hoc_phan hp ON ht.hoc_phan_key = hp.hoc_phan_key
WHERE ht.diem_tong_ket IS NOT NULL
GROUP BY hp.ma_hoc_phan,
    hp.ten_mon,
    hp.so_tin_chi,
    hp.bat_buoc,
    hp.ten_khoa,
    hp.loai_hoc_phan;
CREATE OR REPLACE VIEW v_cohort_summary AS
SELECT sv.khoa_hoc,
    sv.ten_khoa,
    sv.ten_nganh,
    COUNT(DISTINCT agg.ma_sinh_vien) AS tong_sinh_vien,
    ROUND(AVG(agg.gpa_he_4), 2) AS gpa_trung_binh,
    SUM(
        CASE
            WHEN agg.xep_loai_hoc_luc = 'Xuất sắc' THEN 1
            ELSE 0
        END
    ) AS xuat_sac,
    SUM(
        CASE
            WHEN agg.xep_loai_hoc_luc = 'Giỏi' THEN 1
            ELSE 0
        END
    ) AS gioi,
    SUM(
        CASE
            WHEN agg.xep_loai_hoc_luc = 'Khá' THEN 1
            ELSE 0
        END
    ) AS kha,
    SUM(
        CASE
            WHEN agg.xep_loai_hoc_luc = 'Trung bình' THEN 1
            ELSE 0
        END
    ) AS trung_binh,
    SUM(
        CASE
            WHEN agg.xep_loai_hoc_luc = 'Yếu' THEN 1
            ELSE 0
        END
    ) AS yeu,
    SUM(
        CASE
            WHEN agg.canh_bao_hoc_vu = TRUE THEN 1
            ELSE 0
        END
    ) AS co_canh_bao
FROM agg_student_summary agg
    JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
    AND sv.la_ban_hien_tai = TRUE
GROUP BY sv.khoa_hoc,
    sv.ten_khoa,
    sv.ten_nganh
ORDER BY sv.khoa_hoc;
CREATE OR REPLACE VIEW v_term_performance AS
SELECT hk.ma_hoc_ky,
    hk.nam_hoc,
    hk.hoc_ky,
    hk.ngay_bat_dau,
    COUNT(DISTINCT ht.ma_sinh_vien) AS tong_sinh_vien,
    COUNT(ht.hoc_tap_key) AS tong_luot_hoc,
    ROUND(AVG(ht.diem_tong_ket), 2) AS diem_trung_binh,
    ROUND(AVG(ht.diem_he_4), 2) AS gpa_trung_binh,
    SUM(
        CASE
            WHEN ht.dat_mon = TRUE THEN 1
            ELSE 0
        END
    ) AS tong_dat,
    SUM(
        CASE
            WHEN ht.dat_mon = FALSE THEN 1
            ELSE 0
        END
    ) AS tong_khong_dat,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN ht.dat_mon = TRUE THEN 1
                ELSE 0
            END
        ) / NULLIF(COUNT(ht.hoc_tap_key), 0),
        2
    ) AS ty_le_dat_pct
FROM fact_hoc_tap ht
    JOIN dim_hoc_ky hk ON ht.hoc_ky_key = hk.hoc_ky_key
WHERE ht.diem_tong_ket IS NOT NULL
GROUP BY hk.ma_hoc_ky,
    hk.nam_hoc,
    hk.hoc_ky,
    hk.ngay_bat_dau
ORDER BY hk.ngay_bat_dau;
-- =============================================
-- VIEW 6-8: giữ nguyên (at_risk_combined, xet_hoc_bong, tai_chinh_hoc_tap)
-- =============================================
CREATE OR REPLACE VIEW v_at_risk_combined AS
SELECT sv.ma_sinh_vien,
    sv.ho_ten,
    sv.khoa_hoc,
    sv.ten_nganh,
    sv.ten_lop,
    sv.ten_co_van,
    agg.gpa_he_4,
    agg.xep_loai_hoc_luc,
    agg.so_mon_khong_dat,
    agg.canh_bao_hoc_vu,
    agg.diem_rl_trung_binh,
    agg.xep_loai_rl_gan_nhat,
    agg.tong_no_hoc_phi,
    agg.co_no_hoc_phi,
    agg.muc_do_rui_ro,
    CASE
        WHEN agg.gpa_he_4 < 2.0 THEN TRUE
        ELSE FALSE
    END AS hoc_tap_yeu,
    CASE
        WHEN agg.diem_rl_trung_binh < 50 THEN TRUE
        ELSE FALSE
    END AS rl_yeu,
    CASE
        WHEN agg.co_no_hoc_phi = TRUE THEN TRUE
        ELSE FALSE
    END AS no_hoc_phi,
    (
        CASE
            WHEN agg.gpa_he_4 < 2.0 THEN 1
            ELSE 0
        END + CASE
            WHEN agg.diem_rl_trung_binh < 50 THEN 1
            ELSE 0
        END + CASE
            WHEN agg.co_no_hoc_phi = TRUE THEN 1
            ELSE 0
        END
    ) AS diem_rui_ro,
    agg.ngay_cap_nhat
FROM agg_student_summary agg
    JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
    AND sv.la_ban_hien_tai = TRUE
WHERE agg.muc_do_rui_ro IN ('Cao', 'Trung bình')
ORDER BY diem_rui_ro DESC,
    agg.gpa_he_4 ASC;
CREATE OR REPLACE VIEW v_xet_hoc_bong AS
SELECT sv.ma_sinh_vien,
    sv.ho_ten,
    sv.khoa_hoc,
    sv.ten_nganh,
    sv.ten_lop,
    agg.gpa_he_4,
    agg.xep_loai_hoc_luc,
    agg.diem_rl_trung_binh,
    agg.xep_loai_rl_gan_nhat,
    agg.co_no_hoc_phi,
    agg.duoc_mien_giam,
    CASE
        WHEN agg.gpa_he_4 >= 3.6 THEN 'KKHT loai 1'
        WHEN agg.gpa_he_4 >= 3.2 THEN 'KKHT loai 2'
        ELSE NULL
    END AS loai_hoc_bong_de_xuat,
    CASE
        WHEN agg.gpa_he_4 >= 3.2
        AND agg.diem_rl_trung_binh >= 80
        AND agg.co_no_hoc_phi = FALSE
        AND NOT EXISTS (
            SELECT 1
            FROM fact_ctsv fc
            WHERE fc.sinh_vien_key = agg.sinh_vien_key
                AND fc.bi_ky_luat = TRUE
        ) THEN TRUE
        ELSE FALSE
    END AS du_dieu_kien
FROM agg_student_summary agg
    JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
    AND sv.la_ban_hien_tai = TRUE
WHERE agg.gpa_he_4 >= 3.2
ORDER BY agg.gpa_he_4 DESC,
    agg.diem_rl_trung_binh DESC;
CREATE OR REPLACE VIEW v_tai_chinh_hoc_tap AS
SELECT sv.ma_sinh_vien,
    sv.ho_ten,
    sv.khoa_hoc,
    sv.ten_nganh,
    agg.gpa_he_4,
    agg.xep_loai_hoc_luc,
    agg.ty_le_dat,
    tc_sum.tong_hoc_phi,
    tc_sum.tong_da_dong,
    tc_sum.tong_con_no,
    tc_sum.so_ky_no,
    tc_sum.duoc_mien_giam,
    CASE
        WHEN tc_sum.tong_con_no = 0 THEN 'Đã đóng đủ'
        WHEN tc_sum.so_ky_no <= 1 THEN 'Nợ nhẹ (1 HK)'
        ELSE 'Nợ nặng (≥2 HK)'
    END AS nhom_tai_chinh
FROM agg_student_summary agg
    JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
    AND sv.la_ban_hien_tai = TRUE
    LEFT JOIN (
        SELECT sinh_vien_key,
            SUM(hoc_phi_phai_dong) AS tong_hoc_phi,
            SUM(da_dong) AS tong_da_dong,
            SUM(con_no) AS tong_con_no,
            COUNT(*) FILTER (
                WHERE con_no > 0
            ) AS so_ky_no,
            BOOL_OR(duoc_mien_giam) AS duoc_mien_giam
        FROM fact_tai_chinh
        GROUP BY sinh_vien_key
    ) tc_sum ON tc_sum.sinh_vien_key = agg.sinh_vien_key;
-- =============================================
-- VIEW 9: v_canh_bao_theo_hoc_ky ← MỚI
-- Thống kê SV cảnh báo HỌC VỤ theo từng kỳ
-- Mục đích: đánh giá xu hướng cải thiện/không cải thiện
-- Nguồn: fact_hoc_tap + dim_hoc_ky + dim_sinh_vien
-- =============================================
CREATE OR REPLACE VIEW v_canh_bao_theo_hoc_ky AS WITH gpa_per_hk AS (
        -- Tính GPA của từng SV trong từng học kỳ cụ thể
        SELECT f.sinh_vien_key,
            f.hoc_ky_key,
            ROUND(
                SUM(f.diem_he_4 * COALESCE(f.so_tin_chi, 0)) / NULLIF(SUM(COALESCE(f.so_tin_chi, 0)), 0),
                2
            ) AS gpa_hoc_ky,
            COUNT(*) AS so_mon,
            SUM(
                CASE
                    WHEN f.dat_mon = TRUE THEN 1
                    ELSE 0
                END
            ) AS so_mon_dat,
            SUM(
                CASE
                    WHEN f.dat_mon = FALSE THEN 1
                    ELSE 0
                END
            ) AS so_mon_rot
        FROM fact_hoc_tap f
        WHERE f.diem_he_4 IS NOT NULL
            AND f.so_tin_chi > 0
        GROUP BY f.sinh_vien_key,
            f.hoc_ky_key
    ),
    canh_bao_per_hk AS (
        SELECT g.hoc_ky_key,
            COUNT(DISTINCT g.sinh_vien_key) AS tong_sv_co_diem,
            COUNT(
                DISTINCT CASE
                    WHEN g.gpa_hoc_ky < 2.0 THEN g.sinh_vien_key
                END
            ) AS sv_canh_bao_hk,
            COUNT(
                DISTINCT CASE
                    WHEN g.gpa_hoc_ky < 1.5 THEN g.sinh_vien_key
                END
            ) AS sv_nguy_hiem_hk,
            COUNT(
                DISTINCT CASE
                    WHEN g.gpa_hoc_ky < 1.0 THEN g.sinh_vien_key
                END
            ) AS sv_boc_thoi_hoc_hk,
            ROUND(AVG(g.gpa_hoc_ky), 2) AS gpa_trung_binh_hk,
            ROUND(
                100.0 * COUNT(
                    DISTINCT CASE
                        WHEN g.gpa_hoc_ky < 2.0 THEN g.sinh_vien_key
                    END
                ) / NULLIF(COUNT(DISTINCT g.sinh_vien_key), 0),
                1
            ) AS pct_canh_bao
        FROM gpa_per_hk g
            JOIN dim_sinh_vien sv ON g.sinh_vien_key = sv.sinh_vien_key
            AND sv.la_ban_hien_tai = TRUE
            AND sv.trang_thai_hoc_tap IN ('Đang học', 'Tốt nghiệp', 'Thôi học', 'Bảo lưu')
        GROUP BY g.hoc_ky_key
    )
SELECT hk.ma_hoc_ky,
    hk.nam_hoc,
    hk.hoc_ky,
    hk.ngay_bat_dau,
    c.tong_sv_co_diem,
    c.sv_canh_bao_hk,
    c.sv_nguy_hiem_hk,
    c.sv_boc_thoi_hoc_hk,
    c.gpa_trung_binh_hk,
    c.pct_canh_bao,
    -- So sánh với kỳ trước để phát hiện xu hướng
    LAG(c.sv_canh_bao_hk) OVER (
        ORDER BY hk.ngay_bat_dau
    ) AS sv_canh_bao_ky_truoc,
    LAG(c.pct_canh_bao) OVER (
        ORDER BY hk.ngay_bat_dau
    ) AS pct_canh_bao_ky_truoc,
    c.sv_canh_bao_hk - LAG(c.sv_canh_bao_hk) OVER (
        ORDER BY hk.ngay_bat_dau
    ) AS thay_doi_so_sv,
    CASE
        WHEN c.sv_canh_bao_hk < LAG(c.sv_canh_bao_hk) OVER (
            ORDER BY hk.ngay_bat_dau
        ) THEN 'Cải thiện'
        WHEN c.sv_canh_bao_hk > LAG(c.sv_canh_bao_hk) OVER (
            ORDER BY hk.ngay_bat_dau
        ) THEN 'Xấu hơn'
        WHEN c.sv_canh_bao_hk = LAG(c.sv_canh_bao_hk) OVER (
            ORDER BY hk.ngay_bat_dau
        ) THEN 'Không đổi'
        ELSE 'N/A (kỳ đầu)'
    END AS xu_huong
FROM canh_bao_per_hk c
    JOIN dim_hoc_ky hk ON c.hoc_ky_key = hk.hoc_ky_key
WHERE hk.ma_hoc_ky NOT LIKE '%HK3%' -- Chỉ tính HK chính quy, không tính HK hè
ORDER BY hk.ngay_bat_dau;
COMMENT ON VIEW v_canh_bao_theo_hoc_ky IS 'Xu hướng cảnh báo học vụ theo từng học kỳ — dùng cho panel "Cảnh báo theo kỳ"';
-- =============================================
-- VIEW 10: v_ha_bac_bang_tot_nghiep ← MỚI (fix panel 3.1 No data)
-- SV có xếp loại bằng bị hạ do rớt >5% TC chương trình lần đầu
-- Nguồn: fact_hoc_tap (hoc_lai=FALSE, dat_mon=FALSE) + dim_sinh_vien
-- =============================================
CREATE OR REPLACE VIEW v_ha_bac_bang_tot_nghiep AS WITH tong_tc_nganh AS (
        -- Tổng số TC của từng chương trình đào tạo
        SELECT ma_nganh,
            CASE
                ma_nganh
                WHEN 'CNTT' THEN 151
                WHEN 'DTVT' THEN 153
                WHEN 'KETOAN' THEN 130
            END AS tong_tc_ct
        FROM (
                VALUES ('CNTT'),
                    ('DTVT'),
                    ('KETOAN')
            ) t(ma_nganh)
    ),
    tc_rot_lan_dau AS (
        -- TC rớt lần đầu: chỉ đếm đăng ký KHÔNG phải học lại mà rớt
        -- hoc_lai=FALSE → đây là lần học đầu tiên
        -- dat_mon=FALSE → rớt
        SELECT f.sinh_vien_key,
            SUM(COALESCE(f.so_tin_chi, 0)) AS tc_rot_lan_dau
        FROM fact_hoc_tap f
        WHERE f.hoc_lai = FALSE
            AND f.dat_mon = FALSE
            AND f.diem_tong_ket IS NOT NULL
        GROUP BY f.sinh_vien_key
    ),
    xep_loai_goc AS (
        -- Xếp loại bằng gốc dựa trên GPA tích lũy
        SELECT sv.sinh_vien_key,
            sv.ma_sinh_vien,
            sv.ho_ten,
            sv.ten_nganh,
            sv.ma_nganh,
            sv.khoa_hoc,
            agg.gpa_he_4,
            CASE
                WHEN agg.gpa_he_4 >= 3.6 THEN 'Xuất sắc'
                WHEN agg.gpa_he_4 >= 3.2 THEN 'Giỏi'
                WHEN agg.gpa_he_4 >= 2.5 THEN 'Khá'
                WHEN agg.gpa_he_4 >= 2.0 THEN 'Trung bình'
                ELSE NULL
            END AS xep_loai_bang_goc,
            agg.tin_chi_dat
        FROM agg_student_summary agg
            JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
            AND sv.la_ban_hien_tai = TRUE
            AND sv.trang_thai_hoc_tap = 'Tốt nghiệp'
        WHERE agg.gpa_he_4 >= 2.0
    )
SELECT xg.ma_sinh_vien,
    xg.ho_ten,
    xg.ten_nganh,
    xg.khoa_hoc,
    xg.gpa_he_4,
    xg.xep_loai_bang_goc,
    COALESCE(rd.tc_rot_lan_dau, 0) AS tc_rot_lan_dau,
    tc.tong_tc_ct,
    ROUND(
        100.0 * COALESCE(rd.tc_rot_lan_dau, 0) / NULLIF(tc.tong_tc_ct, 0),
        1
    ) AS pct_tc_rot,
    -- Xếp loại bằng chính thức sau khi hạ bậc theo quy chế
    -- Quy tắc: rớt >25%TC → xếp loại tối đa là Trung bình
    --          rớt >5%TC  → hạ 1 bậc so với xếp loại gốc
    CASE
        WHEN 100.0 * COALESCE(rd.tc_rot_lan_dau, 0) / NULLIF(tc.tong_tc_ct, 0) > 25 THEN 'Trung bình'
        WHEN 100.0 * COALESCE(rd.tc_rot_lan_dau, 0) / NULLIF(tc.tong_tc_ct, 0) > 5 THEN CASE
            xg.xep_loai_bang_goc
            WHEN 'Xuất sắc' THEN 'Giỏi'
            WHEN 'Giỏi' THEN 'Khá'
            WHEN 'Khá' THEN 'Trung bình'
            ELSE xg.xep_loai_bang_goc
        END
        ELSE xg.xep_loai_bang_goc
    END AS xep_loai_bang_chinh_thuc,
    -- Có bị hạ không?
    CASE
        WHEN 100.0 * COALESCE(rd.tc_rot_lan_dau, 0) / NULLIF(tc.tong_tc_ct, 0) > 5 THEN TRUE
        ELSE FALSE
    END AS bi_ha_bac
FROM xep_loai_goc xg
    LEFT JOIN tc_rot_lan_dau rd ON xg.sinh_vien_key = rd.sinh_vien_key
    JOIN tong_tc_nganh tc ON xg.ma_nganh = tc.ma_nganh
WHERE 100.0 * COALESCE(rd.tc_rot_lan_dau, 0) / NULLIF(tc.tong_tc_ct, 0) > 5
ORDER BY pct_tc_rot DESC;
COMMENT ON VIEW v_ha_bac_bang_tot_nghiep IS 'SV tốt nghiệp bị hạ bậc bằng do rớt >5% TC CT lần đầu — fix panel 3.1';
DO $$ BEGIN RAISE NOTICE '==========================================';
RAISE NOTICE 'create_views.sql v3.0 DONE';
RAISE NOTICE 'Views 1-8: giữ nguyên từ v2.0';
RAISE NOTICE 'View mới 9: v_canh_bao_theo_hoc_ky';
RAISE NOTICE '   - Thống kê cảnh báo THEO TỪNG KỲ';
RAISE NOTICE '   - Có xu_huong: Cải thiện/Xấu hơn/Không đổi';
RAISE NOTICE 'View mới 10: v_ha_bac_bang_tot_nghiep';
RAISE NOTICE '   - Fix panel 3.1 No data';
RAISE NOTICE '   - Dùng hoc_lai=FALSE để tính TC rớt lần đầu';
RAISE NOTICE '==========================================';
END $$;

-- ============================================================
-- PATCH: Dọn view v_theo_doi_sv_canh_bao
-- File này ghi đè bản aggregated (sai) bằng bản detail (đúng)
-- Chạy 1 lần duy nhất sau khi đã chạy create_views.sql v3.0
-- ============================================================

-- Bước 1: xoá bản cũ (bản aggregated hôm đầu)
DROP VIEW IF EXISTS v_theo_doi_sv_canh_bao CASCADE;

-- Bước 2: tạo bản DETAIL (1 dòng/SV/kỳ CB) — đây là bản CHÍNH THỨC
CREATE VIEW v_theo_doi_sv_canh_bao AS
WITH gpa_per_hk AS (
    -- GPA của từng SV trong từng kỳ — cùng công thức với v_canh_bao_theo_hoc_ky
    SELECT
        f.sinh_vien_key,
        f.hoc_ky_key,
        ROUND(
            SUM(f.diem_he_4 * COALESCE(f.so_tin_chi, 0))
            / NULLIF(SUM(COALESCE(f.so_tin_chi, 0)), 0),
            2
        ) AS gpa_hk,
        SUM(COALESCE(f.so_tin_chi, 0))                          AS tc_hk,
        SUM(CASE WHEN f.dat_mon = FALSE THEN 1 ELSE 0 END)      AS so_mon_rot
    FROM fact_hoc_tap f
    WHERE f.diem_he_4 IS NOT NULL AND f.so_tin_chi > 0
    GROUP BY f.sinh_vien_key, f.hoc_ky_key
),
hk_chain AS (
    -- Chuỗi HK chronological. next_HK là kỳ CHÍNH QUY ngay sau (không phải "kỳ có điểm kế tiếp")
    SELECT
        hoc_ky_key,
        ma_hoc_ky,
        ngay_bat_dau,
        LEAD(hoc_ky_key) OVER (ORDER BY ngay_bat_dau) AS next_hoc_ky_key,
        LEAD(ma_hoc_ky)  OVER (ORDER BY ngay_bat_dau) AS next_ma_hoc_ky
    FROM dim_hoc_ky
    WHERE ma_hoc_ky NOT LIKE '%HK3%'
),
sv_canh_bao AS (
    -- Mỗi SV × mỗi kỳ họ bị CB (GPA_HK < 2.0)
    SELECT
        g.sinh_vien_key,
        g.hoc_ky_key                AS hoc_ky_key_N,
        g.gpa_hk                    AS gpa_hk_N,
        g.tc_hk                     AS tc_hk_N,
        g.so_mon_rot                AS mon_rot_N,
        hc.ma_hoc_ky                AS ma_hoc_ky_N,
        hc.ngay_bat_dau             AS ngay_bat_dau_N,
        hc.next_hoc_ky_key          AS hoc_ky_key_N1,
        hc.next_ma_hoc_ky           AS ma_hoc_ky_N1
    FROM gpa_per_hk g
    JOIN hk_chain hc ON hc.hoc_ky_key = g.hoc_ky_key
    WHERE g.gpa_hk < 2.0
)
SELECT
    s.sinh_vien_key,
    sv.ma_sinh_vien,
    (sv.ho || ' ' || sv.ten)        AS ho_ten,
    sv.ma_nganh,
    sv.ten_nganh,
    sv.khoa_hoc,
    sv.trang_thai_hoc_tap,
    -- Kỳ N
    s.ma_hoc_ky_N                   AS ky_canh_bao,
    s.ngay_bat_dau_N                AS ngay_ky_canh_bao,
    s.gpa_hk_N                      AS gpa_ky_canh_bao,
    s.tc_hk_N                       AS tc_ky_canh_bao,
    s.mon_rot_N                     AS mon_rot_ky_canh_bao,
    -- Kỳ N+1
    s.ma_hoc_ky_N1                  AS ky_sau,
    g_next.gpa_hk                   AS gpa_ky_sau,
    -- Nhãn kết quả cá nhân
    CASE
        WHEN s.hoc_ky_key_N1 IS NULL                                        THEN 'N/A (chưa có kỳ sau)'
        WHEN g_next.gpa_hk IS NULL AND sv.trang_thai_hoc_tap = 'Thôi học'   THEN 'Đã thôi học'
        WHEN g_next.gpa_hk IS NULL AND sv.trang_thai_hoc_tap = 'Bảo lưu'    THEN 'Bảo lưu'
        WHEN g_next.gpa_hk IS NULL AND sv.trang_thai_hoc_tap = 'Tốt nghiệp' THEN 'Đã tốt nghiệp'
        WHEN g_next.gpa_hk IS NULL                                          THEN 'Không có điểm kỳ sau'
        WHEN g_next.gpa_hk >= 2.0                                           THEN 'Thoát cảnh báo'
        WHEN g_next.gpa_hk >  s.gpa_hk_N                                    THEN 'Có tiến bộ (vẫn CB)'
        WHEN g_next.gpa_hk <  s.gpa_hk_N                                    THEN 'Tệ đi'
        ELSE                                                                     'Không đổi'
    END                             AS ket_qua_ca_nhan,
    ROUND((g_next.gpa_hk - s.gpa_hk_N)::numeric, 2) AS delta_gpa
FROM sv_canh_bao s
JOIN dim_sinh_vien sv
    ON sv.sinh_vien_key = s.sinh_vien_key
    AND sv.la_ban_hien_tai = TRUE
LEFT JOIN gpa_per_hk g_next
    ON g_next.sinh_vien_key = s.sinh_vien_key
    AND g_next.hoc_ky_key   = s.hoc_ky_key_N1;

COMMENT ON VIEW v_theo_doi_sv_canh_bao IS
'Cohort tracking DETAIL: 1 dòng/SV/kỳ CB. Dùng cho panel 101-107 (section "Theo dõi cohort"). GPA_HK<2.0 = định nghĩa CB.';

DO $$ BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE ' PATCH DONE: v_theo_doi_sv_canh_bao';
    RAISE NOTICE '   - Đã xoá bản aggregated (sai)';
    RAISE NOTICE '   - Đã tạo bản DETAIL (chuẩn)';
    RAISE NOTICE '   - Test: SELECT * FROM v_theo_doi_sv_canh_bao LIMIT 5;';
    RAISE NOTICE '==========================================';
END $$;