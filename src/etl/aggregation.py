from sqlalchemy import text
from src.config.database import warehouse_engine
from src.utils.logger import get_logger

logger = get_logger("etl.aggregation")


class DataAggregator:
    def __init__(self):
        self.engine = warehouse_engine

    def run_all(self) -> int:
        logger.info("== BẮT ĐẦU AGGREGATION ==")
        agg_count = self.rebuild_agg_student_summary()

    

        logger.info(f" agg_student_summary rebuilt : {agg_count:,}")
        logger.info("== AGGREGATION HOÀN TẤT ==")
        return agg_count

    def rebuild_agg_student_summary(self) -> int:
        """
        Rebuild agg_student_summary từ dữ liệu đã load vào warehouse.
        GPA ở đây là GPA TÍCH LŨY, không phải GPA 1 học kỳ.
        """
        sql_truncate = "TRUNCATE TABLE agg_student_summary;"

        sql_insert = """
        INSERT INTO agg_student_summary (
            sinh_vien_key,
            ma_sinh_vien,
            gpa_he_4,
            gpa_he_10,
            xep_loai_hoc_luc,
            tong_tin_chi_dang_ky,
            so_mon_khong_dat,
            tong_mon_dang_ky,
            diem_rl_trung_binh,
            xep_loai_rl_gan_nhat,
            tong_no_hoc_phi,
            co_no_hoc_phi,
            duoc_mien_giam,
            muc_do_rui_ro,
            canh_bao_hoc_vu,
            hoc_ky_key_gan_nhat
        )
        WITH best_grade AS (
            SELECT
                f.sinh_vien_key,
                f.ma_sinh_vien,
                f.hoc_phan_key,
                MAX(COALESCE(f.diem_he_4, 0)) AS best_diem_he_4,
                MAX(COALESCE(f.diem_tong_ket, 0)) AS best_diem_he_10,
                MAX(CASE WHEN COALESCE(f.dat_mon, FALSE) THEN 1 ELSE 0 END) AS dat_mon,
                MAX(COALESCE(f.so_tin_chi, 0)) AS so_tin_chi
            FROM fact_hoc_tap f
            GROUP BY f.sinh_vien_key, f.ma_sinh_vien, f.hoc_phan_key
        ),
        gpa_tich_luy AS (
            SELECT
                bg.sinh_vien_key,
                bg.ma_sinh_vien,
                ROUND(
                    CASE WHEN SUM(bg.so_tin_chi) > 0
                        THEN SUM(bg.best_diem_he_4 * bg.so_tin_chi) / SUM(bg.so_tin_chi)
                        ELSE 0
                    END
                , 2) AS gpa_he_4,
                ROUND(
                    CASE WHEN SUM(bg.so_tin_chi) > 0
                        THEN SUM(bg.best_diem_he_10 * bg.so_tin_chi) / SUM(bg.so_tin_chi)
                        ELSE 0
                    END
                , 2) AS gpa_he_10,
                SUM(bg.so_tin_chi) AS tong_tin_chi_dang_ky,
                SUM(CASE WHEN bg.dat_mon = 0 THEN 1 ELSE 0 END) AS so_mon_khong_dat,
                COUNT(*) AS tong_mon_dang_ky
            FROM best_grade bg
            GROUP BY bg.sinh_vien_key, bg.ma_sinh_vien
        ),
        rl_avg AS (
            SELECT
                c.sinh_vien_key,
                ROUND(AVG(c.diem_rl)::numeric, 2) AS diem_rl_trung_binh
            FROM fact_ctsv c
            WHERE c.diem_rl IS NOT NULL
            GROUP BY c.sinh_vien_key
        ),
        rl_latest AS (
            SELECT DISTINCT ON (c.sinh_vien_key)
                c.sinh_vien_key,
                c.xep_loai_rl,
                c.hoc_ky_key
            FROM fact_ctsv c
            WHERE c.hoc_ky_key IS NOT NULL
            ORDER BY c.sinh_vien_key, c.hoc_ky_key DESC
        ),
        tc_sum AS (
            SELECT
                t.sinh_vien_key,
                SUM(COALESCE(t.con_no, 0)) AS tong_no_hoc_phi,
                BOOL_OR(COALESCE(t.duoc_mien_giam, FALSE)) AS duoc_mien_giam
            FROM fact_tai_chinh t
            GROUP BY t.sinh_vien_key
        ),
        latest_hk AS (
            SELECT
                f.sinh_vien_key,
                MAX(f.hoc_ky_key) AS hoc_ky_key_gan_nhat
            FROM fact_hoc_tap f
            GROUP BY f.sinh_vien_key
        )
        SELECT
            sv.sinh_vien_key,
            sv.ma_sinh_vien,
            COALESCE(gpa.gpa_he_4, 0.00) AS gpa_he_4,
            COALESCE(gpa.gpa_he_10, 0.00) AS gpa_he_10,
            CASE
                WHEN COALESCE(gpa.gpa_he_4, 0) >= 3.6 THEN 'Xuat sac'
                WHEN COALESCE(gpa.gpa_he_4, 0) >= 3.2 THEN 'Gioi'
                WHEN COALESCE(gpa.gpa_he_4, 0) >= 2.5 THEN 'Kha'
                WHEN COALESCE(gpa.gpa_he_4, 0) >= 2.0 THEN 'Trung binh'
                ELSE 'Yeu'
            END AS xep_loai_hoc_luc,
            COALESCE(gpa.tong_tin_chi_dang_ky, 0) AS tong_tin_chi_dang_ky,
            COALESCE(gpa.so_mon_khong_dat, 0) AS so_mon_khong_dat,
            COALESCE(gpa.tong_mon_dang_ky, 0) AS tong_mon_dang_ky,
            rlavg.diem_rl_trung_binh,
            rll.xep_loai_rl AS xep_loai_rl_gan_nhat,
            COALESCE(tc.tong_no_hoc_phi, 0) AS tong_no_hoc_phi,
            CASE WHEN COALESCE(tc.tong_no_hoc_phi, 0) > 0 THEN TRUE ELSE FALSE END AS co_no_hoc_phi,
            COALESCE(tc.duoc_mien_giam, FALSE) AS duoc_mien_giam,
            CASE
                WHEN COALESCE(gpa.gpa_he_4, 0) < 2.0
                     AND COALESCE(rlavg.diem_rl_trung_binh, 100) < 50
                     AND COALESCE(tc.tong_no_hoc_phi, 0) > 0
                    THEN 'Rất cao'
                WHEN COALESCE(gpa.gpa_he_4, 0) < 1.5
                    THEN 'Cao'
                WHEN COALESCE(gpa.gpa_he_4, 0) < 2.0
                    THEN 'Trung bình'
                ELSE 'Thấp'
            END AS muc_do_rui_ro,
            CASE
                WHEN COALESCE(gpa.gpa_he_4, 0) < 2.0 THEN TRUE
                ELSE FALSE
            END AS canh_bao_hoc_vu,
            lhk.hoc_ky_key_gan_nhat
        FROM dim_sinh_vien sv
        LEFT JOIN gpa_tich_luy gpa
            ON sv.sinh_vien_key = gpa.sinh_vien_key
        LEFT JOIN rl_avg rlavg
            ON sv.sinh_vien_key = rlavg.sinh_vien_key
        LEFT JOIN rl_latest rll
            ON sv.sinh_vien_key = rll.sinh_vien_key
        LEFT JOIN tc_sum tc
            ON sv.sinh_vien_key = tc.sinh_vien_key
        LEFT JOIN latest_hk lhk
            ON sv.sinh_vien_key = lhk.sinh_vien_key
        WHERE sv.la_ban_hien_tai = TRUE;
        """

        sql_count = "SELECT COUNT(*) FROM agg_student_summary;"

        with self.engine.begin() as conn:
            conn.execute(text(sql_truncate))
            conn.execute(text(sql_insert))
            count = conn.execute(text(sql_count)).scalar() or 0

        logger.info(f" agg_student_summary → {count:,} records")
        return int(count)

    def sync_student_status(self) -> int:
        """
        Đồng bộ trạng thái sinh viên hiện tại theo GPA TÍCH LŨY + DRL.
        Rule:
        - GPA < 1.0 -> Thôi học
        - GPA < 1.2 AND DRL TB < 50 -> Thôi học
        Chỉ update bản hiện tại.
        Không đụng vào sinh viên đã Tốt nghiệp.
        """
        sql_update = """
        UPDATE dim_sinh_vien sv
        SET trang_thai_hoc_tap = 'Thôi học'
        FROM agg_student_summary agg
        WHERE sv.sinh_vien_key = agg.sinh_vien_key
          AND sv.la_ban_hien_tai = TRUE
          AND COALESCE(sv.trang_thai_hoc_tap, 'Đang học') <> 'Tốt nghiệp'
          AND (
                COALESCE(agg.gpa_he_4, 0) < 1.0
             OR (
                    COALESCE(agg.gpa_he_4, 0) < 1.2
                AND COALESCE(agg.diem_rl_trung_binh, 100) < 50
             )
          );
        """

        sql_count = """
        SELECT COUNT(*)
        FROM dim_sinh_vien sv
        WHERE sv.la_ban_hien_tai = TRUE
          AND sv.trang_thai_hoc_tap = 'Thôi học';
        """

        with self.engine.begin() as conn:
            conn.execute(text(sql_update))
            count = conn.execute(text(sql_count)).scalar() or 0

        logger.info(f" dim_sinh_vien synced → current 'Thôi học' = {count:,}")
        return int(count)