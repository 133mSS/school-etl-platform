from sqlalchemy import text
from src.utils.logger import get_logger
from datetime import date
from src.config.database import warehouse_engine, source_engine
logger = get_logger("etl.aggregation")

class DataAggregator:
    def __init__(self):
        self.engine = warehouse_engine

    def run_all(self) -> int:
        logger.info("== BẮT ĐẦU AGGREGATION ==")
        agg_count = self.rebuild_agg_student_summary()
        logger.info(f"  agg_student_summary rebuilt : {agg_count:,}")
        logger.info("== AGGREGATION HOÀN TẤT ==")
        return agg_count

    def rebuild_agg_student_summary(self) -> int:
        sql_truncate = "TRUNCATE TABLE agg_student_summary;"

        # ─────────────────────────────────────────────────────────────────
        # FIX: xep_loai_hoc_luc phải dùng chuỗi có dấu tiếng Việt để
        #      khớp với filter trong create_views.sql (v_cohort_summary,
        #      v_at_risk_students, v_xet_hoc_bong...).
        #      Code cũ dùng 'Xuat sac', 'Gioi', 'Kha', 'Trung binh', 'Yeu'
        #      → Grafana cohort chart luôn hiển thị 0.
        # ─────────────────────────────────────────────────────────────────
        sql_insert = """
            INSERT INTO agg_student_summary (
                sinh_vien_key, ma_sinh_vien, gpa_he_4, gpa_he_10, xep_loai_hoc_luc,
                tong_tin_chi_dang_ky, tin_chi_dat, tin_chi_khong_dat, ty_le_dat,
                tong_mon_dang_ky, so_mon_dat, so_mon_khong_dat, so_mon_hoc_lai,
                diem_rl_trung_binh, xep_loai_rl_gan_nhat, tong_no_hoc_phi,
                co_no_hoc_phi, duoc_mien_giam, muc_do_rui_ro, canh_bao_hoc_vu,
                hoc_ky_key_gan_nhat
            )
            WITH best_grade AS (
                SELECT
                    f.sinh_vien_key, f.ma_sinh_vien, f.hoc_phan_key,
                    MAX(f.diem_he_4)                                        AS best_diem_he_4,
                    MAX(f.diem_tong_ket)                                    AS best_diem_he_10,
                    MAX(CASE WHEN COALESCE(f.dat_mon, FALSE) THEN 1 ELSE 0 END) AS dat_mon,
                    BOOL_OR(COALESCE(f.hoc_lai, FALSE))                    AS hoc_lai,
                    MAX(COALESCE(f.so_tin_chi, 0))                         AS so_tin_chi
                FROM fact_hoc_tap f
                GROUP BY f.sinh_vien_key, f.ma_sinh_vien, f.hoc_phan_key
            ),
            gpa_tich_luy AS (
                SELECT
                    bg.sinh_vien_key,
                    bg.ma_sinh_vien,
                    ROUND(
                        CASE
                            WHEN SUM(CASE WHEN bg.best_diem_he_4 IS NOT NULL
                                     THEN bg.so_tin_chi ELSE 0 END) > 0
                            THEN SUM(CASE WHEN bg.best_diem_he_4 IS NOT NULL
                                     THEN bg.best_diem_he_4 * bg.so_tin_chi ELSE 0 END)
                               / SUM(CASE WHEN bg.best_diem_he_4 IS NOT NULL
                                     THEN bg.so_tin_chi ELSE 0 END)
                            ELSE NULL
                        END, 2
                    ) AS gpa_he_4,
                    ROUND(
                        CASE
                            WHEN SUM(CASE WHEN bg.best_diem_he_10 IS NOT NULL
                                     THEN bg.so_tin_chi ELSE 0 END) > 0
                            THEN SUM(CASE WHEN bg.best_diem_he_10 IS NOT NULL
                                     THEN bg.best_diem_he_10 * bg.so_tin_chi ELSE 0 END)
                               / SUM(CASE WHEN bg.best_diem_he_10 IS NOT NULL
                                     THEN bg.so_tin_chi ELSE 0 END)
                            ELSE NULL
                        END, 2
                    ) AS gpa_he_10,
                    SUM(bg.so_tin_chi)                                      AS tong_tin_chi_dang_ky,
                    SUM(CASE WHEN bg.dat_mon = 1 AND bg.best_diem_he_4 IS NOT NULL
                             THEN bg.so_tin_chi ELSE 0 END)                 AS tin_chi_dat,
                    SUM(CASE WHEN bg.dat_mon = 0 AND bg.best_diem_he_4 IS NOT NULL
                             THEN bg.so_tin_chi ELSE 0 END)                 AS tin_chi_khong_dat,
                    COUNT(*)                                                AS tong_mon_dang_ky,
                    SUM(CASE WHEN bg.dat_mon = 1 AND bg.best_diem_he_4 IS NOT NULL
                             THEN 1 ELSE 0 END)                             AS so_mon_dat,
                    SUM(CASE WHEN bg.dat_mon = 0 AND bg.best_diem_he_4 IS NOT NULL
                             THEN 1 ELSE 0 END)                             AS so_mon_khong_dat,
                    SUM(CASE WHEN bg.hoc_lai THEN 1 ELSE 0 END)            AS so_mon_hoc_lai,
                    ROUND(
                        CASE
                            WHEN SUM(CASE WHEN bg.best_diem_he_4 IS NOT NULL
                                     THEN 1 ELSE 0 END) > 0
                            THEN 100.0
                               * SUM(CASE WHEN bg.dat_mon = 1 AND bg.best_diem_he_4 IS NOT NULL
                                     THEN 1 ELSE 0 END)
                               / SUM(CASE WHEN bg.best_diem_he_4 IS NOT NULL
                                     THEN 1 ELSE 0 END)
                            ELSE NULL
                        END, 2
                    ) AS ty_le_dat
                FROM best_grade bg
                GROUP BY bg.sinh_vien_key, bg.ma_sinh_vien
            ),
            rl_avg AS (
                SELECT
                    sinh_vien_key,
                    ROUND(AVG(diem_rl)::numeric, 2) AS diem_rl_trung_binh
                FROM fact_ctsv
                WHERE diem_rl IS NOT NULL
                GROUP BY sinh_vien_key
            ),
            rl_latest AS (
                SELECT DISTINCT ON (sinh_vien_key)
                    sinh_vien_key, xep_loai_rl, hoc_ky_key
                FROM fact_ctsv
                WHERE hoc_ky_key IS NOT NULL
                ORDER BY sinh_vien_key, hoc_ky_key DESC
            ),
            tc_sum AS (
                SELECT
                    sinh_vien_key,
                    SUM(COALESCE(con_no, 0))          AS tong_no_hoc_phi,
                    BOOL_OR(COALESCE(duoc_mien_giam, FALSE)) AS duoc_mien_giam
                FROM fact_tai_chinh
                GROUP BY sinh_vien_key
            ),
            latest_hk AS (
                SELECT sinh_vien_key, MAX(hoc_ky_key) AS hoc_ky_key_gan_nhat
                FROM fact_hoc_tap
                GROUP BY sinh_vien_key
            )
            SELECT
                sv.sinh_vien_key,
                sv.ma_sinh_vien,
                COALESCE(gpa.gpa_he_4,  0.00)   AS gpa_he_4,
                COALESCE(gpa.gpa_he_10, 0.00)   AS gpa_he_10,

                -- FIX: Dùng chuỗi có dấu để khớp với create_views.sql
                CASE
                    WHEN COALESCE(gpa.gpa_he_4, 0) >= 3.6 THEN 'Xuất sắc'
                    WHEN COALESCE(gpa.gpa_he_4, 0) >= 3.2 THEN 'Giỏi'
                    WHEN COALESCE(gpa.gpa_he_4, 0) >= 2.5 THEN 'Khá'
                    WHEN COALESCE(gpa.gpa_he_4, 0) >= 2.0 THEN 'Trung bình'
                    ELSE 'Yếu'
                END                             AS xep_loai_hoc_luc,

                COALESCE(gpa.tong_tin_chi_dang_ky, 0) AS tong_tin_chi_dang_ky,
                COALESCE(gpa.tin_chi_dat,          0) AS tin_chi_dat,
                COALESCE(gpa.tin_chi_khong_dat,    0) AS tin_chi_khong_dat,
                gpa.ty_le_dat,
                COALESCE(gpa.tong_mon_dang_ky,  0) AS tong_mon_dang_ky,
                COALESCE(gpa.so_mon_dat,        0) AS so_mon_dat,
                COALESCE(gpa.so_mon_khong_dat,  0) AS so_mon_khong_dat,
                COALESCE(gpa.so_mon_hoc_lai,    0) AS so_mon_hoc_lai,

                rlavg.diem_rl_trung_binh,
                rll.xep_loai_rl                 AS xep_loai_rl_gan_nhat,

                COALESCE(tc.tong_no_hoc_phi, 0) AS tong_no_hoc_phi,
                (COALESCE(tc.tong_no_hoc_phi, 0) > 0) AS co_no_hoc_phi,
                COALESCE(tc.duoc_mien_giam, FALSE)    AS duoc_mien_giam,

                CASE
                    WHEN COALESCE(gpa.gpa_he_4, 0) < 2.0
                     AND COALESCE(rlavg.diem_rl_trung_binh, 100) < 50
                     AND COALESCE(tc.tong_no_hoc_phi, 0) > 0    THEN 'Rất cao'
                    WHEN COALESCE(gpa.gpa_he_4, 0) < 1.5
                     AND (COALESCE(rlavg.diem_rl_trung_binh, 100) < 50
                          OR COALESCE(tc.tong_no_hoc_phi, 0) > 0) THEN 'Cao'
                    WHEN COALESCE(gpa.gpa_he_4, 0) < 2.0        THEN 'Trung bình'
                    ELSE 'Thấp'
                END                             AS muc_do_rui_ro,

                (COALESCE(gpa.gpa_he_4, 0) < 2.0) AS canh_bao_hoc_vu,
                lhk.hoc_ky_key_gan_nhat

            FROM dim_sinh_vien sv
            LEFT JOIN gpa_tich_luy gpa  ON sv.sinh_vien_key = gpa.sinh_vien_key
            LEFT JOIN rl_avg       rlavg ON sv.sinh_vien_key = rlavg.sinh_vien_key
            LEFT JOIN rl_latest    rll   ON sv.sinh_vien_key = rll.sinh_vien_key
            LEFT JOIN tc_sum       tc    ON sv.sinh_vien_key = tc.sinh_vien_key
            LEFT JOIN latest_hk    lhk   ON sv.sinh_vien_key = lhk.sinh_vien_key
            WHERE sv.la_ban_hien_tai = TRUE;
        """

        sql_count = "SELECT COUNT(*) FROM agg_student_summary;"

        with self.engine.begin() as conn:
            conn.execute(text(sql_truncate))
            conn.execute(text(sql_insert))
            count = conn.execute(text(sql_count)).scalar() or 0

        logger.info(f"  agg_student_summary → {count:,} records")
        return int(count)


    def sync_student_status(self) -> int:
    
        logger.info("  sync_student_status | Bắt đầu đồng bộ trạng thái SV...")
        today = date.today()
        updated_count = 0

        # ── Bước 1: Lấy trạng thái hiện tại từ Source ─────────────────────
        # Chỉ lấy 2 cột cần thiết để tránh load dữ liệu thừa
        with source_engine.connect() as src_conn:
            rows = src_conn.execute(text(
                "SELECT ma_sinh_vien, trang_thai_hoc_tap "
                "FROM sinh_vien"
            )).fetchall()

        # Chuyển thành dict để lookup nhanh O(1) thay vì vòng lặp O(n²)
        source_status: dict = {
            r.ma_sinh_vien: r.trang_thai_hoc_tap
            for r in rows
        }
        logger.info(f"  sync_student_status | Đọc {len(source_status)} SV từ Source")

        # ── Bước 2: Lấy trạng thái đang active trong Warehouse ────────────
        with self.engine.connect() as wh_conn:
            wh_rows = wh_conn.execute(text(
                "SELECT sinh_vien_key, ma_sinh_vien, trang_thai_hoc_tap, phien_ban "
                "FROM dim_sinh_vien "
                "WHERE la_ban_hien_tai = TRUE"
            )).fetchall()

        logger.info(f"  sync_student_status | Tìm thấy {len(wh_rows)} bản ghi active trong Warehouse")

        # ── Bước 3: So sánh và cập nhật ────────────────────────────────────
        with self.engine.begin() as wh_conn:
            for wh_row in wh_rows:
                ma_sv      = wh_row.ma_sinh_vien
                old_status = wh_row.trang_thai_hoc_tap
                new_status = source_status.get(ma_sv)

                # Bỏ qua nếu SV không còn trong Source (hiếm, nhưng phòng thủ)
                if new_status is None:
                    logger.warning(f"  sync_student_status | {ma_sv} không tìm thấy trong Source")
                    continue

                # Bỏ qua nếu trạng thái không thay đổi
                if old_status == new_status:
                    continue

                logger.info(
                    f"  sync_student_status | {ma_sv}: "
                    f"'{old_status}' → '{new_status}'"
                )

                # ── SCD Type 2: Expire bản ghi cũ ─────────────────────────
                wh_conn.execute(text("""
                    UPDATE dim_sinh_vien
                    SET    la_ban_hien_tai   = FALSE,
                           ngay_het_hieu_luc = :today
                    WHERE  sinh_vien_key    = :key
                """), {
                    "today": today,
                    "key":   wh_row.sinh_vien_key,
                })

                # ── SCD Type 2: Tạo bản ghi mới với trạng thái cập nhật ───
                # Copy toàn bộ thông tin từ bản ghi cũ, chỉ đổi trang_thai + SCD cols
                wh_conn.execute(text("""
                    INSERT INTO dim_sinh_vien (
                        ma_sinh_vien, ho, ten, ho_ten, ngay_sinh, gioi_tinh, email,
                        khoa_hoc, trang_thai_hoc_tap,
                        ma_nganh, ten_nganh, ma_khoa, ten_khoa,
                        ma_lop, ten_lop, ma_co_van, ten_co_van,
                        ngay_hieu_luc, ngay_het_hieu_luc,
                        la_ban_hien_tai, phien_ban
                    )
                    SELECT
                        ma_sinh_vien, ho, ten, ho_ten, ngay_sinh, gioi_tinh, email,
                        khoa_hoc, :new_status,
                        ma_nganh, ten_nganh, ma_khoa, ten_khoa,
                        ma_lop, ten_lop, ma_co_van, ten_co_van,
                        :today,  NULL,
                        TRUE,    phien_ban + 1
                    FROM dim_sinh_vien
                    WHERE sinh_vien_key = :old_key
                """), {
                    "new_status": new_status,
                    "today":      today,
                    "old_key":    wh_row.sinh_vien_key,
                })

                updated_count += 1

        logger.info(
            f"  sync_student_status | Hoàn tất: {updated_count} SV được cập nhật trạng thái"
        )
        return updated_count

class WeeklyAggregator:
    def __init__(self):
        self._agg = DataAggregator()

    def run(self) -> dict:
        logger.info("== WeeklyAggregator: BẮT ĐẦU ==")

        # Bước 1: Đồng bộ trạng thái SV trước (source of truth từ Source DB)
        sync_count = self._agg.sync_student_status()

        # Bước 2: Rebuild aggregate SAU KHI đã sync trạng thái
        # Quan trọng: thứ tự này đảm bảo agg_student_summary phản ánh
        # trạng thái mới nhất (ví dụ: SV vừa "Tốt nghiệp" không còn
        # bị tính vào nhóm "cảnh báo học vụ")
        agg_count = self._agg.rebuild_agg_student_summary()

        result = {
            "agg_student_summary": agg_count,
            "sv_status_synced":    sync_count,
        }

        # Log có nghĩa: phân biệt "0 vì không có thay đổi" vs "0 vì bug"
        if sync_count == 0:
            logger.info("  sv_status_synced    : Không có thay đổi trạng thái trong tuần")
        else:
            logger.info(f"  sv_status_synced    : {sync_count:,} SV được cập nhật trạng thái")

        logger.info(f"  agg_student_summary : {agg_count:,} records")
        logger.info("== WeeklyAggregator: HOÀN TẤT ==")
        return result

class WeeklyReporter:
    def __init__(self):
        self.engine = warehouse_engine

    def generate(self) -> dict:
        logger.info("== WeeklyReporter: Đang tạo báo cáo tuần ==")
        report = {}

        with self.engine.connect() as conn:
            report["sv_canh_bao"] = int(conn.execute(text("""
                SELECT COUNT(*)
                FROM agg_student_summary agg
                JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
                WHERE sv.la_ban_hien_tai    = TRUE
                  AND sv.trang_thai_hoc_tap = 'Đang học'
                  AND agg.canh_bao_hoc_vu  = TRUE
            """)).scalar() or 0)

            report["sv_hoc_bong"] = int(conn.execute(text("""
                SELECT COUNT(*)
                FROM agg_student_summary agg
                JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
                WHERE sv.la_ban_hien_tai    = TRUE
                  AND sv.trang_thai_hoc_tap = 'Đang học'
                  AND agg.gpa_he_4         >= 3.2
                  AND COALESCE(agg.diem_rl_trung_binh, 0) >= 80
                  AND agg.co_no_hoc_phi    = FALSE
            """)).scalar() or 0)

            report["ty_le_dat"] = float(conn.execute(text("""
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN dat_mon = TRUE THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 1
                )
                FROM fact_hoc_tap
                WHERE diem_tong_ket IS NOT NULL
            """)).scalar() or 0.0)

            rows = conn.execute(text("""
                SELECT hp.ten_mon,
                       ROUND(
                           100.0 * SUM(CASE WHEN f.dat_mon = FALSE THEN 1 ELSE 0 END)
                           / NULLIF(COUNT(*), 0), 1
                       ) AS ty_le_rot
                FROM fact_hoc_tap f
                JOIN dim_hoc_phan hp ON f.hoc_phan_key = hp.hoc_phan_key
                WHERE f.diem_tong_ket IS NOT NULL
                GROUP BY hp.ten_mon
                HAVING COUNT(*) >= 20
                ORDER BY ty_le_rot DESC
                LIMIT 5
            """)).fetchall()
            report["top_mon_kho"] = [
                {"ten_mon": r.ten_mon, "ty_le_rot": float(r.ty_le_rot or 0)}
                for r in rows
            ]

            report["sv_rui_ro_cao"] = int(conn.execute(text("""
                SELECT COUNT(*)
                FROM agg_student_summary agg
                JOIN dim_sinh_vien sv ON agg.sinh_vien_key = sv.sinh_vien_key
                WHERE sv.la_ban_hien_tai    = TRUE
                  AND sv.trang_thai_hoc_tap = 'Đang học'
                  AND agg.muc_do_rui_ro    IN ('Cao', 'Rất cao')
            """)).scalar() or 0)

        logger.info(f"  sv_canh_bao   : {report['sv_canh_bao']}")
        logger.info(f"  sv_hoc_bong   : {report['sv_hoc_bong']}")
        logger.info(f"  ty_le_dat     : {report['ty_le_dat']:.1f}%")
        logger.info(f"  sv_rui_ro_cao : {report['sv_rui_ro_cao']}")
        logger.info("== WeeklyReporter: HOÀN TẤT ==")
        return report