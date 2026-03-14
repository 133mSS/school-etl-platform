# src/etl/load.py
"""
load.py - Load dữ liệu đã transform vào Data Warehouse.

Thứ tự load BẮT BUỘC (theo FK dependency):
  1. dim_hoc_ky          → fact cần hoc_ky_key
  2. dim_giang_vien      → fact cần giang_vien_key
  3. dim_hoc_phan        → fact cần hoc_phan_key
  4. dim_sinh_vien       → fact cần sinh_vien_key
  5. fact_hoc_tap        → Nguồn 1 PostgreSQL
  6. fact_ctsv           → Nguồn 2 CSV CTSV
  7. fact_tai_chinh      → Nguồn 3 API Portal
  8. agg_student_summary → tổng hợp 3 nguồn (load cuối)
"""

import pandas as pd
from datetime import datetime, date
from sqlalchemy import create_engine, text
from src.config.settings import WAREHOUSE_DB_URL
from src.utils.logger import get_logger

logger    = get_logger(__name__)
wh_engine = create_engine(WAREHOUSE_DB_URL, pool_pre_ping=True)


# ══════════════════════════════════════════════════════════════════
# TIỆN ÍCH DÙNG CHUNG
# ══════════════════════════════════════════════════════════════════

def _count_table(table: str) -> int:
    """Đếm số bản ghi trong bảng warehouse."""
    with wh_engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def _lookup_key(conn, table: str, key_col: str,
                natural_col: str, natural_val: str):
    """
    Tìm surrogate key trong dimension.
    Ví dụ: _lookup_key(conn, 'dim_sinh_vien', 'sinh_vien_key',
                       'ma_sinh_vien', 'B21DCAT001')
    Trả về: int key hoặc None nếu không tìm thấy
    """
    row = conn.execute(text(
        f"SELECT {key_col} FROM {table} WHERE {natural_col} = :val"
    ), {"val": natural_val}).fetchone()
    return row[0] if row else None


# ══════════════════════════════════════════════════════════════════
# PHẦN 1 — LOAD DIMENSIONS
# ══════════════════════════════════════════════════════════════════

def load_dim_hoc_ky(df: pd.DataFrame):
    """
    Load học kỳ vào dim_hoc_ky. Dùng UPSERT.
    Input: DataFrame từ bảng hoc_ky_nam_hoc (source)
    """
    logger.info(f"Load dim_hoc_ky: {len(df)} rows...")

    with wh_engine.begin() as conn:
        for _, row in df.iterrows():
            # Tính nam_bat_dau / nam_ket_thuc từ nam_hoc (vd: '2023-2024')
            nam_bd, nam_kt = None, None
            if pd.notna(row.get("nam_hoc")):
                parts = str(row["nam_hoc"]).split("-")
                if len(parts) == 2:
                    try:
                        nam_bd = int(parts[0])
                        nam_kt = int(parts[1])
                    except ValueError:
                        pass

            conn.execute(text("""
                INSERT INTO dim_hoc_ky (
                    ma_hoc_ky, nam_hoc, hoc_ky,
                    ngay_bat_dau, ngay_ket_thuc,
                    nam_bat_dau, nam_ket_thuc
                ) VALUES (
                    :ma_hk, :nam_hoc, :hoc_ky,
                    :ngay_bd, :ngay_kt,
                    :nam_bd, :nam_kt
                )
                ON CONFLICT (ma_hoc_ky) DO UPDATE SET
                    nam_hoc       = EXCLUDED.nam_hoc,
                    hoc_ky        = EXCLUDED.hoc_ky,
                    ngay_bat_dau  = EXCLUDED.ngay_bat_dau,
                    ngay_ket_thuc = EXCLUDED.ngay_ket_thuc,
                    nam_bat_dau   = EXCLUDED.nam_bat_dau,
                    nam_ket_thuc  = EXCLUDED.nam_ket_thuc
            """), {
                "ma_hk"  : row["ma_hoc_ky"],
                "nam_hoc": row.get("nam_hoc"),
                "hoc_ky" : row.get("hoc_ky"),  # cột thực tế trong DB là 'hoc_ky'
                "ngay_bd": row.get("ngay_bat_dau"),
                "ngay_kt": row.get("ngay_ket_thuc"),
                "nam_bd" : nam_bd,
                "nam_kt" : nam_kt,
            })

    total = _count_table("dim_hoc_ky")
    logger.info(f"  → dim_hoc_ky: {total} bản ghi")


def load_dim_giang_vien(df: pd.DataFrame):
    """
    Load giảng viên vào dim_giang_vien. Dùng UPSERT.
    Input: DataFrame từ transform_giang_vien()
    """
    logger.info(f"Load dim_giang_vien: {len(df)} rows...")

    with wh_engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO dim_giang_vien (
                    ma_giang_vien, ho, ten, ho_ten, email,
                    chuc_danh, trang_thai_cong_tac,
                    ma_khoa, ma_co_so
                ) VALUES (
                    :mgv, :ho, :ten, :ho_ten, :email,
                    :chuc_danh, :trang_thai,
                    :ma_khoa, :ma_co_so
                )
                ON CONFLICT (ma_giang_vien) DO UPDATE SET
                    ho_ten              = EXCLUDED.ho_ten,
                    chuc_danh           = EXCLUDED.chuc_danh,
                    trang_thai_cong_tac = EXCLUDED.trang_thai_cong_tac,
                    ma_khoa             = EXCLUDED.ma_khoa
            """), {
                "mgv"       : row["ma_giang_vien"],
                "ho"        : row.get("ho"),
                "ten"       : row.get("ten"),
                "ho_ten"    : row["ho_ten"],
                "email"     : row.get("email"),
                "chuc_danh" : row.get("chuc_danh"),
                "trang_thai": row.get("trang_thai_cong_tac", "Đang công tác"),
                "ma_khoa"   : row.get("ma_khoa"),
                "ma_co_so"  : row.get("ma_co_so"),
            })

    total = _count_table("dim_giang_vien")
    logger.info(f"  → dim_giang_vien: {total} bản ghi")


def load_dim_hoc_phan(df: pd.DataFrame):
    """
    Load học phần vào dim_hoc_phan. Dùng UPSERT.
    Lưu ý: warehouse dùng 'loai_mon' (warehouse_models.py line 114)
    Input: DataFrame từ bảng hoc_phan (source)
    """
    logger.info(f"Load dim_hoc_phan: {len(df)} rows...")

    with wh_engine.begin() as conn:
        for _, row in df.iterrows():
            loai = "Bắt buộc" if row.get("bat_buoc", True) else "Tự chọn"
            conn.execute(text("""
                INSERT INTO dim_hoc_phan (
                    ma_hoc_phan, ma_mon, ten_mon,
                    so_tin_chi, so_gio_ly_thuyet, so_gio_thuc_hanh,
                    hoc_ky_de_xuat, bat_buoc, loai_hoc_phan, ma_khoa
                ) VALUES (
                    :mhp, :ma_mon, :ten_mon,
                    :tc, :gio_lt, :gio_th,
                    :hk_de_xuat, :bat_buoc, :loai_hoc_phan, :ma_khoa
                )
                ON CONFLICT (ma_hoc_phan) DO UPDATE SET
                    ten_mon       = EXCLUDED.ten_mon,
                    so_tin_chi    = EXCLUDED.so_tin_chi,
                    bat_buoc      = EXCLUDED.bat_buoc,
                    loai_hoc_phan = EXCLUDED.loai_hoc_phan
            """), {
                "mhp"       : row["ma_hoc_phan"],
                "ma_mon"    : row.get("ma_mon"),
                "ten_mon"   : row["ten_mon"],
                "tc"        : row.get("so_tin_chi"),
                "gio_lt"    : row.get("so_gio_ly_thuyet", 0),
                "gio_th"    : row.get("so_gio_thuc_hanh", 0),
                "hk_de_xuat": row.get("hoc_ky_de_xuat"),
                "bat_buoc"  : bool(row.get("bat_buoc", True)),
                "loai_hoc_phan": loai,
                "ma_khoa"   : row.get("ma_khoa"),
            })

    total = _count_table("dim_hoc_phan")
    logger.info(f"  → dim_hoc_phan: {total} bản ghi")


def load_dim_sinh_vien(df: pd.DataFrame):
    """
    Load sinh viên vào dim_sinh_vien theo chuẩn SCD Type 2.

    SCD Type 2 hoạt động như thế nào?
    - Lần đầu: INSERT bản ghi mới, la_ban_hien_tai=TRUE, phien_ban=1
    - Lần sau nếu trang_thai hoặc ma_lop THAY ĐỔI:
        → Đóng bản ghi cũ: la_ban_hien_tai=FALSE, ngay_het_hieu_luc=hôm nay
        → INSERT bản ghi mới, phien_ban tăng thêm 1
    → Warehouse giữ toàn bộ lịch sử thay đổi

    Input: DataFrame từ transform_sinh_vien()
    """
    logger.info(f"Load dim_sinh_vien: {len(df)} rows...")
    today    = date.today()
    inserted = 0
    updated  = 0

    with wh_engine.begin() as conn:
        for _, row in df.iterrows():
            existing = conn.execute(text("""
                SELECT sinh_vien_key, trang_thai_hoc_tap, ma_lop, phien_ban
                FROM dim_sinh_vien
                WHERE ma_sinh_vien    = :msv
                  AND la_ban_hien_tai = TRUE
            """), {"msv": row["ma_sinh_vien"]}).fetchone()

            params = {
                "msv"       : row["ma_sinh_vien"],
                "ho"        : row.get("ho"),
                "ten"       : row.get("ten"),
                "ho_ten"    : row["ho_ten"],
                "ngay_sinh" : row.get("ngay_sinh"),
                "gioi_tinh" : row.get("gioi_tinh"),
                "email"     : row.get("email"),
                "khoa_hoc"  : row.get("khoa_hoc"),
                # v2.0: bo he_dao_tao, hoc_ky_hien_tai
                "ma_khoa"   : row.get("ma_khoa"),
                "ten_khoa"  : row.get("ten_khoa"),
                "ma_nganh"  : row.get("ma_nganh"),
                "ten_nganh" : row.get("ten_nganh"),
                "ma_lop"    : row.get("ma_lop"),
                "ten_lop"   : row.get("ten_lop"),
                "trang_thai": row.get("trang_thai_hoc_tap"),
                "ma_co_van" : row.get("ma_co_van"),
                "ten_co_van": row.get("ten_co_van"),
                "today"     : today,
            }

            if existing is None:
                params["phien_ban"] = 1
                conn.execute(text("""
                    INSERT INTO dim_sinh_vien (
                        ma_sinh_vien, ho, ten, ho_ten,
                        ngay_sinh, gioi_tinh, email,
                        khoa_hoc,
                        ma_khoa, ten_khoa,
                        ma_nganh, ten_nganh,
                        ma_lop, ten_lop,
                        trang_thai_hoc_tap,
                        ma_co_van, ten_co_van,
                        ngay_hieu_luc, ngay_het_hieu_luc,
                        la_ban_hien_tai, phien_ban
                    ) VALUES (
                        :msv, :ho, :ten, :ho_ten,
                        :ngay_sinh, :gioi_tinh, :email,
                        :khoa_hoc,
                        :ma_khoa, :ten_khoa,
                        :ma_nganh, :ten_nganh,
                        :ma_lop, :ten_lop,
                        :trang_thai,
                        :ma_co_van, :ten_co_van,
                        :today, NULL,
                        TRUE, :phien_ban
                    )
                """), params)
                inserted += 1

            else:
                changed = (
                    existing.trang_thai_hoc_tap != row.get("trang_thai_hoc_tap") or
                    existing.ma_lop             != row.get("ma_lop")
                )
                if changed:
                    conn.execute(text("""
                        UPDATE dim_sinh_vien
                        SET la_ban_hien_tai   = FALSE,
                            ngay_het_hieu_luc = :today
                        WHERE sinh_vien_key   = :sk
                    """), {"sk": existing.sinh_vien_key, "today": today})

                    params["phien_ban"] = existing.phien_ban + 1
                    conn.execute(text("""
                        INSERT INTO dim_sinh_vien (
                            ma_sinh_vien, ho, ten, ho_ten,
                            ngay_sinh, gioi_tinh, email,
                            khoa_hoc,
                            ma_khoa, ten_khoa,
                            ma_nganh, ten_nganh,
                            ma_lop, ten_lop,
                            trang_thai_hoc_tap,
                            ma_co_van, ten_co_van,
                            ngay_hieu_luc, ngay_het_hieu_luc,
                            la_ban_hien_tai, phien_ban
                        ) VALUES (
                            :msv, :ho, :ten, :ho_ten,
                            :ngay_sinh, :gioi_tinh, :email,
                            :khoa_hoc,
                            :ma_khoa, :ten_khoa,
                            :ma_nganh, :ten_nganh,
                            :ma_lop, :ten_lop,
                            :trang_thai,
                            :ma_co_van, :ten_co_van,
                            :today, NULL,
                            TRUE, :phien_ban
                        )
                    """), params)
                    updated += 1

    total = _count_table("dim_sinh_vien")
    logger.info(f"  → dim_sinh_vien: +{inserted} mới, ~{updated} cập nhật | Tổng: {total}")


# ══════════════════════════════════════════════════════════════════
# PHẦN 2 — LOAD FACT
# ══════════════════════════════════════════════════════════════════

def load_fact_hoc_tap(df_diem: pd.DataFrame):
    """
    Load điểm vào fact_hoc_tap.

    Quy trình mỗi dòng:
      1. Lookup sinh_vien_key  từ dim_sinh_vien (la_ban_hien_tai=TRUE)
      2. Lookup hoc_phan_key   từ dim_hoc_phan
      3. Lookup hoc_ky_key     từ dim_hoc_ky
      4. Lookup giang_vien_key từ dim_giang_vien (có thể NULL)
      5. INSERT, bỏ qua nếu đã tồn tại (uq_fact_ht_sv_hp_hk)

    Input: DataFrame từ transform_diem()
    """
    logger.info(f"Load fact_hoc_tap: {len(df_diem)} rows...")
    inserted = 0
    skipped  = 0

    with wh_engine.begin() as conn:
        for _, row in df_diem.iterrows():

            # Lookup surrogate key sinh viên (bản hiện tại)
            sv_sk = conn.execute(text("""
                SELECT sinh_vien_key FROM dim_sinh_vien
                WHERE ma_sinh_vien    = :msv
                  AND la_ban_hien_tai = TRUE
            """), {"msv": row["ma_sinh_vien"]}).scalar()

            if sv_sk is None:
                skipped += 1
                continue

            hp_sk = _lookup_key(conn, "dim_hoc_phan", "hoc_phan_key",
                                 "ma_hoc_phan", row["ma_hoc_phan"])
            if hp_sk is None:
                skipped += 1
                continue

            hk_sk = _lookup_key(conn, "dim_hoc_ky", "hoc_ky_key",
                                 "ma_hoc_ky", row["ma_hoc_ky"])
            if hk_sk is None:
                skipped += 1
                continue

            # giang_vien_key: NULL được chấp nhận (FK không bắt buộc)
            gv_sk = None
            if pd.notna(row.get("ma_giang_vien")):
                gv_sk = _lookup_key(conn, "dim_giang_vien", "giang_vien_key",
                                    "ma_giang_vien", row["ma_giang_vien"])

            # date_key từ ngay_cham
            date_key = None
            if pd.notna(row.get("ngay_cham")):
                d  = pd.to_datetime(row["ngay_cham"]).date()
                dk = conn.execute(text(
                    "SELECT date_key FROM dim_date WHERE full_date = :d"
                ), {"d": d}).scalar()
                date_key = dk

            # diem_chat_luong = diem_he_4 × so_tin_chi
            diem_cl = None
            if pd.notna(row.get("diem_he_4")) and pd.notna(row.get("so_tin_chi")):
                diem_cl = round(float(row["diem_he_4"]) * float(row["so_tin_chi"]), 4)

            result = conn.execute(text("""
                INSERT INTO fact_hoc_tap (
                    sinh_vien_key, hoc_phan_key, giang_vien_key,
                    hoc_ky_key, date_key,
                    ma_sinh_vien, ma_hoc_phan, ma_dang_ky,
                    diem_chuyen_can, diem_bai_tap,
                    diem_giua_ky,   diem_cuoi_ky,
                    diem_tong_ket,  diem_he_4,
                    diem_chu, dat_mon, hoc_lai,
                    so_tin_chi, diem_chat_luong
                ) VALUES (
                    :sv_sk, :hp_sk, :gv_sk,
                    :hk_sk, :date_key,
                    :msv, :mhp, :ma_dk,
                    :cc, :bt, :gk, :ck,
                    :dtk, :he4,
                    :chu, :dat, :hoc_lai,
                    :tc, :chat_luong
                )
                ON CONFLICT (ma_sinh_vien, ma_hoc_phan, hoc_ky_key) DO NOTHING
            """), {
                "sv_sk"     : sv_sk,
                "hp_sk"     : hp_sk,
                "gv_sk"     : gv_sk,
                "hk_sk"     : hk_sk,
                "date_key"  : date_key,
                "msv"       : row["ma_sinh_vien"],
                "mhp"       : row["ma_hoc_phan"],
                "ma_dk"     : row.get("ma_dang_ky"),
                "cc"        : row.get("diem_chuyen_can"),
                "bt"        : row.get("diem_bai_tap"),
                "gk"        : row.get("diem_giua_ky"),
                "ck"        : row.get("diem_cuoi_ky"),
                "dtk"       : row.get("diem_tong_ket"),
                "he4"       : row.get("diem_he_4"),
                "chu"       : row.get("diem_chu"),
                "dat"       : bool(row.get("dat_mon", False)),
                "hoc_lai"   : bool(row.get("hoc_lai", False)),
                "tc"        : row.get("so_tin_chi", 3),
                "chat_luong": diem_cl,
            })
            inserted += result.rowcount

    total = _count_table("fact_hoc_tap")
    logger.info(f"  → fact_hoc_tap: +{inserted} mới, {skipped} bỏ qua | Tổng: {total}")


# ══════════════════════════════════════════════════════════════════
# PHẦN 3 — LOAD AGGREGATE
# ══════════════════════════════════════════════════════════════════

def load_agg_summary(df_agg: pd.DataFrame):
    """
    Cập nhật agg_student_summary — tổng hợp từ 3 nguồn.

    Input: DataFrame từ calculate_agg_summary() (không phải calculate_gpa).
    calculate_agg_summary() đã merge GPA (Nguồn 1) + RL (Nguồn 2) + HP (Nguồn 3).

    Cột bắt buộc từ df_agg:
      ma_sinh_vien, gpa_he_4, gpa_he_10, xep_loai,
      tong_tc, tc_tich_luy, tong_mon, so_mon_truot,
      diem_rl_tb, xep_loai_rl_last,         ← Nguồn 2
      tong_no, co_no_hp, co_mien_giam,       ← Nguồn 3
      muc_do_rui_ro, canh_bao
    """
    logger.info(f"Load agg_student_summary: {len(df_agg)} rows...")

    with wh_engine.begin() as conn:
        for _, row in df_agg.iterrows():
            sv_sk = conn.execute(text("""
                SELECT sinh_vien_key FROM dim_sinh_vien
                WHERE ma_sinh_vien    = :msv
                  AND la_ban_hien_tai = TRUE
            """), {"msv": row["ma_sinh_vien"]}).scalar()

            if sv_sk is None:
                continue

            tong_tc  = int(row.get("tong_tc", 0))
            tc_dat   = int(row.get("tc_tich_luy", 0))
            tc_k_dat = tong_tc - tc_dat
            tong_mon = int(row.get("tong_mon", 0))
            so_truot = int(row.get("so_mon_truot", 0))
            so_dat   = tong_mon - so_truot
            ty_le    = round(so_dat / tong_mon, 4) if tong_mon > 0 else 0.0
            gpa4     = float(row.get("gpa_he_4", 0) or 0)

            conn.execute(text("""
                INSERT INTO agg_student_summary (
                    sinh_vien_key, ma_sinh_vien,
                    gpa_he_4, gpa_he_10, xep_loai_hoc_luc,
                    tong_tin_chi_dang_ky, tin_chi_dat,
                    tin_chi_khong_dat, ty_le_dat,
                    tong_mon_dang_ky, so_mon_dat, so_mon_khong_dat,
                    diem_rl_trung_binh, xep_loai_rl_gan_nhat,
                    tong_no_hoc_phi, co_no_hoc_phi, duoc_mien_giam,
                    canh_bao_hoc_vu, muc_do_rui_ro, co_the_tot_nghiep,
                    ngay_cap_nhat
                ) VALUES (
                    :sv_sk, :msv,
                    :gpa4, :gpa10, :xep_loai,
                    :tong_tc, :tc_dat, :tc_k_dat, :ty_le,
                    :tong_mon, :so_dat, :so_truot,
                    :drl_tb, :xep_rl,
                    :tong_no, :co_no, :mien_giam,
                    :canh_bao, :rui_ro, :co_the_tn,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (ma_sinh_vien) DO UPDATE SET
                    gpa_he_4             = EXCLUDED.gpa_he_4,
                    gpa_he_10            = EXCLUDED.gpa_he_10,
                    xep_loai_hoc_luc     = EXCLUDED.xep_loai_hoc_luc,
                    tong_tin_chi_dang_ky = EXCLUDED.tong_tin_chi_dang_ky,
                    tin_chi_dat          = EXCLUDED.tin_chi_dat,
                    tin_chi_khong_dat    = EXCLUDED.tin_chi_khong_dat,
                    ty_le_dat            = EXCLUDED.ty_le_dat,
                    tong_mon_dang_ky     = EXCLUDED.tong_mon_dang_ky,
                    so_mon_dat           = EXCLUDED.so_mon_dat,
                    so_mon_khong_dat     = EXCLUDED.so_mon_khong_dat,
                    diem_rl_trung_binh   = EXCLUDED.diem_rl_trung_binh,
                    xep_loai_rl_gan_nhat = EXCLUDED.xep_loai_rl_gan_nhat,
                    tong_no_hoc_phi      = EXCLUDED.tong_no_hoc_phi,
                    co_no_hoc_phi        = EXCLUDED.co_no_hoc_phi,
                    duoc_mien_giam       = EXCLUDED.duoc_mien_giam,
                    canh_bao_hoc_vu      = EXCLUDED.canh_bao_hoc_vu,
                    muc_do_rui_ro        = EXCLUDED.muc_do_rui_ro,
                    co_the_tot_nghiep    = EXCLUDED.co_the_tot_nghiep,
                    ngay_cap_nhat        = CURRENT_TIMESTAMP
            """), {
                "sv_sk"    : sv_sk,
                "msv"      : row["ma_sinh_vien"],
                "gpa4"     : gpa4,
                "gpa10"    : float(row.get("gpa_he_10", 0) or 0),
                "xep_loai" : row.get("xep_loai"),
                "tong_tc"  : tong_tc,
                "tc_dat"   : tc_dat,
                "tc_k_dat" : tc_k_dat,
                "ty_le"    : ty_le,
                "tong_mon" : tong_mon,
                "so_dat"   : so_dat,
                "so_truot" : so_truot,
                # Nguồn 2 — từ calculate_agg_summary()
                "drl_tb"   : float(row.get("diem_rl_tb", 0) or 0),
                "xep_rl"   : row.get("xep_loai_rl_last", ""),
                # Nguồn 3 — từ calculate_agg_summary()
                "tong_no"  : int(row.get("tong_no", 0) or 0),
                "co_no"    : bool(row.get("co_no_hp", False)),
                "mien_giam": bool(row.get("co_mien_giam", False)),
                # Tổng hợp
                "canh_bao" : bool(row.get("canh_bao", False)),
                "rui_ro"   : row.get("muc_do_rui_ro", "Thấp"),  # FIX: muc_nguy_co → muc_do_rui_ro
                "co_the_tn": tc_dat >= 130,
            })

    total = _count_table("agg_student_summary")
    logger.info(f"  → agg_student_summary: {total} bản ghi")


# ══════════════════════════════════════════════════════════════════
# CHẠY TOÀN BỘ PIPELINE LOAD
# ══════════════════════════════════════════════════════════════════

def load_fact_ctsv(df: pd.DataFrame):
    """
    Load dữ liệu từ CSV Phòng Công tác Sinh viên vào fact_ctsv.

    Bảng fact_ctsv đã được tạo bởi create_facts.sql — KHÔNG tạo lại ở đây.
    Cần lookup surrogate key: sinh_vien_key, hoc_ky_key từ dimension.

    Mỗi dòng = 1 sinh viên × 1 học kỳ, gồm:
      - diem_rl, xep_loai_rl   : điểm rèn luyện
      - loai_hoc_bong, muc_tien: học bổng (nếu có)
      - hinh_thuc_kl, ly_do_kl : kỷ luật (nếu có)

    Input: DataFrame từ transform_ctsv() — cột ma_hoc_ky (đã rename)
    """
    logger.info(f"Load fact_ctsv: {len(df)} rows...")
    inserted = 0
    skipped  = 0

    with wh_engine.begin() as conn:
        for _, row in df.iterrows():
            # Lookup sinh_vien_key từ dim_sinh_vien
            sv_sk = conn.execute(text("""
                SELECT sinh_vien_key FROM dim_sinh_vien
                WHERE ma_sinh_vien    = :msv
                  AND la_ban_hien_tai = TRUE
            """), {"msv": row["ma_sinh_vien"]}).scalar()
            if sv_sk is None:
                skipped += 1
                continue

            # Lookup hoc_ky_key từ dim_hoc_ky
            hk_sk = _lookup_key(conn, "dim_hoc_ky", "hoc_ky_key",
                                 "ma_hoc_ky", row["ma_hoc_ky"])
            if hk_sk is None:
                skipped += 1
                continue

            result = conn.execute(text("""
                INSERT INTO fact_ctsv (
                    sinh_vien_key, hoc_ky_key,
                    ma_sinh_vien,  ma_hoc_ky,
                    diem_rl,       xep_loai_rl,
                    loai_hoc_bong, muc_tien_hb,
                    hinh_thuc_kl,  ly_do_kl,
                    co_hoc_bong,   bi_ky_luat
                ) VALUES (
                    :sv_sk, :hk_sk,
                    :msv,   :ma_hk,
                    :diem_rl, :xep_loai,
                    :loai_hb, :muc_tien,
                    :hinh_thuc_kl, :ly_do_kl,
                    :co_hb, :bi_kl
                )
                ON CONFLICT (ma_sinh_vien, hoc_ky_key) DO UPDATE SET
                    diem_rl       = EXCLUDED.diem_rl,
                    xep_loai_rl   = EXCLUDED.xep_loai_rl,
                    loai_hoc_bong = EXCLUDED.loai_hoc_bong,
                    muc_tien_hb   = EXCLUDED.muc_tien_hb,
                    hinh_thuc_kl  = EXCLUDED.hinh_thuc_kl,
                    ly_do_kl      = EXCLUDED.ly_do_kl,
                    co_hoc_bong   = EXCLUDED.co_hoc_bong,
                    bi_ky_luat    = EXCLUDED.bi_ky_luat,
                    ngay_load     = CURRENT_TIMESTAMP
            """), {
                "sv_sk"        : sv_sk,
                "hk_sk"        : hk_sk,
                "msv"          : row["ma_sinh_vien"],
                "ma_hk"        : row["ma_hoc_ky"],
                "diem_rl"      : int(row.get("diem_rl", 0) or 0),
                "xep_loai"     : row.get("xep_loai_rl", ""),
                "loai_hb"      : row.get("loai_hoc_bong", ""),
                "muc_tien"     : int(row.get("muc_tien_hb", 0) or 0),
                "hinh_thuc_kl" : row.get("hinh_thuc_kl", ""),
                "ly_do_kl"     : row.get("ly_do_kl", ""),
                "co_hb"        : bool(row.get("co_hoc_bong", False)),
                "bi_kl"        : bool(row.get("bi_ky_luat", False)),
            })
            inserted += result.rowcount

    logger.info(f"  → fact_ctsv: +{inserted} | bỏ qua {skipped} | Tổng: {_count_table('fact_ctsv')}")


def load_fact_tai_chinh(df: pd.DataFrame):
    """
    Load dữ liệu từ API Portal Tài chính vào fact_tai_chinh.

    Bảng fact_tai_chinh đã được tạo bởi create_facts.sql — KHÔNG tạo lại ở đây.
    Cần lookup surrogate key: sinh_vien_key, hoc_ky_key từ dimension.

    Mỗi dòng = 1 sinh viên × 1 học kỳ, gồm:
      - hoc_phi_phai_dong, da_dong, con_no
      - duoc_mien_giam, ly_do_mien_giam, so_tien_mien_giam
      - ngay_dong_cuoi

    Input: DataFrame từ transform_tai_chinh() — cột ma_hoc_ky (đã rename)
    """
    logger.info(f"Load fact_tai_chinh: {len(df)} rows...")
    inserted = 0
    skipped  = 0

    with wh_engine.begin() as conn:
        for _, row in df.iterrows():
            # Lookup sinh_vien_key từ dim_sinh_vien
            sv_sk = conn.execute(text("""
                SELECT sinh_vien_key FROM dim_sinh_vien
                WHERE ma_sinh_vien    = :msv
                  AND la_ban_hien_tai = TRUE
            """), {"msv": row["ma_sinh_vien"]}).scalar()
            if sv_sk is None:
                skipped += 1
                continue

            # Lookup hoc_ky_key từ dim_hoc_ky
            hk_sk = _lookup_key(conn, "dim_hoc_ky", "hoc_ky_key",
                                 "ma_hoc_ky", row["ma_hoc_ky"])
            if hk_sk is None:
                skipped += 1
                continue

            # Xử lý ngay_dong_cuoi — có thể NULL (chưa đóng)
            ngay_dong = None
            val = row.get("ngay_dong_cuoi")
            if val and str(val).strip() not in ("", "None", "NaT", "nan"):
                try:
                    ngay_dong = pd.to_datetime(str(val)).date()
                except Exception:
                    pass

            result = conn.execute(text("""
                INSERT INTO fact_tai_chinh (
                    sinh_vien_key,  hoc_ky_key,
                    ma_sinh_vien,   ma_hoc_ky,
                    hoc_phi_phai_dong, da_dong, con_no,
                    duoc_mien_giam, ly_do_mien_giam, so_tien_mien_giam,
                    ngay_dong_cuoi
                ) VALUES (
                    :sv_sk, :hk_sk,
                    :msv,   :ma_hk,
                    :phai_dong, :da_dong, :con_no,
                    :mien_giam, :ly_do_mg, :so_tien_mg,
                    :ngay_dong
                )
                ON CONFLICT (ma_sinh_vien, hoc_ky_key) DO UPDATE SET
                    hoc_phi_phai_dong = EXCLUDED.hoc_phi_phai_dong,
                    da_dong           = EXCLUDED.da_dong,
                    con_no            = EXCLUDED.con_no,
                    duoc_mien_giam    = EXCLUDED.duoc_mien_giam,
                    ly_do_mien_giam   = EXCLUDED.ly_do_mien_giam,
                    so_tien_mien_giam = EXCLUDED.so_tien_mien_giam,
                    ngay_dong_cuoi    = EXCLUDED.ngay_dong_cuoi,
                    ngay_load         = CURRENT_TIMESTAMP
            """), {
                "sv_sk"      : sv_sk,
                "hk_sk"      : hk_sk,
                "msv"        : row["ma_sinh_vien"],
                "ma_hk"      : row["ma_hoc_ky"],
                "phai_dong"  : int(row.get("hoc_phi_phai_dong", 0) or 0),
                "da_dong"    : int(row.get("da_dong", 0) or 0),
                "con_no"     : int(row.get("con_no", 0) or 0),
                "mien_giam"  : bool(row.get("duoc_mien_giam", False)),
                "ly_do_mg"   : row.get("ly_do_mien_giam", ""),
                "so_tien_mg" : int(row.get("so_tien_mien_giam", 0) or 0),
                "ngay_dong"  : ngay_dong,
            })
            inserted += result.rowcount

    logger.info(f"  → fact_tai_chinh: +{inserted} | bỏ qua {skipped} | Tổng: {_count_table('fact_tai_chinh')}")


# ══════════════════════════════════════════════════════════════════
# CHẠY TOÀN BỘ PIPELINE LOAD
# ══════════════════════════════════════════════════════════════════

def run_full_load(df_hk, df_gv, df_hp, df_sv, df_diem, df_agg,
                  df_ctsv=None, df_tai_chinh=None):
    """
    Chạy toàn bộ bước load theo đúng thứ tự FK.

    Tham số bắt buộc (từ PostgreSQL source):
        df_hk   : DataFrame bảng hoc_ky_nam_hoc
        df_gv   : DataFrame từ transform_giang_vien()
        df_hp   : DataFrame bảng hoc_phan
        df_sv   : DataFrame từ transform_sinh_vien()
        df_diem : DataFrame từ transform_diem()
        df_agg  : DataFrame từ calculate_agg_summary() — tổng hợp 3 nguồn
                  (KHÔNG phải calculate_gpa, phải merge xong 3 nguồn trước)

    Tham số tùy chọn (nguồn bổ sung — pipeline vẫn chạy nếu thiếu):
        df_ctsv      : DataFrame từ transform_ctsv()       (Nguồn 2 CSV)
        df_tai_chinh : DataFrame từ transform_tai_chinh()  (Nguồn 3 API)
    """
    logger.info("=" * 55)
    logger.info("BẮT ĐẦU LOAD VÀO WAREHOUSE — 3 NGUỒN")
    logger.info("=" * 55)

    # Dimensions (phải load trước facts)
    load_dim_hoc_ky(df_hk)
    load_dim_giang_vien(df_gv)
    load_dim_hoc_phan(df_hp)
    load_dim_sinh_vien(df_sv)

    # Fact Nguồn 1: PostgreSQL
    load_fact_hoc_tap(df_diem)

    # Fact Nguồn 2: CSV CTSV
    if df_ctsv is not None and not df_ctsv.empty:
        load_fact_ctsv(df_ctsv)
    else:
        logger.warning("Bỏ qua load_fact_ctsv: không có dữ liệu CSV CTSV")

    # Fact Nguồn 3: API Portal
    if df_tai_chinh is not None and not df_tai_chinh.empty:
        load_fact_tai_chinh(df_tai_chinh)
    else:
        logger.warning("Bỏ qua load_fact_tai_chinh: không có dữ liệu API Portal")

    # Aggregate tổng hợp 3 nguồn — load CUỐI CÙNG
    load_agg_summary(df_agg)

    logger.info("=" * 55)
    logger.info("LOAD HOÀN THÀNH ✅")
    logger.info("=" * 55)