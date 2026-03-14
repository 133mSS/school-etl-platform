# src/etl/transform.py
"""
transform.py - Business logic xử lý dữ liệu từ 3 nguồn.

  Nguồn 1 (PostgreSQL):
    transform_sinh_vien()  → dim_sinh_vien
    transform_giang_vien() → dim_giang_vien
    transform_diem()       → fact_hoc_tap
    calculate_gpa()        → agg_student_summary (phần học tập)

  Nguồn 2 (CSV - Phòng CTSV):
    transform_ctsv()       → fact_ctsv

  Nguồn 3 (API Portal):
    transform_tai_chinh()  → fact_tai_chinh

  Tổng hợp:
    calculate_agg_summary() → agg_student_summary (3 nguồn)
"""

import pandas as pd
import numpy as np
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════
# PHẦN 1 — CLEANING
# ══════════════════════════════════════════════════════════════════

def clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip khoảng trắng đầu/cuối các cột string."""
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
            df[col] = df[col].str.strip()
    return df


def remove_duplicates(df: pd.DataFrame, subset: list) -> pd.DataFrame:
    """Xóa bản ghi trùng lặp theo subset, giữ lại bản ghi đầu tiên."""
    before = len(df)
    df     = df.drop_duplicates(subset=subset, keep="first")
    after  = len(df)
    if before != after:
        logger.warning(f"Xóa {before - after} bản ghi trùng lặp (subset={subset})")
    return df


def handle_nulls(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    """
    Xử lý NULL theo quy tắc từng cột.
    rules = { 'ten_mon': 'Không rõ', 'diem': 0.0 }
    """
    df = df.copy()
    for col, default in rules.items():
        if col in df.columns and default is not None:
            n = df[col].isnull().sum()
            if n > 0:
                df[col] = df[col].fillna(default)
                logger.warning(f"Điền NULL '{col}': {n} giá trị → {default}")
    return df


# ══════════════════════════════════════════════════════════════════
# PHẦN 2 — BUSINESS LOGIC
# ══════════════════════════════════════════════════════════════════

def quy_doi_diem_chu(diem) -> tuple:
    """
    Quy đổi điểm hệ 10 → (diem_chu, diem_he_4) theo quy chế PTIT.
    Ví dụ: 8.7 → ('A', 4.0)
    """
    if pd.isna(diem): return None, None
    d = float(diem)
    if d >= 9.0: return "A+", 4.0
    if d >= 8.5: return "A",  4.0
    if d >= 8.0: return "B+", 3.5
    if d >= 7.0: return "B",  3.0
    if d >= 6.5: return "C+", 2.5
    if d >= 5.5: return "C",  2.0
    if d >= 5.0: return "D+", 1.5
    if d >= 4.0: return "D",  1.0
    return "F", 0.0


def xep_loai_hoc_luc(gpa4) -> str:
    """Xếp loại học lực theo GPA hệ 4."""
    if pd.isna(gpa4):  return "Chưa xác định"
    g = float(gpa4)
    if g >= 3.6: return "Xuất sắc"
    if g >= 3.2: return "Giỏi"
    if g >= 2.5: return "Khá"
    if g >= 2.0: return "Trung bình"
    if g >= 1.0: return "Yếu"
    return "Kém"


def tinh_tuoi(ngay_sinh) -> int:
    if pd.isna(ngay_sinh): return None
    today = datetime.today()
    ns    = pd.to_datetime(ngay_sinh)
    return today.year - ns.year - ((today.month, today.day) < (ns.month, ns.day))


def xac_dinh_rui_ro(gpa4, diem_rl, co_no_hp) -> str:
    """
    Xác định mức độ rủi ro tổng hợp từ 3 nguồn.
    Cao        : GPA < 2.0 hoặc RL < 50 hoặc nợ HP
    Trung bình : GPA < 2.5 hoặc RL < 65
    Thấp       : không có tiêu chí nào
    """
    if pd.isna(gpa4): return "Chưa xác định"
    g  = float(gpa4)
    rl = float(diem_rl) if pd.notna(diem_rl) else 100.0
    no = bool(co_no_hp)

    if g < 2.0 or rl < 50 or no:     return "Cao"
    if g < 2.5 or rl < 65:           return "Trung bình"
    return "Thấp"


# ══════════════════════════════════════════════════════════════════
# PHẦN 3 — TRANSFORM NGUỒN 1 (PostgreSQL)
# ══════════════════════════════════════════════════════════════════

def transform_sinh_vien(df_sv: pd.DataFrame,
                        df_lop: pd.DataFrame,
                        df_nganh: pd.DataFrame,
                        df_khoa: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn bị DataFrame sinh viên để load vào dim_sinh_vien.

    v2.0: join thêm lop → nganh → khoa để lấy ten_nganh, ten_khoa,
          ma_co_van (cố vấn của lớp).
    Bỏ: he_dao_tao, hoc_ky_hien_tai (không còn trong source v2.0)

    Input:
        df_sv    : bảng sinh_vien
        df_lop   : bảng lop_hanh_chinh (để lấy ma_co_van, ma_nganh)
        df_nganh : bảng nganh (để lấy ten_nganh)
        df_khoa  : bảng khoa  (để lấy ten_khoa)
    """
    logger.info(f"Transform sinh_vien: {len(df_sv)} rows")
    df = df_sv.copy()

    # 1. Làm sạch
    df = clean_string_columns(df)
    df = remove_duplicates(df, subset=["ma_sinh_vien"])
    df = handle_nulls(df, {
        "gioi_tinh"         : "Không rõ",
        "trang_thai_hoc_tap": "Đang học",
    })

    # 2. Join lop → lấy ma_co_van, ten_lop
    lop_cols = ["ma_lop", "ten_lop", "ma_co_van", "ma_nganh"]
    lop_cols = [c for c in lop_cols if c in df_lop.columns]
    df = df.merge(df_lop[lop_cols], on="ma_lop", how="left",
                  suffixes=("", "_lop"))
    # ma_nganh ưu tiên từ sinh_vien, nếu NULL thì lấy từ lop
    if "ma_nganh_lop" in df.columns:
        df["ma_nganh"] = df["ma_nganh"].fillna(df["ma_nganh_lop"])
        df.drop(columns=["ma_nganh_lop"], inplace=True)

    # 3. Join nganh → lấy ten_nganh, ma_khoa
    df = df.merge(
        df_nganh[["ma_nganh", "ten_nganh", "ma_khoa"]],
        on="ma_nganh", how="left", suffixes=("", "_nganh")
    )

    # 4. Join khoa → lấy ten_khoa
    df = df.merge(
        df_khoa[["ma_khoa", "ten_khoa"]],
        on="ma_khoa", how="left", suffixes=("", "_khoa")
    )

    # 5. Tính toán
    df["ho_ten"] = df["ho"].str.strip() + " " + df["ten"].str.strip()

    # 6. SCD Type 2 fields
    df["ngay_hieu_luc"]     = datetime.today().date()
    df["ngay_het_hieu_luc"] = None
    df["la_ban_hien_tai"]   = True
    df["phien_ban"]         = 1

    logger.info(f"  → {len(df)} rows sau transform")
    return df


def transform_giang_vien(df_gv: pd.DataFrame,
                         df_khoa: pd.DataFrame,
                         df_co_so: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn bị DataFrame giảng viên cho dim_giang_vien."""
    logger.info(f"Transform giang_vien: {len(df_gv)} rows")
    df = df_gv.copy()
    df = clean_string_columns(df)
    df = remove_duplicates(df, subset=["ma_giang_vien"])

    # Join khoa, co_so
    df = df.merge(df_khoa[["ma_khoa", "ten_khoa"]], on="ma_khoa", how="left")
    df = df.merge(df_co_so[["ma_co_so", "ten_co_so"]], on="ma_co_so", how="left")

    df["ho_ten"] = df["ho"].str.strip() + " " + df["ten"].str.strip()
    logger.info(f"  → {len(df)} rows")
    return df


def transform_diem(df_diem: pd.DataFrame,
                   df_dk:   pd.DataFrame,
                   df_hp:   pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn bị DataFrame điểm cho fact_hoc_tap.
    Join: diem ← dang_ky ← hoc_phan (để lấy so_tin_chi, ma_hoc_ky)
    """
    logger.info(f"Transform diem: {len(df_diem)} rows")

    df = df_diem.merge(
        df_dk[["ma_dang_ky", "ma_sinh_vien", "ma_hoc_phan",
               "ma_hoc_ky", "ma_giang_vien"]],
        on="ma_dang_ky", how="left"
    ).merge(
        df_hp[["ma_hoc_phan", "so_tin_chi"]],
        on="ma_hoc_phan", how="left"
    )

    df = clean_string_columns(df)
    df = handle_nulls(df, {"so_tin_chi": 3})

    df["diem_chat_luong"] = (
        pd.to_numeric(df["diem_he_4"],  errors="coerce") *
        pd.to_numeric(df["so_tin_chi"], errors="coerce")
    ).round(4)

    logger.info(f"  → {len(df)} rows")
    return df


def calculate_gpa(df_diem_full: pd.DataFrame) -> pd.DataFrame:
    """
    Tính GPA tổng hợp theo từng sinh viên.
    Trả về DataFrame: ma_sinh_vien + các chỉ số GPA.
    """
    logger.info("Tính GPA...")
    df = df_diem_full.copy()
    df["diem_he_4"]       = pd.to_numeric(df["diem_he_4"],    errors="coerce")
    df["so_tin_chi"]      = pd.to_numeric(df["so_tin_chi"],   errors="coerce")
    df["dat_mon"]         = df["dat_mon"].astype(bool)
    df["diem_chat_luong"] = df["diem_he_4"] * df["so_tin_chi"]

    gpa_df = df.groupby("ma_sinh_vien").apply(
        lambda g: pd.Series({
            "tong_tc"      : g["so_tin_chi"].sum(),
            "tong_cl"      : g["diem_chat_luong"].sum(),
            "tc_tich_luy"  : g.loc[g["dat_mon"], "so_tin_chi"].sum(),
            "so_mon_truot" : (~g["dat_mon"]).sum(),
            "tong_mon"     : len(g),
        })
    ).reset_index()

    gpa_df["gpa_he_4"]    = (gpa_df["tong_cl"] / gpa_df["tong_tc"]).round(2)
    gpa_df["gpa_he_10"]   = (gpa_df["gpa_he_4"] * 2.5).clip(upper=10.0).round(2)
    gpa_df["ty_le_truot"] = gpa_df["so_mon_truot"] / gpa_df["tong_mon"]
    gpa_df["xep_loai"]    = gpa_df["gpa_he_4"].apply(xep_loai_hoc_luc)
    gpa_df["canh_bao"]    = gpa_df["gpa_he_4"] < 2.0

    logger.info(f"  → GPA tính xong cho {len(gpa_df)} SV")
    return gpa_df


# ══════════════════════════════════════════════════════════════════
# PHẦN 4 — TRANSFORM NGUỒN 2 (CSV - Phòng CTSV)
# ══════════════════════════════════════════════════════════════════

# Mapping học kỳ: "HK1-2021-22" → "HK1-2021-22" (đã khớp)
# File CSV dùng cùng format với PostgreSQL → không cần convert

def transform_ctsv(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hóa DataFrame từ CSV Phòng CTSV để load vào fact_ctsv.

    Xử lý:
      - Strip string, điền NULL
      - Tính cờ co_hoc_bong, bi_ky_luat
      - Đảm bảo diem_rl trong khoảng 0-100
      - muc_tien_hb: NULL → 0
      - Rename hoc_ky → ma_hoc_ky (cuối cùng, SAU khi remove_duplicates)

    Input : DataFrame thô từ extract_csv_ctsv()
    Output: DataFrame sẵn sàng load — cột ma_hoc_ky (không phải hoc_ky)
    """
    logger.info(f"Transform CTSV: {len(df_raw)} rows")
    df = df_raw.copy()

    # 1. Làm sạch
    df = clean_string_columns(df)
    # Dùng tên gốc 'hoc_ky' (chưa rename) để remove_duplicates đúng
    df = remove_duplicates(df, subset=["ma_sinh_vien", "hoc_ky"])

    # 2. Xử lý NULL
    df = handle_nulls(df, {
        "diem_rl"      : 0,
        "xep_loai_rl"  : "Chưa xếp loại",
        "loai_hoc_bong": "",
        "muc_tien_hb"  : 0,
        "hinh_thuc_kl" : "",
        "ly_do_kl"     : "",
    })

    # 3. Đảm bảo kiểu số
    df["diem_rl"]     = pd.to_numeric(df["diem_rl"],     errors="coerce").clip(0, 100).fillna(0).astype(int)
    df["muc_tien_hb"] = pd.to_numeric(df["muc_tien_hb"], errors="coerce").fillna(0).astype(int)

    # 4. Cờ tổng hợp
    df["co_hoc_bong"] = df["loai_hoc_bong"].str.strip().ne("") & df["loai_hoc_bong"].notna()
    df["bi_ky_luat"]  = df["hinh_thuc_kl"].str.strip().ne("") & df["hinh_thuc_kl"].notna()

    # 5. Rename hoc_ky → ma_hoc_ky CUỐI CÙNG (sau tất cả bước trên)
    df = df.rename(columns={"hoc_ky": "ma_hoc_ky"})

    logger.info(f"  → {len(df)} rows sau transform")
    logger.info(f"     Co HB: {df['co_hoc_bong'].sum()} | Bi KL: {df['bi_ky_luat'].sum()}")
    return df


# ══════════════════════════════════════════════════════════════════
# PHẦN 5 — TRANSFORM NGUỒN 3 (API Portal Tài chính)
# ══════════════════════════════════════════════════════════════════

def transform_tai_chinh(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hóa DataFrame từ API Portal để load vào fact_tai_chinh.

    API trả về JSON → DataFrame, cần:
      - Đảm bảo kiểu số (BigInteger cho tiền)
      - Xử lý ngay_dong_cuoi: string → date
      - Điền NULL các cột tiền về 0
      - Rename hoc_ky → ma_hoc_ky (CUỐI CÙNG, sau remove_duplicates)

    Input : DataFrame thô từ extract_api_tai_chinh()
    Output: DataFrame sẵn sàng load — cột ma_hoc_ky (không phải hoc_ky)
    """
    logger.info(f"Transform Tài chính: {len(df_raw)} rows")
    df = df_raw.copy()

    # 1. Làm sạch
    df = clean_string_columns(df)
    # Dùng tên gốc 'hoc_ky' (chưa rename) để remove_duplicates đúng
    df = remove_duplicates(df, subset=["ma_sinh_vien", "hoc_ky"])

    # 2. Kiểu số cho các cột tiền
    money_cols = ["hoc_phi_phai_dong", "da_dong", "con_no", "so_tien_mien_giam"]
    for col in money_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # 3. Convert ngay_dong_cuoi: string → date
    if "ngay_dong_cuoi" in df.columns:
        df["ngay_dong_cuoi"] = pd.to_datetime(
            df["ngay_dong_cuoi"], errors="coerce"
        ).dt.date
        # Nếu NULL (chưa đóng) → giữ NULL, không điền

    # 4. Boolean
    if "duoc_mien_giam" in df.columns:
        df["duoc_mien_giam"] = df["duoc_mien_giam"].astype(bool)

    # 5. Xử lý NULL chuỗi
    df = handle_nulls(df, {
        "ly_do_mien_giam"  : "",
        "so_tien_mien_giam": 0,
    })

    # 6. Rename hoc_ky → ma_hoc_ky CUỐI CÙNG (sau tất cả bước trên)
    df = df.rename(columns={"hoc_ky": "ma_hoc_ky"})

    logger.info(f"  → {len(df)} rows sau transform")
    logger.info(f"     Co no HP : {(df['con_no'] > 0).sum()}")
    logger.info(f"     Mien giam: {df['duoc_mien_giam'].sum()}")
    return df


# ══════════════════════════════════════════════════════════════════
# PHẦN 6 — TỔNG HỢP AGG_STUDENT_SUMMARY (3 nguồn)
# ══════════════════════════════════════════════════════════════════

def calculate_agg_summary(df_gpa:     pd.DataFrame,
                          df_ctsv:    pd.DataFrame,
                          df_tc:      pd.DataFrame) -> pd.DataFrame:
    """
    Tính toán agg_student_summary từ 3 nguồn.

    Tham số:
        df_gpa  : kết quả từ calculate_gpa()  (Nguồn 1)
        df_ctsv : kết quả transform_ctsv()     (Nguồn 2)
        df_tc   : kết quả transform_tai_chinh() (Nguồn 3)

    Trả về: DataFrame tổng hợp cho mỗi sinh viên
    """
    logger.info("Tính agg_student_summary từ 3 nguồn...")

    # --- Nguồn 2: Tổng hợp RL trung bình theo SV ---
    rl_agg = df_ctsv.groupby("ma_sinh_vien").agg(
        diem_rl_tb      = ("diem_rl",       "mean"),
        xep_loai_rl_last= ("xep_loai_rl",   "last"),
    ).reset_index()
    rl_agg["diem_rl_tb"] = rl_agg["diem_rl_tb"].round(1)

    # --- Nguồn 3: Tổng hợp tài chính theo SV ---
    tc_agg = df_tc.groupby("ma_sinh_vien").agg(
        tong_no          = ("con_no",        "sum"),
        co_mien_giam     = ("duoc_mien_giam", "any"),
    ).reset_index()
    tc_agg["co_no_hp"] = tc_agg["tong_no"] > 0

    # --- Merge 3 nguồn ---
    result = df_gpa.copy()
    result = result.merge(rl_agg, on="ma_sinh_vien", how="left")
    result = result.merge(tc_agg, on="ma_sinh_vien", how="left")

    # Điền NULL cho SV chưa có dữ liệu từ nguồn 2/3
    result["diem_rl_tb"]       = result["diem_rl_tb"].fillna(0)
    result["xep_loai_rl_last"] = result["xep_loai_rl_last"].fillna("Chưa có")
    result["tong_no"]          = result["tong_no"].fillna(0)
    result["co_no_hp"]         = result["co_no_hp"].fillna(False)
    result["co_mien_giam"]     = result["co_mien_giam"].fillna(False)

    # Đánh giá rủi ro tổng hợp
    result["muc_do_rui_ro"] = result.apply(
        lambda r: xac_dinh_rui_ro(r["gpa_he_4"], r["diem_rl_tb"], r["co_no_hp"]),
        axis=1
    )
    result["canh_bao"] = (result["gpa_he_4"] < 2.0) | (result["diem_rl_tb"] < 50)

    logger.info(f"  → agg_summary tính xong cho {len(result)} SV")
    return result