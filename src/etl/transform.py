# src/etl/transform.py

from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

from src.etl.extract import ExtractedData
from src.utils.logger import get_logger
from src.utils.minio_client import MinIOClient

logger = get_logger("etl.transform")


# ─────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────

GRADE_SCALE = [
    (9.5, 10.01, "A+", 4.0),
    (8.5,  9.5,  "A",  3.7),
    (8.0,  8.5,  "B+", 3.5),
    (7.0,  8.0,  "B",  3.0),
    (6.5,  7.0,  "C+", 2.5),
    (5.5,  6.5,  "C",  2.0),
    (5.0,  5.5,  "D+", 1.5),
    (4.0,  5.0,  "D",  1.0),
    (0.0,  4.0,  "F",  0.0),
]

WEIGHT_CHUYEN_CAN = 0.10
WEIGHT_BAI_TAP    = 0.10
WEIGHT_GIUA_KY    = 0.20
WEIGHT_CUOI_KY    = 0.60

RL_THRESHOLDS = [
    (90, 101, "Xuất sắc"),
    (80,  90, "Tốt"),
    (65,  80, "Khá"),
    (50,  65, "Trung bình"),
    (0,   50, "Yếu"),
]


# ─────────────────────────────────────────────────────────────────
# TRANSFORMED DATA
# ─────────────────────────────────────────────────────────────────

@dataclass
class TransformedData:
    dim_giang_vien: pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_sinh_vien:  pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_hoc_phan:   pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_thoi_gian:  pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_diem:      pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_ren_luyen: pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_tai_chinh: pd.DataFrame = field(default_factory=pd.DataFrame)

    def summary(self) -> Dict[str, int]:
        counts = {}
        for attr in vars(self):
            val = getattr(self, attr)
            if isinstance(val, pd.DataFrame):
                counts[attr] = len(val)
        return counts

# ─────────────────────────────────────────────────────────────────
# MAIN CLASS
# ─────────────────────────────────────────────────────────────────

class DataTransformer:

    def __init__(self):
        self._last_run_id: str = ""
        self._helper = _TransformHelper()

    # ─────────────────────────────────────────────────────────────
    # MAIN ENTRY
    # ─────────────────────────────────────────────────────────────

    def transform_all(self, extracted: ExtractedData) -> TransformedData:
        logger.info("=" * 70)
        logger.info("BẮT ĐẦU TRANSFORM")
        logger.info("=" * 70)

        result = TransformedData()

        # ── Bước 1: Dimension Tables ──────────────────────────────
        logger.info("── Bước 1: Transform Dimension Tables ──")

        result.dim_thoi_gian  = self._transform_dim_hoc_ky(
            extracted.hoc_ky_nam_hoc
        )
        result.dim_giang_vien = self._transform_dim_giang_vien(
            extracted.giang_vien,
            extracted.khoa,
        )
        result.dim_hoc_phan   = self._transform_dim_hoc_phan(
            extracted.hoc_phan,
            extracted.khoa,
        )
        result.dim_sinh_vien  = self._transform_dim_sinh_vien(
            extracted.sinh_vien,
            extracted.nganh,
            extracted.lop_hanh_chinh,
            extracted.khoa,
            extracted.giang_vien,
        )

        # ── Bước 2: Fact Tables ───────────────────────────────────
        logger.info("── Bước 2: Transform Fact Tables ──")

        result.fact_diem      = self._transform_fact_diem(
            extracted.diem_hoc_phan,
            extracted.dang_ky_hoc_phan,
        )
        result.fact_ren_luyen = self._transform_fact_ren_luyen(
            extracted.ctsv_data
        )
        result.fact_tai_chinh = self._transform_fact_tai_chinh(
            extracted.tai_chinh_data
        )

        # ── Summary ───────────────────────────────────────────────
        logger.info("=" * 70)
        logger.info("TRANSFORM HOÀN TẤT")
        for name, count in result.summary().items():
            logger.info(f"  {name:<25s}: {count:>8,} records")
        logger.info("=" * 70)

        self._save_to_staging(result)
        return result
    # ─────────────────────────────────────────────────────────────
    # DIMENSION TRANSFORMS
    # ─────────────────────────────────────────────────────────────

    def _transform_dim_hoc_ky(self, hk_df: pd.DataFrame) -> pd.DataFrame:
        TABLE = "dim_hoc_ky"

        if hk_df.empty:
            logger.warning(f"[{TABLE}] DataFrame rỗng → trả về empty")
            return pd.DataFrame()

        result = hk_df[[
            "ma_hoc_ky", "nam_hoc", "hoc_ky",
            "ngay_bat_dau", "ngay_ket_thuc",
        ]].copy()

        # ── Cast datetime ─────────────────────────────────────────
        result["ngay_bat_dau"] = pd.to_datetime(
            result["ngay_bat_dau"], errors="coerce"
        )
        result["ngay_ket_thuc"] = pd.to_datetime(
            result["ngay_ket_thuc"], errors="coerce"
        )
        result["nam_bat_dau"] = result["ngay_bat_dau"].dt.year
        result["nam_ket_thuc"] = result["ngay_ket_thuc"].dt.year

        # ── Làm sạch string ───────────────────────────────────────
        result["nam_hoc"] = result["nam_hoc"].fillna("").astype(str).str.strip()
        result["hoc_ky"]  = result["hoc_ky"].fillna("").astype(str).str.strip()

        # ── Assert critical fields ────────────────────────────────
        # ma_hoc_ky không được NULL
        _TransformHelper.assert_no_null(result, ["ma_hoc_ky"], TABLE)

        # nam_hoc, hoc_ky không được rỗng
        for col in ["nam_hoc", "hoc_ky"]:
            empty_count = int((result[col] == "").sum())
            if empty_count > 0:
                raise TransformError(
                    f"[{TABLE}] cột '{col}' có {empty_count} giá trị rỗng. "
                    f"Kiểm tra nguồn hoc_ky_nam_hoc."
                )

        # ── Dedup ────────────────────────────────────────────────
        before = len(result)
        result = result.drop_duplicates(subset=["ma_hoc_ky"])
        after  = len(result)
        if before != after:
            logger.warning(
                f"[{TABLE}] Đã drop {before - after} duplicate ma_hoc_ky"
            )

        _TransformHelper.log_transform_result(TABLE, result)
        return result

    # ─────────────────────────────────────────────────────────────

    def _transform_dim_giang_vien(
        self,
        gv_df:   pd.DataFrame,
        khoa_df: pd.DataFrame,
    ) -> pd.DataFrame:
        TABLE = "dim_giang_vien"

        if gv_df.empty:
            logger.warning(f"[{TABLE}] DataFrame rỗng → trả về empty")
            return pd.DataFrame()

        cols_needed = [
            "ma_giang_vien", "ho", "ten", "email",
            "so_dien_thoai", "chuc_danh",
            "trang_thai_cong_tac", "ma_khoa",
        ]
        existing_cols = [c for c in cols_needed if c in gv_df.columns]
        result = gv_df[existing_cols].copy()

        # ── Assert ma_giang_vien không NULL ──────────────────────
        _TransformHelper.assert_no_null(result, ["ma_giang_vien"], TABLE)

        # ── Ghép ho_ten an toàn ───────────────────────────────────
        result = _TransformHelper.build_full_name(result, "ho", "ten", "ho_ten")

        # ── Join khoa (optional, allow_missing=True) ──────────────
        if "ma_khoa" in result.columns:
            result = _TransformHelper.left_join_with_check(
                left        = result,
                right       = khoa_df,
                on          = "ma_khoa",
                cols        = ["ma_khoa", "ten_khoa"],
                check_col   = "ten_khoa",
                table_name  = f"{TABLE} ← khoa",
                allow_missing = True,   # khoa là optional info
            )

        # ── Dedup ─────────────────────────────────────────────────
        before = len(result)
        result = result.drop_duplicates(subset=["ma_giang_vien"])
        after  = len(result)
        if before != after:
            logger.warning(
                f"[{TABLE}] Đã drop {before - after} duplicate ma_giang_vien"
            )

        _TransformHelper.log_transform_result(TABLE, result)
        return result

    # ─────────────────────────────────────────────────────────────

    def _transform_dim_hoc_phan(
        self,
        hp_df:   pd.DataFrame,
        khoa_df: pd.DataFrame,
    ) -> pd.DataFrame:
        TABLE = "dim_hoc_phan"

        if hp_df.empty:
            logger.warning(f"[{TABLE}] DataFrame rỗng → trả về empty")
            return pd.DataFrame()

        cols_needed = [
            "ma_hoc_phan", "ma_mon", "ten_mon", "so_tin_chi",
            "so_gio_ly_thuyet", "so_gio_thuc_hanh",
            "hoc_ky_de_xuat", "bat_buoc", "ma_khoa",
        ]
        existing_cols = [c for c in cols_needed if c in hp_df.columns]
        result = hp_df[existing_cols].copy()

        # ── Assert ma_hoc_phan không NULL ────────────────────────
        _TransformHelper.assert_no_null(result, ["ma_hoc_phan"], TABLE)

        # ── Tính loai_hoc_phan ────────────────────────────────────
        if "bat_buoc" in result.columns:
            result["loai_hoc_phan"] = result["bat_buoc"].apply(
                lambda x: "Bat buoc" if x else "Tu chon"
            )
        else:
            result["loai_hoc_phan"] = "Bat buoc"

        # ── Validate so_tin_chi ───────────────────────────────────
        if "so_tin_chi" in result.columns:
            result["so_tin_chi"] = pd.to_numeric(
                result["so_tin_chi"], errors="coerce"
            )
            invalid_tc = result[
                result["so_tin_chi"].isna() |
                (result["so_tin_chi"] < 1) |
                (result["so_tin_chi"] > 10)
            ]
            if len(invalid_tc) > 0:
                logger.warning(
                    f"[{TABLE}] {len(invalid_tc)} records có so_tin_chi "
                    f"không hợp lệ (ngoài [1,10] hoặc NULL)"
                )

        # ── Join khoa (optional) ──────────────────────────────────
        if "ma_khoa" in result.columns:
            result = _TransformHelper.left_join_with_check(
                left          = result,
                right         = khoa_df,
                on            = "ma_khoa",
                cols          = ["ma_khoa", "ten_khoa"],
                check_col     = "ten_khoa",
                table_name    = f"{TABLE} ← khoa",
                allow_missing = True,
            )

        # ── Dedup ─────────────────────────────────────────────────
        before = len(result)
        result = result.drop_duplicates(subset=["ma_hoc_phan"])
        after  = len(result)
        if before != after:
            logger.warning(
                f"[{TABLE}] Đã drop {before - after} duplicate ma_hoc_phan"
            )

        _TransformHelper.log_transform_result(TABLE, result)
        return result

    # ─────────────────────────────────────────────────────────────

    def _transform_dim_sinh_vien(
        self,
        sv_df:   pd.DataFrame,
        nganh_df: pd.DataFrame,
        lop_df:   pd.DataFrame,
        khoa_df:  pd.DataFrame,
        gv_df:    pd.DataFrame,
    ) -> pd.DataFrame:
        TABLE = "dim_sinh_vien"

        if sv_df.empty:
            logger.warning(f"[{TABLE}] DataFrame rỗng → trả về empty")
            return pd.DataFrame()

        result = sv_df[[
            "ma_sinh_vien", "ho", "ten", "ngay_sinh",
            "gioi_tinh", "email", "ma_nganh", "ma_lop",
            "khoa_hoc", "trang_thai_hoc_tap",
        ]].copy()

        # ── Assert ma_sinh_vien không NULL (critical) ─────────────
        _TransformHelper.assert_no_null(result, ["ma_sinh_vien"], TABLE)

        # ── Ghép ho_ten an toàn ───────────────────────────────────
        result = _TransformHelper.build_full_name(result, "ho", "ten", "ho_ten")

        # ── Join nganh (critical: sinh viên phải có ngành) ────────
        if not nganh_df.empty and "ma_nganh" in result.columns:
            nganh_cols = ["ma_nganh", "ten_nganh"]
            if "ma_khoa" in nganh_df.columns:
                nganh_cols.append("ma_khoa")

            result = _TransformHelper.left_join_with_check(
                left          = result,
                right         = nganh_df,
                on            = "ma_nganh",
                cols          = nganh_cols,
                check_col     = "ten_nganh",
                table_name    = f"{TABLE} ← nganh",
                allow_missing = True,  # Sinh viên mới chưa có ngành → warning
            )

        # ── Join khoa ─────────────────────────────────────────────
        if not khoa_df.empty and "ma_khoa" in result.columns:
            result = _TransformHelper.left_join_with_check(
                left          = result,
                right         = khoa_df,
                on            = "ma_khoa",
                cols          = ["ma_khoa", "ten_khoa"],
                check_col     = "ten_khoa",
                table_name    = f"{TABLE} ← khoa",
                allow_missing = True,
            )

        # ── Join lop (optional) ───────────────────────────────────
        if not lop_df.empty and "ma_lop" in result.columns:
            lop_cols = ["ma_lop", "ten_lop"]
            if "ma_co_van" in lop_df.columns:
                lop_cols.append("ma_co_van")

            result = _TransformHelper.left_join_with_check(
                left          = result,
                right         = lop_df,
                on            = "ma_lop",
                cols          = lop_cols,
                check_col     = "ten_lop",
                table_name    = f"{TABLE} ← lop",
                allow_missing = True,
            )

        # ── Join co_van (optional) ────────────────────────────────
        if not gv_df.empty and "ma_co_van" in result.columns:
            gv_name = gv_df[["ma_giang_vien", "ho", "ten"]].copy()
            gv_name["ten_co_van"] = (
                gv_name["ho"].fillna("").str.strip()
                + " "
                + gv_name["ten"].fillna("").str.strip()
            ).str.strip()
            gv_name = gv_name[
                ["ma_giang_vien", "ten_co_van"]
            ].drop_duplicates(subset=["ma_giang_vien"])

            result = result.merge(
                gv_name,
                left_on  = "ma_co_van",
                right_on = "ma_giang_vien",
                how      = "left",
            )
            if "ma_giang_vien" in result.columns:
                result = result.drop(columns=["ma_giang_vien"])
        else:
            result["ten_co_van"] = None

        if "ma_co_van" not in result.columns:
            result["ma_co_van"] = None

        # ── Cast datetime ─────────────────────────────────────────
        result["ngay_sinh"] = pd.to_datetime(
            result["ngay_sinh"], errors="coerce"
        )

        # ── Dedup ─────────────────────────────────────────────────
        before = len(result)
        result = result.drop_duplicates(subset=["ma_sinh_vien"])
        after  = len(result)
        if before != after:
            logger.warning(
                f"[{TABLE}] Đã drop {before - after} duplicate ma_sinh_vien"
            )

        _TransformHelper.log_transform_result(TABLE, result)
        return result
        # ─────────────────────────────────────────────────────────────
    # FACT TRANSFORMS
    # ─────────────────────────────────────────────────────────────

    def _transform_fact_diem(
        self,
        diem_df: pd.DataFrame,
        dk_df:   pd.DataFrame,
    ) -> pd.DataFrame:
        TABLE = "fact_diem"

        if diem_df.empty:
            logger.warning(f"[{TABLE}] Không có dữ liệu điểm → trả về empty")
            return pd.DataFrame()

        if dk_df.empty:
            logger.warning(f"[{TABLE}] Không có dữ liệu đăng ký → trả về empty")
            return pd.DataFrame()

        # ── Join diem + dang_ky ───────────────────────────────────
        result = diem_df.merge(
            dk_df[[
                "ma_dang_ky", "ma_sinh_vien",
                "ma_hoc_phan", "ma_hoc_ky", "ma_giang_vien"
            ]],
            on       = "ma_dang_ky",
            how      = "left",
            suffixes = ("", "_dk"),
        )

        # Fill từ _dk nếu cột gốc NULL
        for col in ["ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky", "ma_giang_vien"]:
            dk_col = f"{col}_dk"
            if dk_col in result.columns:
                result[col] = result[col].fillna(result[dk_col])
                result = result.drop(columns=[dk_col])

        # ── Assert FK critical ────────────────────────────────────
        # ma_sinh_vien và ma_hoc_ky PHẢI có → nếu không sẽ orphan fact
        critical_fks = ["ma_sinh_vien", "ma_hoc_ky"]
        for fk in critical_fks:
            if fk in result.columns:
                null_count = int(result[fk].isna().sum())
                if null_count > 0:
                    pct = null_count / len(result) * 100
                    raise TransformError(
                        f"[{TABLE}] FK '{fk}' bị NULL {null_count} records "
                        f"({pct:.1f}%) sau khi join với đăng ký. "
                        f"Kiểm tra ma_dang_ky tồn tại trong bảng đăng ký."
                    )

        # ma_giang_vien được phép NULL (môn tự học)
        if "ma_giang_vien" in result.columns:
            null_gv = int(result["ma_giang_vien"].isna().sum())
            if null_gv > 0:
                logger.warning(
                    f"[{TABLE}] ma_giang_vien NULL: {null_gv} records "
                    f"(có thể là môn tự học)"
                )

        # ── Cast & tính điểm ──────────────────────────────────────
        score_cols = [
            "diem_chuyen_can", "diem_bai_tap",
            "diem_giua_ky", "diem_cuoi_ky",
        ]
        for col in score_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

        if "diem_tong_ket" not in result.columns:
            result["diem_tong_ket"] = np.nan
        result["diem_tong_ket"] = pd.to_numeric(
            result["diem_tong_ket"], errors="coerce"
        )

        # Tính lại nếu NULL nhưng có đủ thành phần
        mask_null     = result["diem_tong_ket"].isna()
        has_all_scores = pd.Series(True, index=result.index)
        for col in score_cols:
            if col in result.columns:
                has_all_scores &= result[col].notna()

        recalc_mask = mask_null & has_all_scores
        if recalc_mask.any():
            result.loc[recalc_mask, "diem_tong_ket"] = (
                result.loc[recalc_mask, "diem_chuyen_can"] * WEIGHT_CHUYEN_CAN
                + result.loc[recalc_mask, "diem_bai_tap"]  * WEIGHT_BAI_TAP
                + result.loc[recalc_mask, "diem_giua_ky"]  * WEIGHT_GIUA_KY
                + result.loc[recalc_mask, "diem_cuoi_ky"]  * WEIGHT_CUOI_KY
            ).round(2)
            logger.info(
                f"[{TABLE}] Tính lại diem_tong_ket cho "
                f"{int(recalc_mask.sum())} bản ghi"
            )

        # ── Validate range điểm ───────────────────────────────────
        valid_scores = result["diem_tong_ket"].dropna()
        out_range = valid_scores[(valid_scores < 0) | (valid_scores > 10)]
        if len(out_range) > 0:
            raise TransformError(
                f"[{TABLE}] diem_tong_ket có {len(out_range)} giá trị "
                f"ngoài [0, 10]. Kiểm tra nguồn dữ liệu điểm."
            )

        # ── Tính xếp loại ─────────────────────────────────────────
        result["diem_chu"] = result["diem_tong_ket"].apply(
            _TransformHelper._to_letter_grade
        )
        result["diem_he_4"] = result["diem_tong_ket"].apply(
            _TransformHelper._to_gpa_4
        )
        result["dat_mon"] = result["diem_tong_ket"] >= 4.0

        if "hoc_lai" not in result.columns:
            result["hoc_lai"] = False

        # ── Select output cols ─────────────────────────────────────
        output_cols = [
            "ma_dang_ky", "ma_sinh_vien", "ma_hoc_phan",
            "ma_hoc_ky", "ma_giang_vien",
            "diem_chuyen_can", "diem_bai_tap",
            "diem_giua_ky", "diem_cuoi_ky",
            "diem_tong_ket", "diem_chu", "diem_he_4",
            "dat_mon", "hoc_lai",
        ]
        existing = [c for c in output_cols if c in result.columns]
        result = result[existing]

        _TransformHelper.log_transform_result(TABLE, result)
        return result

    # ─────────────────────────────────────────────────────────────

    def _transform_fact_ren_luyen(self, ctsv_df: pd.DataFrame) -> pd.DataFrame:
        TABLE = "fact_ren_luyen"

        if ctsv_df.empty:
            logger.warning(f"[{TABLE}] Không có dữ liệu CSV → trả về empty")
            return pd.DataFrame()

        result = ctsv_df.copy()

        # ── Chuẩn hoá ma_sinh_vien ────────────────────────────────
        result["ma_sinh_vien"] = (
            result["ma_sinh_vien"].fillna("").astype(str).str.strip().str.upper()
        )
        empty_msv = int((result["ma_sinh_vien"] == "").sum())
        if empty_msv > 0:
            raise TransformError(
                f"[{TABLE}] ma_sinh_vien rỗng: {empty_msv} records. "
                f"Kiểm tra file CSV nguồn 2."
            )

        # ── Chuẩn hoá hoc_ky ─────────────────────────────────────
        if "hoc_ky" in result.columns:
            result["hoc_ky"] = (
                result["hoc_ky"].fillna("").astype(str).str.strip()
            )
            empty_hk = int((result["hoc_ky"] == "").sum())
            if empty_hk > 0:
                logger.warning(
                    f"[{TABLE}] hoc_ky rỗng: {empty_hk} records"
                )

        # ── Cast numeric ──────────────────────────────────────────
        result["diem_ren_luyen"] = pd.to_numeric(
            result["diem_ren_luyen"], errors="coerce"
        )
        result["muc_tien_hb"] = pd.to_numeric(
            result["muc_tien_hb"], errors="coerce"
        ).fillna(0)

        # ── Validate diem_ren_luyen range ─────────────────────────
        valid_rl = result["diem_ren_luyen"].dropna()
        out_range = valid_rl[(valid_rl < 0) | (valid_rl > 100)]
        if len(out_range) > 0:
            logger.warning(
                f"[{TABLE}] diem_ren_luyen có {len(out_range)} giá trị "
                f"ngoài [0, 100]"
            )

        # ── Tính xếp loại nếu NULL ────────────────────────────────
        mask = result["xep_loai_rl"].isna() & result["diem_ren_luyen"].notna()
        if mask.any():
            result.loc[mask, "xep_loai_rl"] = (
                result.loc[mask, "diem_ren_luyen"].apply(_TransformHelper._classify_rl)
            )
            logger.info(
                f"[{TABLE}] Tính xep_loai_rl cho {int(mask.sum())} bản ghi"
            )

        # ── Boolean flags ─────────────────────────────────────────
        result["co_hoc_bong"] = (
            result["loai_hoc_bong"].notna()
            & (result["loai_hoc_bong"].astype(str).str.strip() != "")
        )
        result["bi_ky_luat"] = (
            result["hinh_thuc_ky_luat"].notna()
            & (result["hinh_thuc_ky_luat"].astype(str).str.strip() != "")
        )

        # ── Clean text fields ─────────────────────────────────────
        for col in ["loai_hoc_bong", "hinh_thuc_ky_luat", "ly_do_ky_luat"]:
            if col in result.columns:
                result[col] = result[col].replace(
                    r"^\s*$", np.nan, regex=True
                )

        _TransformHelper.log_transform_result(TABLE, result)
        return result

    # ─────────────────────────────────────────────────────────────

    def _transform_fact_tai_chinh(self, api_df: pd.DataFrame) -> pd.DataFrame:
        TABLE = "fact_tai_chinh"

        if api_df.empty:
            logger.warning(f"[{TABLE}] Không có dữ liệu API → trả về empty")
            return pd.DataFrame()

        result = api_df.copy()

        # ── Chuẩn hoá ma_sinh_vien ────────────────────────────────
        if "ma_sinh_vien" in result.columns:
            result["ma_sinh_vien"] = (
                result["ma_sinh_vien"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.upper()
            )
            empty_msv = int((result["ma_sinh_vien"] == "").sum())
            if empty_msv > 0:
                raise TransformError(
                    f"[{TABLE}] ma_sinh_vien rỗng: {empty_msv} records. "
                    f"Kiểm tra API nguồn 3."
                )

        # ── Cast numeric ──────────────────────────────────────────
        money_cols = [
            "hoc_phi_phai_dong", "da_dong",
            "con_no", "so_tien_mien_giam",
        ]
        for col in money_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(
                    result[col], errors="coerce"
                ).fillna(0)

        # ── Validate số tiền không âm ─────────────────────────────
        for col in money_cols:
            if col in result.columns:
                negative = int((result[col] < 0).sum())
                if negative > 0:
                    raise TransformError(
                        f"[{TABLE}] cột '{col}' có {negative} giá trị âm. "
                        f"Kiểm tra API tài chính."
                    )

        # ── Cast datetime ─────────────────────────────────────────
        if "ngay_dong_cuoi" in result.columns:
            result["ngay_dong_cuoi"] = pd.to_datetime(
                result["ngay_dong_cuoi"], errors="coerce"
            )

        _TransformHelper.log_transform_result(TABLE, result)
        return result
        # ─────────────────────────────────────────────────────────────
    # SAVE / LOAD STAGING
    # ─────────────────────────────────────────────────────────────

    def _save_to_staging(self, data: TransformedData) -> None:
        """Upload TransformedData lên MinIO bucket staging."""
        try:
            client   = MinIOClient()
            run_id   = MinIOClient.make_run_id()

            upload_map = {
                "dim_hoc_ky.parquet":     data.dim_thoi_gian,
                "dim_giang_vien.parquet": data.dim_giang_vien,
                "dim_hoc_phan.parquet":   data.dim_hoc_phan,
                "dim_sinh_vien.parquet":  data.dim_sinh_vien,
                "fact_diem.parquet":      data.fact_diem,
                "fact_ren_luyen.parquet": data.fact_ren_luyen,
                "fact_tai_chinh.parquet": data.fact_tai_chinh,
            }

            results = {}
            for file_name, df in upload_map.items():
                results[file_name] = client.upload_df(
                    df, file_name, run_id, bucket="staging"
                )

            self._last_run_id = run_id

            success = sum(results.values())
            total   = len(results)
            logger.info(
                f"  MinIO staging: {success}/{total} files OK "
                f"→ run_id={run_id}"
            )

        except TransformError:
            raise  # Không catch TransformError
        except Exception as e:
            logger.warning(
                f"  MinIO staging thất bại (pipeline vẫn tiếp tục): {e}"
            )

    def load_from_staging(self, run_id: str = None) -> "TransformedData":
        """Đọc lại TransformedData từ MinIO staging bucket."""
        client = MinIOClient()

        if run_id is None:
            run_id = client.get_latest_run_id(bucket="staging")
        if run_id is None:
            raise FileNotFoundError(
                "Không tìm thấy staging data trong MinIO bucket 'staging'. "
                "Hãy chạy transform_all() ít nhất 1 lần trước."
            )

        logger.info(f"  Load TransformedData từ staging: run_id={run_id}")

        data = TransformedData(
            dim_thoi_gian  = client.download_df("dim_hoc_ky.parquet",     run_id, bucket="staging"),
            dim_giang_vien = client.download_df("dim_giang_vien.parquet", run_id, bucket="staging"),
            dim_hoc_phan   = client.download_df("dim_hoc_phan.parquet",   run_id, bucket="staging"),
            dim_sinh_vien  = client.download_df("dim_sinh_vien.parquet",  run_id, bucket="staging"),
            fact_diem      = client.download_df("fact_diem.parquet",      run_id, bucket="staging"),
            fact_ren_luyen = client.download_df("fact_ren_luyen.parquet", run_id, bucket="staging"),
            fact_tai_chinh = client.download_df("fact_tai_chinh.parquet", run_id, bucket="staging"),
        )

        logger.info(f"  Load from staging OK — run_id={run_id}")
        return data
    # ─────────────────────────────────────────────────────────────────
# CUSTOM EXCEPTION - phân biệt lỗi transform với lỗi runtime
# ─────────────────────────────────────────────────────────────────

class TransformError(Exception):
    pass


# ─────────────────────────────────────────────────────────────────
# PRIVATE HELPERS - dùng trong DataTransformer
# ─────────────────────────────────────────────────────────────────
class _TransformHelper:
    @staticmethod
    def safe_name(series: pd.Series, col_name: str) -> pd.Series:
        cleaned = series.fillna("").astype(str).str.strip()
        cleaned = cleaned.replace(
            r"^(nan|none|null|NaN|None|NULL)$", "", regex=True
        )
        return cleaned

    @staticmethod
    def build_full_name(
        df: pd.DataFrame,
        ho_col: str = "ho",
        ten_col: str = "ten",
        out_col: str = "ho_ten",
    ) -> pd.DataFrame:
    
        df = df.copy()
        df[ho_col]  = _TransformHelper.safe_name(df[ho_col],  ho_col)
        df[ten_col] = _TransformHelper.safe_name(df[ten_col], ten_col)

        df[out_col] = (df[ho_col] + " " + df[ten_col]).str.strip()

        # Phát hiện record có ho_ten rỗng sau khi ghép
        empty_mask = df[out_col] == ""
        if empty_mask.any():
            count = int(empty_mask.sum())
            sample_idx = df[empty_mask].index[:3].tolist()
            raise TransformError(
                f"[build_full_name] {count} records có '{out_col}' rỗng sau khi ghép. "
                f"Index ví dụ: {sample_idx}. "
                f"Kiểm tra cột '{ho_col}' và '{ten_col}' ở nguồn."
            )
        return df

    @staticmethod
    def left_join_with_check(
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: str,
        cols: list,
        check_col: str,
        table_name: str,
        allow_missing: bool = False,
    ) -> pd.DataFrame:
        if right.empty:
            logger.warning(
                f"[left_join] '{table_name}': bảng right rỗng → bỏ qua join"
            )
            return left

        if on not in left.columns:
            logger.warning(
                f"[left_join] '{table_name}': cột '{on}' không có trong left → bỏ qua join"
            )
            return left

        result = left.merge(
            right[cols].drop_duplicates(subset=[on]),
            on=on,
            how="left",
        )

        # Kiểm tra sau join
        if check_col in result.columns:
            missing_mask = result[check_col].isna()
            missing_count = int(missing_mask.sum())

            if missing_count > 0:
                # Tìm các giá trị key không match được
                unmatched_keys = (
                    result.loc[missing_mask, on]
                    .dropna()
                    .unique()[:5]
                    .tolist()
                )
                pct = missing_count / len(result) * 100

                msg = (
                    f"[left_join] '{table_name}': sau join on='{on}', "
                    f"cột '{check_col}' bị NULL {missing_count} records ({pct:.1f}%). "
                    f"Key không match ví dụ: {unmatched_keys}"
                )

                if allow_missing:
                    logger.warning(f"⚠ {msg}")
                else:
                    logger.error(f"✗ {msg}")
                    raise TransformError(msg)
            else:
                logger.info(
                    f"[left_join] '{table_name}': join OK "
                    f"({len(result)} records, 0 unmatched)"
                )

        return result

    @staticmethod
    def assert_no_null(
        df: pd.DataFrame,
        cols: list,
        context: str,
    ) -> None:
        for col in cols:
            if col not in df.columns:
                raise TransformError(
                    f"[assert_no_null] '{context}': thiếu cột '{col}'"
                )
            null_count = int(df[col].isna().sum())
            if null_count > 0:
                pct = null_count / len(df) * 100
                raise TransformError(
                    f"[assert_no_null] '{context}': cột '{col}' "
                    f"có {null_count} NULL ({pct:.1f}%)"
                )

    @staticmethod
    def assert_no_duplicate(
        df: pd.DataFrame,
        subset: list,
        context: str,
    ) -> None:
        """
        Kiểm tra không có duplicate theo subset cột.
        Raise TransformError nếu có.
        """
        dupe_count = int(df.duplicated(subset=subset).sum())
        if dupe_count > 0:
            raise TransformError(
                f"[assert_no_duplicate] '{context}': "
                f"{dupe_count} duplicate records theo {subset}"
            )

    @staticmethod
    def log_transform_result(name: str, df: pd.DataFrame) -> None:
        logger.info(f"  {name:<25s} → {len(df):>8,} records ✓")
    # ─────────────────────────────────────────────────────────────
    # STATIC HELPERS
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _to_letter_grade(score: float) -> Optional[str]:
        if pd.isna(score):
            return None
        for lower, upper, letter, _ in GRADE_SCALE:
            if lower <= score < upper:
                return letter
        return "F"

    @staticmethod
    def _to_gpa_4(score: float) -> Optional[float]:
        if pd.isna(score):
            return None
        for lower, upper, _, gpa4 in GRADE_SCALE:
            if lower <= score < upper:
                return gpa4
        return 0.0

    @staticmethod
    def _classify_rl(score: float) -> Optional[str]:
        if pd.isna(score):
            return None
        for lower, upper, label in RL_THRESHOLDS:
            if lower <= score < upper:
                return label
        return "Yếu"