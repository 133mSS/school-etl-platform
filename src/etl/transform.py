from typing import Dict, Optional
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

from src.etl.extract import ExtractedData
from src.utils.logger import get_logger
from src.utils.minio_client import MinIOClient

logger = get_logger("etl.transform")


@dataclass
class TransformedData:
    dim_giang_vien: pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_sinh_vien:  pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_hoc_phan:   pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_thoi_gian:  pd.DataFrame = field(default_factory=pd.DataFrame)

    fact_diem:       pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_dang_ky:    pd.DataFrame = field(default_factory=pd.DataFrame)  # FIX: thêm mới
    fact_ren_luyen:  pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_tai_chinh:  pd.DataFrame = field(default_factory=pd.DataFrame)

    def summary(self) -> Dict[str, int]:
        counts = {}
        for attr in vars(self):
            val = getattr(self, attr)
            if isinstance(val, pd.DataFrame):
                counts[attr] = len(val)
        return counts


# ── Thang điểm ───────────────────────────────────────────────────────────
GRADE_SCALE = [
    (9.5,  10.01, "A+", 4.0),
    (8.5,  9.5,   "A",  3.7),
    (8.0,  8.5,   "B+", 3.5),
    (7.0,  8.0,   "B",  3.0),
    (6.5,  7.0,   "C+", 2.5),
    (5.5,  6.5,   "C",  2.0),
    (5.0,  5.5,   "D+", 1.5),
    (4.0,  5.0,   "D",  1.0),
    (0.0,  4.0,   "F",  0.0),
]

WEIGHT_CHUYEN_CAN = 0.10
WEIGHT_BAI_TAP    = 0.10
WEIGHT_GIUA_KY    = 0.20
WEIGHT_CUOI_KY    = 0.60

# FIX: Upper bound của "Xuất sắc" phải là 101 (không phải 100).
# Hàm _classify_rl dùng điều kiện  lower <= score < upper.
# Khi score = 100:  90 <= 100 < 100  →  False  → trả về "Yếu"  ❌
# Khi score = 100:  90 <= 100 < 101  →  True   → trả về "Xuất sắc" ✓
RL_THRESHOLDS = [
    (90, 101, "Xuất sắc"),
    (80,  90, "Tốt"),
    (65,  80, "Khá"),
    (50,  65, "Trung bình"),
    (0,   50, "Yếu"),
]


class DataTransformer:
    def __init__(self):
        self._last_run_id: str = ""

    # ────────────────────────────────────────────────────────────────
    # MAIN ENTRY
    # ────────────────────────────────────────────────────────────────

    def transform_all(self, extracted: ExtractedData) -> TransformedData:
        logger.info("==BẮT ĐẦU TRANSFORM==")
        logger.info("=" * 70)

        result = TransformedData()

        logger.info("── Bước 1: Transform Dimension Tables ──")
        result.dim_thoi_gian  = self._transform_dim_hoc_ky(extracted.hoc_ky_nam_hoc)
        result.dim_giang_vien = self._transform_dim_giang_vien(
            extracted.giang_vien, extracted.khoa
        )
        result.dim_hoc_phan   = self._transform_dim_hoc_phan(
            extracted.hoc_phan, extracted.khoa
        )
        result.dim_sinh_vien  = self._transform_dim_sinh_vien(
            extracted.sinh_vien,
            extracted.nganh,
            extracted.lop_hanh_chinh,
            extracted.khoa,
            extracted.giang_vien,
        )

        logger.info("── Bước 2: Transform Fact Tables ──")
        result.fact_diem      = self._transform_fact_diem(
            extracted.diem_hoc_phan,
            extracted.dang_ky_hoc_phan,
        )
        # FIX: Transform fact_dang_ky (trước đây bị bỏ qua)
        result.fact_dang_ky   = self._transform_fact_dang_ky(
            extracted.dang_ky_hoc_phan
        )
        result.fact_ren_luyen = self._transform_fact_ren_luyen(extracted.ctsv_data)
        result.fact_tai_chinh = self._transform_fact_tai_chinh(extracted.tai_chinh_data)

        logger.info("=" * 70)
        logger.info("TRANSFORM HOÀN TẤT")
        for name, count in result.summary().items():
            logger.info(f"   {name:<25s}: {count:>8,} records")
        logger.info("=" * 70)

        self._save_to_staging(result)
        return result

    def _save_to_staging(self, data: TransformedData) -> None:
        try:
            client = MinIOClient()
            run_id = MinIOClient.make_run_id()

            upload_map = {
                "dim_hoc_ky.parquet":      data.dim_thoi_gian,
                "dim_giang_vien.parquet":  data.dim_giang_vien,
                "dim_hoc_phan.parquet":    data.dim_hoc_phan,
                "dim_sinh_vien.parquet":   data.dim_sinh_vien,
                "fact_diem.parquet":       data.fact_diem,
                "fact_dang_ky.parquet":    data.fact_dang_ky,   # FIX: thêm mới
                "fact_ren_luyen.parquet":  data.fact_ren_luyen,
                "fact_tai_chinh.parquet":  data.fact_tai_chinh,
            }

            results = {}
            for file_name, df in upload_map.items():
                results[file_name] = client.upload_df(df, file_name, run_id, bucket="staging")

            self._last_run_id = run_id

            success = sum(results.values())
            total   = len(results)
            logger.info(
                f"  MinIO staging-data: {success}/{total} files OK → run_id={run_id}"
            )

        except Exception as e:
            logger.warning(f"  MinIO staging thất bại (pipeline vẫn tiếp tục): {e}")

    def load_from_staging(self, run_id: str = None) -> "TransformedData":
        client = MinIOClient()

        if run_id is None:
            run_id = client.get_latest_run_id(bucket="staging")
            if run_id is None:
                raise FileNotFoundError(
                    "Không tìm thấy staging data trong MinIO bucket 'staging-data'. "
                    "Hãy chạy transform_all() ít nhất 1 lần trước."
                )

        logger.info(f"  Load TransformedData từ MinIO staging: run_id={run_id}")

        data = TransformedData(
            dim_thoi_gian  = client.download_df("dim_hoc_ky.parquet",     run_id, bucket="staging"),
            dim_giang_vien = client.download_df("dim_giang_vien.parquet", run_id, bucket="staging"),
            dim_hoc_phan   = client.download_df("dim_hoc_phan.parquet",   run_id, bucket="staging"),
            dim_sinh_vien  = client.download_df("dim_sinh_vien.parquet",  run_id, bucket="staging"),
            fact_diem      = client.download_df("fact_diem.parquet",      run_id, bucket="staging"),
            fact_dang_ky   = client.download_df("fact_dang_ky.parquet",   run_id, bucket="staging"),  # FIX
            fact_ren_luyen = client.download_df("fact_ren_luyen.parquet", run_id, bucket="staging"),
            fact_tai_chinh = client.download_df("fact_tai_chinh.parquet", run_id, bucket="staging"),
        )

        logger.info(f"Load from staging-data OK — run_id={run_id}")
        return data

    # ────────────────────────────────────────────────────────────────
    # DIMENSION TRANSFORMS
    # ────────────────────────────────────────────────────────────────

    def _transform_dim_hoc_ky(self, hk_df: pd.DataFrame) -> pd.DataFrame:
        if hk_df.empty:
            return pd.DataFrame()

        result = hk_df[["ma_hoc_ky", "nam_hoc", "hoc_ky",
                         "ngay_bat_dau", "ngay_ket_thuc"]].copy()
        result["ngay_bat_dau"]  = pd.to_datetime(result["ngay_bat_dau"],  errors="coerce")
        result["ngay_ket_thuc"] = pd.to_datetime(result["ngay_ket_thuc"], errors="coerce")
        result["nam_bat_dau"]   = result["ngay_bat_dau"].dt.year
        result["nam_ket_thuc"]  = result["ngay_ket_thuc"].dt.year
        result = result.drop_duplicates(subset=["ma_hoc_ky"])
        logger.info(f"  dim_thoi_gian               → {len(result):>6,} records")
        return result

    def _transform_dim_giang_vien(
        self, gv_df: pd.DataFrame, khoa_df: pd.DataFrame
    ) -> pd.DataFrame:
        if gv_df.empty:
            return pd.DataFrame()

        cols_needed = [
            "ma_giang_vien", "ho", "ten", "email",
            "so_dien_thoai", "chuc_danh", "trang_thai_cong_tac", "ma_khoa",
        ]
        existing_cols = [c for c in cols_needed if c in gv_df.columns]
        result = gv_df[existing_cols].copy()
        result["ho_ten"] = result["ho"].str.strip() + " " + result["ten"].str.strip()

        if not khoa_df.empty and "ma_khoa" in result.columns:
            result = result.merge(
                khoa_df[["ma_khoa", "ten_khoa"]], on="ma_khoa", how="left"
            )

        result = result.drop_duplicates(subset=["ma_giang_vien"])
        logger.info(f"  dim_giang_vien              → {len(result):>6,} records")
        return result

    def _transform_dim_hoc_phan(
        self, hp_df: pd.DataFrame, khoa_df: pd.DataFrame
    ) -> pd.DataFrame:
        if hp_df.empty:
            return pd.DataFrame()

        cols_needed = [
            "ma_hoc_phan", "ma_mon", "ten_mon", "so_tin_chi",
            "so_gio_ly_thuyet", "so_gio_thuc_hanh",
            "hoc_ky_de_xuat", "bat_buoc", "ma_khoa",
        ]
        existing_cols = [c for c in cols_needed if c in hp_df.columns]
        result = hp_df[existing_cols].copy()

        result["loai_hoc_phan"] = result.get(
            "bat_buoc", pd.Series(True, index=result.index)
        ).apply(lambda x: "Bat buoc" if x else "Tu chon")

        if not khoa_df.empty and "ma_khoa" in result.columns:
            result = result.merge(
                khoa_df[["ma_khoa", "ten_khoa"]], on="ma_khoa", how="left"
            )

        result = result.drop_duplicates(subset=["ma_hoc_phan"])
        logger.info(f"  dim_hoc_phan                → {len(result):>6,} records")
        return result

    def _transform_dim_sinh_vien(
        self,
        sv_df: pd.DataFrame,
        nganh_df: pd.DataFrame,
        lop_df: pd.DataFrame,
        khoa_df: pd.DataFrame,
        gv_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if sv_df.empty:
            return pd.DataFrame()

        result = sv_df[[
            "ma_sinh_vien", "ho", "ten", "ngay_sinh",
            "gioi_tinh", "email", "ma_nganh", "ma_lop",
            "khoa_hoc", "trang_thai_hoc_tap",
        ]].copy()
        result["ho_ten"] = result["ho"].str.strip() + " " + result["ten"].str.strip()

        if not nganh_df.empty:
            nganh_cols = ["ma_nganh", "ten_nganh"]
            if "ma_khoa" in nganh_df.columns:
                nganh_cols.append("ma_khoa")
            result = result.merge(
                nganh_df[nganh_cols].drop_duplicates(subset=["ma_nganh"]),
                on="ma_nganh", how="left",
            )

        if not khoa_df.empty and "ma_khoa" in result.columns:
            result = result.merge(
                khoa_df[["ma_khoa", "ten_khoa"]].drop_duplicates(subset=["ma_khoa"]),
                on="ma_khoa", how="left",
            )

        if not lop_df.empty:
            lop_cols = ["ma_lop", "ten_lop"]
            if "ma_co_van" in lop_df.columns:
                lop_cols.append("ma_co_van")
            result = result.merge(
                lop_df[lop_cols].drop_duplicates(subset=["ma_lop"]),
                on="ma_lop", how="left",
            )

        if not gv_df.empty and "ma_co_van" in result.columns:
            gv_name = gv_df[["ma_giang_vien", "ho", "ten"]].copy()
            gv_name["ten_co_van"] = (
                gv_name["ho"].str.strip() + " " + gv_name["ten"].str.strip()
            )
            gv_name = gv_name[["ma_giang_vien", "ten_co_van"]].drop_duplicates(
                subset=["ma_giang_vien"]
            )
            result = result.merge(
                gv_name, left_on="ma_co_van", right_on="ma_giang_vien", how="left"
            )
            if "ma_giang_vien" in result.columns:
                result = result.drop(columns=["ma_giang_vien"])
        else:
            result["ten_co_van"] = None

        if "ma_co_van" not in result.columns:
            result["ma_co_van"] = None

        result["ngay_sinh"] = pd.to_datetime(result["ngay_sinh"], errors="coerce")
        result = result.drop_duplicates(subset=["ma_sinh_vien"])
        logger.info(f"  dim_sinh_vien               → {len(result):>6,} records")
        return result

    # ────────────────────────────────────────────────────────────────
    # FACT TRANSFORMS
    # ────────────────────────────────────────────────────────────────

    def _transform_fact_diem(
        self, diem_df: pd.DataFrame, dk_df: pd.DataFrame
    ) -> pd.DataFrame:
        if diem_df.empty or dk_df.empty:
            logger.warning("  fact_diem | Không có dữ liệu điểm hoặc đăng ký")
            return pd.DataFrame()

        result = diem_df.merge(
            dk_df[["ma_dang_ky", "ma_sinh_vien", "ma_hoc_phan",
                   "ma_hoc_ky", "ma_giang_vien"]],
            on="ma_dang_ky", how="left", suffixes=("", "_dk"),
        )

        for col in ["ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky", "ma_giang_vien"]:
            dk_col = f"{col}_dk"
            if dk_col in result.columns:
                result[col] = result[col].fillna(result[dk_col])
                result = result.drop(columns=[dk_col])

        score_cols = ["diem_chuyen_can", "diem_bai_tap", "diem_giua_ky", "diem_cuoi_ky"]
        for col in score_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

        if "diem_tong_ket" not in result.columns:
            result["diem_tong_ket"] = np.nan
        result["diem_tong_ket"] = pd.to_numeric(result["diem_tong_ket"], errors="coerce")

        # Tính lại điểm tổng kết nếu NULL nhưng có đủ thành phần
        mask_null      = result["diem_tong_ket"].isna()
        has_all_scores = pd.Series(True, index=result.index)
        for col in score_cols:
            if col in result.columns:
                has_all_scores = has_all_scores & result[col].notna()

        recalc_mask = mask_null & has_all_scores
        if recalc_mask.any():
            result.loc[recalc_mask, "diem_tong_ket"] = (
                result.loc[recalc_mask, "diem_chuyen_can"] * WEIGHT_CHUYEN_CAN
                + result.loc[recalc_mask, "diem_bai_tap"]  * WEIGHT_BAI_TAP
                + result.loc[recalc_mask, "diem_giua_ky"]  * WEIGHT_GIUA_KY
                + result.loc[recalc_mask, "diem_cuoi_ky"]  * WEIGHT_CUOI_KY
            ).round(2)
            logger.info(
                f"  fact_diem | Tính lại điểm tổng kết cho {recalc_mask.sum()} bản ghi"
            )

        result["diem_chu"] = result["diem_tong_ket"].apply(self._to_letter_grade)
        result["diem_he_4"] = result["diem_tong_ket"].apply(self._to_gpa_4)
        result["dat_mon"]   = result["diem_tong_ket"] >= 4.0

        if "hoc_lai" not in result.columns:
            result["hoc_lai"] = False

        output_cols = [
            "ma_dang_ky", "ma_sinh_vien", "ma_hoc_phan",
            "ma_hoc_ky", "ma_giang_vien",
            "diem_chuyen_can", "diem_bai_tap", "diem_giua_ky", "diem_cuoi_ky",
            "diem_tong_ket", "diem_chu", "diem_he_4", "dat_mon", "hoc_lai",
        ]
        existing = [c for c in output_cols if c in result.columns]
        result = result[existing]

        logger.info(f"  fact_diem                   → {len(result):>6,} records")
        return result

    def _transform_fact_dang_ky(self, dk_df: pd.DataFrame) -> pd.DataFrame:
        """
        FIX: Transform fact_dang_ky từ dang_ky_hoc_phan.
        Trước đây bị bỏ qua hoàn toàn → bảng fact_dang_ky trống.
        Bảng này lưu TẤT CẢ lượt đăng ký (kể cả chưa thi),
        khác fact_hoc_tap chỉ có bản ghi đã có điểm.
        """
        if dk_df.empty:
            logger.warning("  fact_dang_ky | Không có dữ liệu đăng ký")
            return pd.DataFrame()

        needed_cols = [
            "ma_dang_ky", "ma_sinh_vien", "ma_hoc_phan",
            "ma_hoc_ky", "ma_giang_vien", "ngay_dang_ky", "trang_thai",
        ]
        existing_cols = [c for c in needed_cols if c in dk_df.columns]
        result = dk_df[existing_cols].copy()

        # Chuẩn hoá ma_sinh_vien
        result["ma_sinh_vien"] = result["ma_sinh_vien"].astype(str).str.strip().str.upper()

        # Chuẩn hoá ngày
        if "ngay_dang_ky" in result.columns:
            result["ngay_dang_ky"] = pd.to_datetime(result["ngay_dang_ky"], errors="coerce")

        # Điền giá trị mặc định cho trang_thai
        if "trang_thai" in result.columns:
            result["trang_thai"] = result["trang_thai"].fillna("Đã đăng ký")

        # Loại bỏ bản ghi trùng theo business key
        result = result.drop_duplicates(
            subset=["ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky"]
        )

        logger.info(f"  fact_dang_ky               → {len(result):>6,} records")
        return result

    def _transform_fact_ren_luyen(self, ctsv_df: pd.DataFrame) -> pd.DataFrame:
        if ctsv_df.empty:
            logger.warning("  fact_ren_luyen | Không có dữ liệu CSV")
            return pd.DataFrame()

        result = ctsv_df.copy()
        result["ma_sinh_vien"]   = result["ma_sinh_vien"].str.strip().str.upper()
        if "hoc_ky" in result.columns:
            result["hoc_ky"] = result["hoc_ky"].str.strip()

        result["diem_ren_luyen"] = pd.to_numeric(result["diem_ren_luyen"], errors="coerce")
        result["muc_tien_hb"]    = pd.to_numeric(result["muc_tien_hb"],
                                                  errors="coerce").fillna(0)

        # Tính xếp loại nếu bị NULL
        mask = result["xep_loai_rl"].isna() & result["diem_ren_luyen"].notna()
        if mask.any():
            result.loc[mask, "xep_loai_rl"] = result.loc[mask, "diem_ren_luyen"].apply(
                self._classify_rl
            )

        # Dùng fillna("") trước str.strip() để tránh NaN propagation
        result["co_hoc_bong"] = result["loai_hoc_bong"].fillna("").str.strip() != ""
        result["bi_ky_luat"]  = result["hinh_thuc_ky_luat"].fillna("").str.strip() != ""

        for col in ["loai_hoc_bong", "hinh_thuc_ky_luat", "ly_do_ky_luat"]:
            if col in result.columns:
                result[col] = result[col].replace(r"^\s*$", np.nan, regex=True)

        logger.info(f"  fact_ren_luyen              → {len(result):>6,} records")
        return result

    def _transform_fact_tai_chinh(self, api_df: pd.DataFrame) -> pd.DataFrame:
        if api_df.empty:
            logger.warning("  fact_tai_chinh | Không có dữ liệu API")
            return pd.DataFrame()

        result = api_df.copy()

        if "ma_sinh_vien" in result.columns:
            result["ma_sinh_vien"] = result["ma_sinh_vien"].str.strip().str.upper()

        for col in ["hoc_phi_phai_dong", "da_dong", "con_no", "so_tien_mien_giam"]:
            if col in result.columns:
                result[col] = (
                    pd.to_numeric(result[col], errors="coerce")
                    .fillna(0)
                    .clip(lower=0)
                )

        if "ngay_dong_cuoi" in result.columns:
            result["ngay_dong_cuoi"] = pd.to_datetime(
                result["ngay_dong_cuoi"], 
                errors="coerce",
                infer_datetime_format=True 
            )

        # ★ DEDUP lần 2: Đảm bảo không có duplicate trước khi load vào DW
        before = len(result)
        result = result.drop_duplicates(
            subset=["ma_sinh_vien", "hoc_ky"],
            keep="last"
        )
        if len(result) < before:
            logger.warning(
                f"  fact_tai_chinh | Transform dedup: "
                f"loại bỏ {before - len(result)} records"
            )

        logger.info(f"  fact_tai_chinh              → {len(result):>6,} records")
        return result

    # ────────────────────────────────────────────────────────────────
    # STATIC HELPERS
    # ────────────────────────────────────────────────────────────────

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