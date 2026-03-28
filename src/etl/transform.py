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
    dim_sinh_vien: pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_hoc_phan: pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_thoi_gian: pd.DataFrame = field(default_factory=pd.DataFrame)

    fact_diem: pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_ren_luyen: pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_tai_chinh: pd.DataFrame = field(default_factory=pd.DataFrame)

    fact_tong_hop_sv: pd.DataFrame = field(default_factory=pd.DataFrame)

    def summary(self) -> Dict[str, int]:
        counts = {}
        for attr in vars(self):
            val = getattr(self, attr)
            if isinstance(val, pd.DataFrame):
                counts[attr] = len(val)
        return counts

GRADE_SCALE = [
    (9.0,  10.01, "A+", 4.0),
    (8.5,  9.0,   "A",  3.7),
    (8.0,  8.5,   "B+", 3.5),
    (7.0,  8.0,   "B",  3.0),
    (6.5,  7.0,   "C+", 2.5),
    (5.5,  6.5,   "C",  2.0),
    (5.0,  5.5,   "D+", 1.5),
    (4.0,  5.0,   "D",  1.0),
    (0.0,  4.0,   "F",  0.0),
]

WEIGHT_CHUYEN_CAN = 0.10
WEIGHT_BAI_TAP = 0.10
WEIGHT_GIUA_KY = 0.20
WEIGHT_CUOI_KY = 0.60

GPA_WARNING_LEVEL_1 = 1.0
GPA_WARNING_LEVEL_2 = 1.2

RL_THRESHOLDS = [
    (90, 100, "Xuất sắc"),
    (80, 90,  "Tốt"),
    (65, 80,  "Khá"),
    (50, 65,  "Trung bình"),
    (35, 50,  "Yếu"),
    (0,  35,  "Kém"),
]

class DataTransformer:
    def __init__(self):
        self._last_run_id: str = ""

    def transform_all(self, extracted: ExtractedData) -> TransformedData:
        logger.info("==BẮT ĐẦU TRANSFORM==")
        logger.info("=" * 70)

        result = TransformedData()

        logger.info("── Bước 1: Transform Dimension Tables ──")

        result.dim_thoi_gian = self._transform_dim_hoc_ky(
            extracted.hoc_ky_nam_hoc
        )
        result.dim_giang_vien = self._transform_dim_giang_vien(
            extracted.giang_vien, extracted.khoa
        )
        result.dim_hoc_phan = self._transform_dim_hoc_phan(
            extracted.hoc_phan, extracted.khoa
        )
        result.dim_sinh_vien = self._transform_dim_sinh_vien(
            extracted.sinh_vien,
            extracted.nganh,
            extracted.lop_hanh_chinh,
            extracted.khoa,
            extracted.giang_vien,
        )

        logger.info("── Bước 2: Transform Fact Tables ──")

        result.fact_diem = self._transform_fact_diem(
            extracted.diem_hoc_phan,
            extracted.dang_ky_hoc_phan,
        )
        result.fact_ren_luyen = self._transform_fact_ren_luyen(
            extracted.ctsv_data
        )
        result.fact_tai_chinh = self._transform_fact_tai_chinh(
            extracted.tai_chinh_data
        )

        logger.info("── Bước 3: Tổng hợp đa nguồn ──")

        result.fact_tong_hop_sv = self._build_agg_student_summary(
            fact_diem=result.fact_diem,
            fact_rl=result.fact_ren_luyen,
            fact_tc=result.fact_tai_chinh,
            dk_df=extracted.dang_ky_hoc_phan,
            hp_df=extracted.hoc_phan,
        )

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
                "dim_hoc_ky.parquet":       data.dim_thoi_gian,
                "dim_giang_vien.parquet":   data.dim_giang_vien,
                "dim_hoc_phan.parquet":     data.dim_hoc_phan,
                "dim_sinh_vien.parquet":    data.dim_sinh_vien,
                "fact_diem.parquet":        data.fact_diem,
                "fact_ren_luyen.parquet":   data.fact_ren_luyen,
                "fact_tai_chinh.parquet":   data.fact_tai_chinh,
                "fact_tong_hop_sv.parquet": data.fact_tong_hop_sv,
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
                f"  MinIO staging-data: {success}/{total} files OK"
                f" → run_id={run_id}"
            )

        except Exception as e:
            logger.warning(f"  MinIO staging thất bại (pipeline vẫn tiếp tục): {e}")

    def load_from_staging(self, run_id: str = None) -> TransformedData:
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
            dim_thoi_gian    = client.download_df("dim_hoc_ky.parquet",       run_id, bucket="staging"),
            dim_giang_vien   = client.download_df("dim_giang_vien.parquet",   run_id, bucket="staging"),
            dim_hoc_phan     = client.download_df("dim_hoc_phan.parquet",     run_id, bucket="staging"),
            dim_sinh_vien    = client.download_df("dim_sinh_vien.parquet",    run_id, bucket="staging"),
            fact_diem        = client.download_df("fact_diem.parquet",        run_id, bucket="staging"),
            fact_ren_luyen   = client.download_df("fact_ren_luyen.parquet",   run_id, bucket="staging"),
            fact_tai_chinh   = client.download_df("fact_tai_chinh.parquet",   run_id, bucket="staging"),
            fact_tong_hop_sv = client.download_df("fact_tong_hop_sv.parquet", run_id, bucket="staging"),
        )

        logger.info(f"Load from staging-data OK — run_id={run_id}")
        return data

    def _transform_dim_hoc_ky(self, hk_df: pd.DataFrame) -> pd.DataFrame:
        if hk_df.empty:
            return pd.DataFrame()

        result = hk_df[[
            "ma_hoc_ky", "nam_hoc", "hoc_ky",
            "ngay_bat_dau", "ngay_ket_thuc",
        ]].copy()

        result["ngay_bat_dau"] = pd.to_datetime(
            result["ngay_bat_dau"], errors="coerce"
        )
        result["ngay_ket_thuc"] = pd.to_datetime(
            result["ngay_ket_thuc"], errors="coerce"
        )

        result["nam_bat_dau"] = result["ngay_bat_dau"].dt.year
        result["nam_ket_thuc"] = result["ngay_ket_thuc"].dt.year

        result = result.drop_duplicates(subset=["ma_hoc_ky"])
        logger.info(f"  dim_thoi_gian (→dim_hoc_ky)  → {len(result):>6,} records")
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

        result["ho_ten"] = (
            result["ho"].str.strip() + " " + result["ten"].str.strip()
        )

        if not khoa_df.empty and "ma_khoa" in result.columns:
            result = result.merge(
                khoa_df[["ma_khoa", "ten_khoa"]],
                on="ma_khoa",
                how="left",
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

        if "bat_buoc" in result.columns:
            result["loai_hoc_phan"] = result["bat_buoc"].apply(
                lambda x: "Bat buoc" if x else "Tu chon"
            )
        else:
            result["loai_hoc_phan"] = "Bat buoc"

        if not khoa_df.empty and "ma_khoa" in result.columns:
            result = result.merge(
                khoa_df[["ma_khoa", "ten_khoa"]],
                on="ma_khoa",
                how="left",
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

        result["ho_ten"] = (
            result["ho"].str.strip() + " " + result["ten"].str.strip()
        )

        if not nganh_df.empty:
            nganh_cols = ["ma_nganh", "ten_nganh"]
            if "ma_khoa" in nganh_df.columns:
                nganh_cols.append("ma_khoa")
            result = result.merge(
                nganh_df[nganh_cols].drop_duplicates(subset=["ma_nganh"]),
                on="ma_nganh",
                how="left",
            )

        if not khoa_df.empty and "ma_khoa" in result.columns:
            result = result.merge(
                khoa_df[["ma_khoa", "ten_khoa"]].drop_duplicates(subset=["ma_khoa"]),
                on="ma_khoa",
                how="left",
            )

        if not lop_df.empty:
            lop_cols = ["ma_lop", "ten_lop"]
            if "ma_co_van" in lop_df.columns:
                lop_cols.append("ma_co_van")
            result = result.merge(
                lop_df[lop_cols].drop_duplicates(subset=["ma_lop"]),
                on="ma_lop",
                how="left",
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
                gv_name,
                left_on="ma_co_van",
                right_on="ma_giang_vien",
                how="left",
            )
            if "ma_giang_vien" in result.columns:
                result = result.drop(columns=["ma_giang_vien"])
        else:
            result["ten_co_van"] = None

        if "ma_co_van" not in result.columns:
            result["ma_co_van"] = None

        result["ngay_sinh"] = pd.to_datetime(
            result["ngay_sinh"], errors="coerce"
        )

        result = result.drop_duplicates(subset=["ma_sinh_vien"])
        logger.info(f"  dim_sinh_vien               → {len(result):>6,} records")
        return result

    def _transform_fact_diem(
        self, diem_df: pd.DataFrame, dk_df: pd.DataFrame
    ) -> pd.DataFrame:
        if diem_df.empty or dk_df.empty:
            logger.warning("  fact_diem | Không có dữ liệu điểm hoặc đăng ký")
            return pd.DataFrame()

        result = diem_df.merge(
            dk_df[[
                "ma_dang_ky", "ma_sinh_vien", "ma_hoc_phan",
                "ma_hoc_ky", "ma_giang_vien",
            ]],
            on="ma_dang_ky",
            how="left",
            suffixes=("", "_dk"),
        )

        for col in ["ma_sinh_vien", "ma_hoc_phan", "ma_hoc_ky", "ma_giang_vien"]:
            dk_col = f"{col}_dk"
            if dk_col in result.columns:
                result[col] = result[col].fillna(result[dk_col])
                result = result.drop(columns=[dk_col])

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

        mask_null = result["diem_tong_ket"].isna()
        has_all_scores = pd.Series(True, index=result.index)
        for col in score_cols:
            if col in result.columns:
                has_all_scores = has_all_scores & result[col].notna()

        recalc_mask = mask_null & has_all_scores
        if recalc_mask.any():
            result.loc[recalc_mask, "diem_tong_ket"] = (
                result.loc[recalc_mask, "diem_chuyen_can"] * WEIGHT_CHUYEN_CAN
                + result.loc[recalc_mask, "diem_bai_tap"] * WEIGHT_BAI_TAP
                + result.loc[recalc_mask, "diem_giua_ky"] * WEIGHT_GIUA_KY
                + result.loc[recalc_mask, "diem_cuoi_ky"] * WEIGHT_CUOI_KY
            ).round(2)
            logger.info(
                f"  fact_diem | Tính lại điểm tổng kết cho {recalc_mask.sum()} bản ghi"
            )

        result["diem_chu"] = result["diem_tong_ket"].apply(self._to_letter_grade)
        result["diem_he_4"] = result["diem_tong_ket"].apply(self._to_gpa_4)

        result["dat_mon"] = result["diem_tong_ket"] >= 4.0

        if "hoc_lai" not in result.columns:
            result["hoc_lai"] = False

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

        logger.info(f"  fact_diem (→fact_hoc_tap)    → {len(result):>6,} records")
        return result

    def _transform_fact_ren_luyen(self, ctsv_df: pd.DataFrame) -> pd.DataFrame:
        if ctsv_df.empty:
            logger.warning("  fact_ren_luyen | Không có dữ liệu CSV")
            return pd.DataFrame()

        result = ctsv_df.copy()

        result["ma_sinh_vien"] = result["ma_sinh_vien"].str.strip().str.upper()

        if "hoc_ky" in result.columns:
            result["hoc_ky"] = result["hoc_ky"].str.strip()

        result["diem_ren_luyen"] = pd.to_numeric(
            result["diem_ren_luyen"], errors="coerce"
        )
        result["muc_tien_hb"] = pd.to_numeric(
            result["muc_tien_hb"], errors="coerce"
        ).fillna(0)

        mask = result["xep_loai_rl"].isna() & result["diem_ren_luyen"].notna()
        if mask.any():
            result.loc[mask, "xep_loai_rl"] = result.loc[
                mask, "diem_ren_luyen"
            ].apply(self._classify_rl)

        result["co_hoc_bong"] = (
            result["loai_hoc_bong"].notna()
            & (result["loai_hoc_bong"].str.strip() != "")
        )
        result["bi_ky_luat"] = (
            result["hinh_thuc_ky_luat"].notna()
            & (result["hinh_thuc_ky_luat"].str.strip() != "")
        )

        for col in ["loai_hoc_bong", "hinh_thuc_ky_luat", "ly_do_ky_luat"]:
            if col in result.columns:
                result[col] = result[col].replace(r"^\s*$", np.nan, regex=True)

        logger.info(f"  fact_ren_luyen (→fact_ctsv)  → {len(result):>6,} records")
        return result

    def _transform_fact_tai_chinh(self, api_df: pd.DataFrame) -> pd.DataFrame:
        if api_df.empty:
            logger.warning("  fact_tai_chinh | Không có dữ liệu API")
            return pd.DataFrame()

        result = api_df.copy()

        col_mapping = {
            "ma_sinh_vien": "ma_sinh_vien",
            "hoc_ky": "hoc_ky",
            "hoc_phi_phai_dong": "hoc_phi_phai_dong",
            "da_dong": "da_dong",
            "con_no": "con_no",
            "duoc_mien_giam": "duoc_mien_giam",
            "ly_do_mien_giam": "ly_do_mien_giam",
            "so_tien_mien_giam": "so_tien_mien_giam",
            "ngay_dong_cuoi": "ngay_dong_cuoi",
        }
        result = result.rename(columns={
            k: v for k, v in col_mapping.items() if k in result.columns
        })

        if "ma_sinh_vien" in result.columns:
            result["ma_sinh_vien"] = result["ma_sinh_vien"].str.strip().str.upper()

        for col in ["hoc_phi_phai_dong", "da_dong", "con_no", "so_tien_mien_giam"]:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

        if "ngay_dong_cuoi" in result.columns:
            result["ngay_dong_cuoi"] = pd.to_datetime(
                result["ngay_dong_cuoi"], errors="coerce"
            )

        logger.info(f"  fact_tai_chinh               → {len(result):>6,} records")
        return result

    def _build_agg_student_summary(
        self,
        fact_diem: pd.DataFrame,
        fact_rl: pd.DataFrame,
        fact_tc: pd.DataFrame,
        dk_df: pd.DataFrame,
        hp_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if fact_diem.empty:
            logger.warning("  agg_summary | Không có điểm → bỏ qua tổng hợp")
            return pd.DataFrame()

        gpa_df = self._calculate_semester_gpa(fact_diem, dk_df, hp_df)
        if gpa_df.empty:
            return pd.DataFrame()

        if not fact_rl.empty:
            rl_cols = [
                "ma_sinh_vien", "hoc_ky",
                "diem_ren_luyen", "xep_loai_rl",
                "co_hoc_bong", "bi_ky_luat",
                "loai_hoc_bong", "muc_tien_hb",
            ]
            existing_rl = [c for c in rl_cols if c in fact_rl.columns]
            gpa_df = gpa_df.merge(
                fact_rl[existing_rl],
                left_on=["ma_sinh_vien", "ma_hoc_ky"],
                right_on=["ma_sinh_vien", "hoc_ky"],
                how="left",
            )
            if "hoc_ky" in gpa_df.columns:
                gpa_df = gpa_df.drop(columns=["hoc_ky"])
        else:
            gpa_df["diem_ren_luyen"] = np.nan
            gpa_df["xep_loai_rl"] = np.nan
            gpa_df["co_hoc_bong"] = False
            gpa_df["bi_ky_luat"] = False

        if not fact_tc.empty:
            tc_cols = [
                "ma_sinh_vien", "hoc_ky",
                "hoc_phi_phai_dong", "con_no",
                "duoc_mien_giam",
            ]
            existing_tc = [c for c in tc_cols if c in fact_tc.columns]
            gpa_df = gpa_df.merge(
                fact_tc[existing_tc],
                left_on=["ma_sinh_vien", "ma_hoc_ky"],
                right_on=["ma_sinh_vien", "hoc_ky"],
                how="left",
            )
            if "hoc_ky" in gpa_df.columns:
                gpa_df = gpa_df.drop(columns=["hoc_ky"])
        else:
            gpa_df["hoc_phi_phai_dong"] = 0
            gpa_df["con_no"] = 0
            gpa_df["duoc_mien_giam"] = False

        gpa_df["con_no"] = gpa_df["con_no"].fillna(0)
        gpa_df["hoc_phi_phai_dong"] = gpa_df["hoc_phi_phai_dong"].fillna(0)

        gpa_df["ty_le_no"] = np.where(
            gpa_df["hoc_phi_phai_dong"] > 0,
            (gpa_df["con_no"] / gpa_df["hoc_phi_phai_dong"] * 100).round(1),
            0,
        )
        gpa_df["da_dong_du"] = gpa_df["con_no"] <= 0

        gpa_df["canh_bao_hoc_vu"] = gpa_df["gpa_hoc_ky_he4"] < GPA_WARNING_LEVEL_1

        gpa_df["du_dieu_kien_hoc_bong"] = (
            (gpa_df["gpa_hoc_ky_he4"] >= 3.2)
            & (gpa_df["diem_ren_luyen"].fillna(0) >= 80)
            & (~gpa_df["bi_ky_luat"].fillna(False))
            & (gpa_df["da_dong_du"].fillna(True))
        )

        gpa_df["nguy_co_bo_hoc"] = (
            (gpa_df["gpa_hoc_ky_he4"] < 2.0)
            & (gpa_df["diem_ren_luyen"].fillna(100) < 50)
            & (gpa_df["ty_le_no"].fillna(0) > 50)
        )

        logger.info(f"  agg_student_summary          → {len(gpa_df):>6,} records")

        if not gpa_df.empty:
            n_warn = gpa_df["canh_bao_hoc_vu"].sum()
            n_hb = gpa_df["du_dieu_kien_hoc_bong"].sum()
            n_risk = gpa_df["nguy_co_bo_hoc"].sum()
            logger.info(f"    → Cảnh báo học vụ:       {n_warn:>5}")
            logger.info(f"    → Đủ ĐK học bổng:       {n_hb:>5}")
            logger.info(f"    → Nguy cơ bỏ học:        {n_risk:>5}")

        return gpa_df

    def _calculate_semester_gpa(
        self,
        fact_diem: pd.DataFrame,
        dk_df: pd.DataFrame,
        hp_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if fact_diem.empty:
            return pd.DataFrame()

        diem = fact_diem.copy()

        diem["diem_he_4"] = pd.to_numeric(diem["diem_he_4"], errors="coerce")
        diem["diem_tong_ket"] = pd.to_numeric(diem["diem_tong_ket"], errors="coerce")

        if not hp_df.empty and "ma_hoc_phan" in diem.columns:
            diem = diem.merge(
                hp_df[["ma_hoc_phan", "so_tin_chi"]].drop_duplicates(
                    subset=["ma_hoc_phan"]
                ),
                on="ma_hoc_phan",
                how="left",
            )
        if "so_tin_chi" not in diem.columns:
            diem["so_tin_chi"] = 3

        diem["so_tin_chi"] = diem["so_tin_chi"].fillna(3).astype(int)

        if "dat_mon" not in diem.columns:
            diem["dat_mon"] = diem["diem_tong_ket"] >= 4.0

        diem["weighted_4"] = diem["diem_he_4"] * diem["so_tin_chi"]
        diem["weighted_10"] = diem["diem_tong_ket"] * diem["so_tin_chi"]

        gpa_df = (
            diem
            .groupby(["ma_sinh_vien", "ma_hoc_ky"])
            .agg(
                total_weighted_4=("weighted_4", "sum"),
                total_weighted_10=("weighted_10", "sum"),
                tong_tin_chi=("so_tin_chi", "sum"),
                so_mon_hoc=("ma_hoc_phan", "count"),
                so_mon_rot=("dat_mon", lambda x: (~x).sum()),
                so_mon_dat=("dat_mon", lambda x: x.sum()),
                tin_chi_dat=("so_tin_chi", lambda x: x[diem.loc[x.index, "dat_mon"]].sum()
                             if hasattr(x, "index") else 0),
            )
            .reset_index()
        )

        gpa_df["gpa_hoc_ky_he4"] = np.where(
            gpa_df["tong_tin_chi"] > 0,
            (gpa_df["total_weighted_4"] / gpa_df["tong_tin_chi"]).round(2),
            0,
        )
        gpa_df["gpa_hoc_ky_he10"] = np.where(
            gpa_df["tong_tin_chi"] > 0,
            (gpa_df["total_weighted_10"] / gpa_df["tong_tin_chi"]).round(2),
            0,
        )

        gpa_df["tin_chi_khong_dat"] = gpa_df["tong_tin_chi"] - gpa_df.get(
            "tin_chi_dat", 0
        )
        gpa_df["ty_le_dat"] = np.where(
            gpa_df["so_mon_hoc"] > 0,
            (gpa_df["so_mon_dat"] / gpa_df["so_mon_hoc"] * 100).round(1),
            0,
        )

        gpa_df = gpa_df.drop(
            columns=["total_weighted_4", "total_weighted_10"],
            errors="ignore",
        )

        return gpa_df

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
        return "Yeu"