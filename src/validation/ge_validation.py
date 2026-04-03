import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import great_expectations as ge
from great_expectations.core import ExpectationSuite

from src.utils.minio_client import MinIOClient
from src.utils.logger import get_logger

logger = get_logger("validation.ge")

# ── Đường dẫn đến thư mục great_expectations/ ─────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GE_DIR       = _PROJECT_ROOT / "great_expectations"
SUITE_DIR    = GE_DIR / "expectations"

# ── Mapping: tên suite → file JSON ────────────────────────────────────────
# ĐÃ SỬA: Tách attendance_suite thành ctsv_suite + tai_chinh_suite
SUITE_FILES = {
    "students_suite":   SUITE_DIR / "students_suite.json",
    "grades_suite":     SUITE_DIR / "grades_suite.json",
    "ctsv_suite":       SUITE_DIR / "ctsv_suite.json",        # MỚI
    "tai_chinh_suite":  SUITE_DIR / "tai_chinh_suite.json",    # MỚI
    # Giữ lại cho backward compatibility (nếu cần)
    # "attendance_suite": SUITE_DIR / "attendance_suite.json",
}


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _load_suite(suite_name: str) -> ExpectationSuite:
    """
    Load ExpectationSuite từ file JSON trong great_expectations/expectations/.
    ĐÃ SỬA: Tự động loại bỏ tham số 'exact_match' không tương thích GE 0.18.8.
    """
    fpath = SUITE_FILES.get(suite_name)
    if fpath is None:
        raise KeyError(f"Suite '{suite_name}' chưa được đăng ký trong SUITE_FILES")

    if not fpath.exists():
        raise FileNotFoundError(
            f"Không tìm thấy suite file: {fpath}\n"
            f"Hãy kiểm tra thư mục great_expectations/expectations/"
        )

    with open(fpath, "r", encoding="utf-8") as f:
        suite_dict = json.load(f)

    # ── FIX Nhóm 1: Loại bỏ 'exact_match' khỏi kwargs ──────────────────
    # GE 0.18.8 không chấp nhận tham số này trong
    # expect_table_columns_to_match_ordered_list
    for exp in suite_dict.get("expectations", []):
        if exp.get("expectation_type") == "expect_table_columns_to_match_ordered_list":
            kwargs = exp.get("kwargs", {})
            if "exact_match" in kwargs:
                logger.warning(
                    f" GE | Removing unsupported 'exact_match' param "
                    f"from {exp['expectation_type']} in suite '{suite_name}'"
                )
                del kwargs["exact_match"]

    # GE 0.18.8: ExpectationSuite nhận dict từ JSON trực tiếp
    suite = ExpectationSuite(**suite_dict)
    logger.info(
        f" GE | Loaded suite '{suite_name}' "
        f"({len(suite.expectations)} expectations)"
    )
    return suite


def _run_suite(
    df: pd.DataFrame,
    suite_name: str,
    asset_name: str,
) -> Dict[str, Any]:
    """
    Validate DataFrame df với suite được chỉ định.

    Trả về dict:
    {
        "suite_name": str,
        "asset_name": str,
        "success":    bool,
        "evaluated":  int,
        "passed":     int,
        "failed":     int,
        "failures":   list[str],
        "statistics": dict
    }
    """
    if df is None or df.empty:
        logger.warning(f" GE | [{suite_name}] DataFrame rỗng — bỏ qua validation")
        return {
            "suite_name": suite_name,
            "asset_name": asset_name,
            "success":    True,
            "evaluated":  0,
            "passed":     0,
            "failed":     0,
            "failures":   [],
            "statistics": {},
            "skipped":    True,
        }

    # ── Load suite từ file JSON ─────────────────────────────────────────
    suite = _load_suite(suite_name)

    # ── Tạo GE DataFrame và validate ───────────────────────────────────
    ge_df = ge.from_pandas(df, expectation_suite=suite)
    validation_result = ge_df.validate(
        result_format="SUMMARY",
        catch_exceptions=True,
    )

    # ── Parse kết quả ──────────────────────────────────────────────────
    stats     = validation_result.statistics
    evaluated = stats.get("evaluated_expectations", 0)
    passed    = stats.get("successful_expectations", 0)
    failed_cnt = stats.get("unsuccessful_expectations", 0)
    success   = bool(validation_result.success)

    # Thu thập mô tả các expectation thất bại
    failures: List[str] = []
    for er in validation_result.results:
        if not er.success:
            exp_type = er.expectation_config.expectation_type
            kwargs   = er.expectation_config.kwargs
            col      = kwargs.get("column", "")
            mostly   = kwargs.get("mostly", 1.0)

            result_detail  = er.result or {}
            unexpected_pct = result_detail.get("unexpected_percent", None)
            element_count  = result_detail.get("element_count", None)
            unexpected_cnt = result_detail.get("unexpected_count", None)

            msg = f"[{asset_name}] {exp_type}"
            if col:
                msg += f" | column='{col}'"
            if unexpected_pct is not None:
                msg += f" | unexpected={unexpected_pct:.1f}%"
            if unexpected_cnt is not None and element_count is not None:
                msg += f" ({unexpected_cnt}/{element_count} records)"
            if mostly < 1.0:
                msg += f" | mostly={mostly}"

            if er.exception_info and er.exception_info.get("raised_exception"):
                exc_msg = er.exception_info.get("exception_message", "")
                if exc_msg:
                    msg += f" | EXCEPTION: {exc_msg[:80]}"

            failures.append(msg)

    logger.info(
        f" GE | [{suite_name}] {asset_name}: "
        f"{passed}/{evaluated} passed "
        f"({'OK' if success else 'FAIL'})"
    )
    if failures:
        for f in failures:
            logger.warning(f"    FAIL: {f}")

    return {
        "suite_name":  suite_name,
        "asset_name":  asset_name,
        "success":     success,
        "evaluated":   evaluated,
        "passed":      passed,
        "failed":      failed_cnt,
        "failures":    failures,
        "statistics": {
            "evaluated":       evaluated,
            "successful":      passed,
            "unsuccessful":    failed_cnt,
            "success_percent": stats.get("success_percent", 0.0),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# CLASS CHÍNH — DataValidator
# ═══════════════════════════════════════════════════════════════════════════

class DataValidator:
    """
    Validate dữ liệu từ MinIO staging bằng Great Expectations.
    """

    def __init__(self):
        if not GE_DIR.exists():
            raise FileNotFoundError(
                f"Không tìm thấy thư mục Great Expectations: {GE_DIR}\n"
                f"Đảm bảo thư mục great_expectations/ tồn tại ở project root."
            )
        if not SUITE_DIR.exists():
            raise FileNotFoundError(
                f"Không tìm thấy thư mục expectations: {SUITE_DIR}\n"
                f"Tạo thư mục great_expectations/expectations/ và thêm suite JSON."
            )
        logger.info(f" GE | DataValidator khởi tạo | GE dir: {GE_DIR}")

    # ─────────────────────────────────────────────────────────────────────
    # ENTRY POINT CHÍNH
    # ─────────────────────────────────────────────────────────────────────

    def validate_from_staging(
        self,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Download dữ liệu từ MinIO staging → validate với GE → trả về tổng hợp.
        """
        # ── 1. Kết nối MinIO và lấy run_id ──────────────────────────────
        client = MinIOClient()

        if run_id is None:
            run_id = client.get_latest_run_id(bucket="raw")

        if run_id is None:
            raise FileNotFoundError(
                "Không có staging data trong MinIO.\n"
                "Hãy chạy extract_full() ít nhất 1 lần trước."
            )

        logger.info(f" GE | Bắt đầu validation | run_id={run_id}")
        logger.info(f" GE | GE suites directory: {SUITE_DIR}")

        # ── 2. Download DataFrames từ MinIO ──────────────────────────────
        logger.info(" GE | Downloading staging data từ MinIO...")

        df_sv      = client.download_df("nguon1_sinh_vien.parquet", run_id)
        df_diem    = client.download_df("nguon1_diem.parquet",      run_id)
        df_tong_hop = client.download_df("nguon1_tong_hop_ket_qua.parquet", run_id)
        df_ctsv    = client.download_df("nguon2_ctsv.parquet",      run_id)
        df_tc      = client.download_df("nguon3_tai_chinh.parquet", run_id)

        logger.info(
            f" GE | Downloaded: "
            f"sv={len(df_sv)}, diem={len(df_diem)}, "
            f"ctsv={len(df_ctsv)}, tc={len(df_tc)}"
        )

        # ── 3. Chuẩn bị DataFrame cho từng suite ─────────────────────────
        df_students   = self._prepare_students_df(df_sv, df_tong_hop)
        df_grades     = df_diem.copy() if not df_diem.empty else pd.DataFrame()
        df_ctsv_ready = self._prepare_ctsv_df(df_ctsv)
        df_tc_ready   = self._prepare_tc_df(df_tc)

        # ── 4. Chạy validation cho từng suite ────────────────────────────
        logger.info(" GE | Chạy validation suites...")

        suite_results = {}

        # Suite 1: Sinh viên
        r1 = _run_suite(df_students, "students_suite", "sinh_vien")
        suite_results["students"] = r1

        # Suite 2: Điểm học phần
        r2 = _run_suite(df_grades, "grades_suite", "diem_hoc_phan")
        suite_results["grades"] = r2

        # ── FIX Nhóm 2+3: Dùng suite riêng cho từng DataFrame ───────────
        # Suite 3: CTSV → dùng ctsv_suite (KHÔNG còn dùng attendance_suite)
        r3a = _run_suite(df_ctsv_ready, "ctsv_suite", "ctsv_data")
        suite_results["ctsv"] = r3a

        # Suite 4: Tài chính → dùng tai_chinh_suite
        r3b = _run_suite(df_tc_ready, "tai_chinh_suite", "tai_chinh_data")
        suite_results["tai_chinh"] = r3b

        # ── 5. Tổng hợp kết quả ──────────────────────────────────────────
        all_failures: List[str] = []
        total_evaluated  = 0
        total_successful = 0

        for name, r in suite_results.items():
            if r.get("skipped"):
                continue
            total_evaluated  += r["evaluated"]
            total_successful += r["passed"]
            all_failures.extend(r["failures"])

        total_failed    = len(all_failures)
        overall_success = total_failed == 0

        # ── 6. Log tổng kết ──────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info(f" GE | VALIDATION {'OK' if overall_success else 'FAILED'}")
        logger.info(f" GE | run_id         : {run_id}")
        logger.info(f" GE | Tổng expectations : {total_evaluated}")
        logger.info(f" GE | Passed           : {total_successful}")
        logger.info(f" GE | Failed           : {total_failed}")
        logger.info(f" GE | Suite results:")
        for name, r in suite_results.items():
            status = "SKIP" if r.get("skipped") else ("OK" if r["success"] else "FAIL")
            logger.info(
                f"     {name:<15s}: {status} "
                f"({r.get('passed',0)}/{r.get('evaluated',0)})"
            )
        if all_failures:
            logger.error(f" GE | {total_failed} expectations thất bại:")
            for f in all_failures:
                logger.error(f"    FAIL: {f}")
        logger.info("=" * 60)

        return {
            "success":                 overall_success,
            "run_id":                  run_id,
            "evaluated_expectations":  total_evaluated,
            "successful_expectations": total_successful,
            "failed_expectations":     all_failures,
            "suite_results":           suite_results,
        }

    # ─────────────────────────────────────────────────────────────────────
    # HELPERS CHUẨN BỊ DỮ LIỆU
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _prepare_students_df(
        df_sv: pd.DataFrame,
        df_tong_hop: pd.DataFrame,
    ) -> pd.DataFrame:
        if df_sv.empty:
            return pd.DataFrame()

        df = df_sv.copy()
        for col in ["ho", "ten", "email", "khoa_hoc",
                     "trang_thai_hoc_tap", "ma_nganh", "ma_lop"]:
            if col not in df.columns:
                df[col] = None
        return df

    @staticmethod
    def _prepare_ctsv_df(df_ctsv: pd.DataFrame) -> pd.DataFrame:
        """
        Chuẩn bị DataFrame CTSV cho ctsv_suite.
        """
        if df_ctsv.empty:
            return pd.DataFrame()

        df = df_ctsv.copy()

        for col in ["ma_sinh_vien", "hoc_ky", "diem_ren_luyen",
                     "xep_loai_rl", "muc_tien_hb",
                     "hinh_thuc_ky_luat", "ly_do_ky_luat", "loai_hoc_bong"]:
            if col not in df.columns:
                df[col] = None

        df["diem_ren_luyen"] = pd.to_numeric(
            df["diem_ren_luyen"], errors="coerce"
        )
        df["muc_tien_hb"] = pd.to_numeric(
            df["muc_tien_hb"], errors="coerce"
        ).fillna(0)

        return df

    @staticmethod
    def _prepare_tc_df(df_tc: pd.DataFrame) -> pd.DataFrame:
        """
        Chuẩn bị DataFrame tài chính cho tai_chinh_suite.
        """
        if df_tc.empty:
            return pd.DataFrame()

        df = df_tc.copy()

        for col in ["ma_sinh_vien", "hoc_ky",
                     "hoc_phi_phai_dong", "da_dong", "con_no",
                     "so_tien_mien_giam"]:
            if col not in df.columns:
                df[col] = 0 if col != "ma_sinh_vien" and col != "hoc_ky" else None

        for col in ["hoc_phi_phai_dong", "da_dong", "con_no", "so_tien_mien_giam"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df