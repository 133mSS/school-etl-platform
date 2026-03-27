
# src/validation/ge_validation.py — Kiểm soát chất lượng dữ liệu



import os
from typing import Dict, Any
from pathlib import Path

import pandas as pd

from src.utils.minio_client import MinIOClient
from src.utils.logger import get_logger

logger = get_logger("validation.ge")


class DataValidator:
    
    def validate_from_staging(self, run_id: str = None) -> Dict[str, Any]:
      
        client = MinIOClient()

        if run_id is None:
            run_id = client.get_latest_run_id(bucket="raw-data")
            if run_id is None:
                raise FileNotFoundError("Không có staging data để validate.")

        logger.info(f"  Validating staging: run_id={run_id}")

        
        df_sv   = client.download_df("nguon1_sinh_vien.parquet",  run_id)
        df_diem = client.download_df("nguon1_diem.parquet",       run_id)
        df_ctsv = client.download_df("nguon2_ctsv.parquet",       run_id)
        df_tc   = client.download_df("nguon3_tai_chinh.parquet",  run_id)

      
        all_failures = []

        sv_result   = self._validate_students(df_sv)
        diem_result = self._validate_grades(df_diem)
        ctsv_result = self._validate_ctsv(df_ctsv)
        tc_result   = self._validate_tai_chinh(df_tc)

        all_failures.extend(sv_result)
        all_failures.extend(diem_result)
        all_failures.extend(ctsv_result)
        all_failures.extend(tc_result)

        total_checks = 23   
        failed_count = len(all_failures)
        passed_count = total_checks - failed_count

        success = failed_count == 0

        if success:
            logger.info(f"  Validation OK: {passed_count}/{total_checks} passed")
        else:
            logger.error(f"  Validation FAILED: {failed_count} failure(s)")
            for f in all_failures:
                logger.error(f"    - {f}")

        return {
            "success":                success,
            "run_id":                 run_id,
            "evaluated_expectations": total_checks,
            "successful_expectations":passed_count,
            "failed_expectations":    all_failures,
        }

 
    # VALIDATION SUITES
 

    def _validate_students(self, df: pd.DataFrame) -> list:
       
        if df.empty:
            return ["[students] DataFrame rỗng — không có dữ liệu sinh viên"]

        failures = []

     
        if len(df) < 100:
            failures.append(f"[students] EXP-01: quá ít records ({len(df)} < 100)")


        required_cols = ["ma_sinh_vien", "ho", "ten", "email", "khoa_hoc"]
        for col in required_cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    failures.append(f"[students] EXP-02: '{col}' có {null_count} giá trị NULL")


        if "ma_sinh_vien" in df.columns:
            dup = df["ma_sinh_vien"].duplicated().sum()
            if dup > 0:
                failures.append(f"[students] EXP-03: {dup} ma_sinh_vien bị trùng")

  
        if "email" in df.columns:
            invalid_email = df["email"].dropna().apply(
                lambda x: "@" not in str(x)
            ).sum()
            if invalid_email > 0:
                failures.append(f"[students] EXP-04: {invalid_email} email không hợp lệ")

     
        if "khoa_hoc" in df.columns:
            valid_khoa = {"B21", "B22", "B23", "B24"}
            invalid_khoa = (~df["khoa_hoc"].isin(valid_khoa)).sum()
            if invalid_khoa > 0:
                failures.append(f"[students] EXP-05: {invalid_khoa} khoa_hoc không hợp lệ")

      
        if "trang_thai_hoc_tap" in df.columns:
            valid_tt = {"Đang học", "Bảo lưu", "Thôi học", "Tốt nghiệp"}
            invalid_tt = (~df["trang_thai_hoc_tap"].isin(valid_tt)).sum()
            if invalid_tt > 0:
                failures.append(f"[students] EXP-06: {invalid_tt} trang_thai không hợp lệ")

   
        if "ma_sinh_vien" in df.columns:
            pattern = r"^B\d{2}[A-Z]+\d{3,}$"
            invalid_ma = (~df["ma_sinh_vien"].str.match(pattern, na=False)).sum()
            if invalid_ma > 0:
                failures.append(f"[students] EXP-07: {invalid_ma} ma_sinh_vien sai format")

        return failures

    def _validate_grades(self, df: pd.DataFrame) -> list:
        
        if df.empty:
            return ["[grades] DataFrame rỗng"]

        failures = []

        
        if len(df) < 1000:
            failures.append(f"[grades] EXP-08: quá ít records ({len(df)} < 1000)")

 
        score_cols = ["diem_chuyen_can", "diem_bai_tap", "diem_giua_ky",
                      "diem_cuoi_ky", "diem_tong_ket"]
        for col in score_cols:
            if col in df.columns:
                out_of_range = df[col].dropna().apply(
                    lambda x: float(x) < 0 or float(x) > 10
                ).sum()
                if out_of_range > 0:
                    failures.append(
                        f"[grades] EXP-09: '{col}' có {out_of_range} giá trị ngoài [0,10]"
                    )


        if "diem_tong_ket" in df.columns:
            null_pct = df["diem_tong_ket"].isna().mean() * 100
            if null_pct > 20:
                failures.append(
                    f"[grades] EXP-10: {null_pct:.1f}% diem_tong_ket là NULL (ngưỡng 20%)"
                )

   
        if "dat_mon" in df.columns and "diem_tong_ket" in df.columns:
            both_valid = df[df["dat_mon"].notna() & df["diem_tong_ket"].notna()]
            inconsistent = (
                (both_valid["dat_mon"] == True)  & (both_valid["diem_tong_ket"].astype(float) < 4.0) |
                (both_valid["dat_mon"] == False) & (both_valid["diem_tong_ket"].astype(float) >= 4.0)
            ).sum()
            if inconsistent > 0:
                failures.append(
                    f"[grades] EXP-11: {inconsistent} records dat_mon không nhất quán với diem_tong_ket"
                )

        return failures

    def _validate_ctsv(self, df: pd.DataFrame) -> list:
       
        if df.empty:
            logger.warning("  [ctsv] Không có data CTSV — bỏ qua validation")
            return []

        failures = []

        for col in ["ma_sinh_vien", "hoc_ky"]:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    failures.append(f"[ctsv] EXP-12: '{col}' có {null_count} NULL")

        if "diem_ren_luyen" in df.columns:
            out_of_range = df["diem_ren_luyen"].dropna().apply(
                lambda x: float(x) < 0 or float(x) > 100
            ).sum()
            if out_of_range > 0:
                failures.append(f"[ctsv] EXP-13: {out_of_range} diem_ren_luyen ngoài [0,100]")


        if "muc_tien_hb" in df.columns:
            negative = (df["muc_tien_hb"].fillna(0) < 0).sum()
            if negative > 0:
                failures.append(f"[ctsv] EXP-14: {negative} muc_tien_hb âm")

        return failures

    def _validate_tai_chinh(self, df: pd.DataFrame) -> list:
       
        if df.empty:
            logger.warning("  [tai_chinh] Không có data tài chính — bỏ qua")
            return []

        failures = []

       
        if "hoc_phi_phai_dong" in df.columns:
            negative = (df["hoc_phi_phai_dong"].fillna(0) < 0).sum()
            if negative > 0:
                failures.append(f"[tai_chinh] EXP-15: {negative} hoc_phi_phai_dong âm")

        needed = ["hoc_phi_phai_dong", "da_dong", "con_no"]
        if all(c in df.columns for c in needed):
            calc_no    = df["hoc_phi_phai_dong"] - df["da_dong"]
            diff       = (df["con_no"] - calc_no).abs()
            inconsist  = (diff > 1000).sum()
            if inconsist > 0:
                failures.append(
                    f"[tai_chinh] EXP-16: {inconsist} records con_no không khớp công thức"
                )

        return failures