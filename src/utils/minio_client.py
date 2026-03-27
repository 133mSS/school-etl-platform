import io
import os
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.config.settings import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_RAW,
    MINIO_BUCKET_STAGING,
)
from src.utils.logger import get_logger

logger = get_logger("utils.minio")


class MinIOClient:
    """
    Wrapper đơn giản để làm việc với MinIO.

    
        client = MinIOClient()

        # Upload DataFrame
        client.upload_df(df, "nguon1_sinh_vien.parquet", bucket="raw")

        # Download về DataFrame
        df = client.download_df("nguon1_sinh_vien.parquet", run_id="2024-01-15_02-00")

        # Liệt kê các lần chạy đã lưu
        runs = client.list_runs()  # ['2024-01-15_02-00', '2024-01-14_02-00', ...]
    """

    def __init__(self):
        # endpoint_url: dùng http (không phải https) cho MinIO local
        # boto3 mặc định kết nối AWS S3, cần chỉ endpoint_url để trỏ vào MinIO
        self._s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        self._bucket_raw     = MINIO_BUCKET_RAW       # "raw-data"
        self._bucket_staging = MINIO_BUCKET_STAGING   # "staging-data"

        # Đảm bảo cả 2 bucket tồn tại khi khởi tạo
        self._ensure_bucket(self._bucket_raw)
        self._ensure_bucket(self._bucket_staging)

    # ─────────────────────────────────────────────────
    # INTERNAL
    # ─────────────────────────────────────────────────

    def _ensure_bucket(self, bucket_name: str) -> None:
        """Tạo bucket nếu chưa tồn tại. Không làm gì nếu đã có."""
        try:
            # head_bucket: kiểm tra bucket có tồn tại không
            # Nếu không tồn tại → raise ClientError với code "404" hoặc "NoSuchBucket"
            self._s3.head_bucket(Bucket=bucket_name)
        except ClientError:
            self._s3.create_bucket(Bucket=bucket_name)
            logger.info(f"  MinIO tạo bucket: {bucket_name}")

    def _resolve_bucket(self, bucket: str) -> str:
        """
        Chuyển tên ngắn sang tên bucket thật.
          "raw"     → MINIO_BUCKET_RAW     (raw-data)
          "staging" → MINIO_BUCKET_STAGING (staging-data)
          khác      → dùng nguyên
        """
        if bucket == "raw":
            return self._bucket_raw
        if bucket == "staging":
            return self._bucket_staging
        return bucket

    @staticmethod
    def make_run_id() -> str:
        """
        Tạo run_id theo timestamp hiện tại.
        Format: YYYY-MM-DD_HH-MM  →  VD: "2024-01-15_02-00"

        Dùng làm tên folder trong MinIO để phân biệt các lần chạy.
        """
        return datetime.now().strftime("%Y-%m-%d_%H-%M")

    # ─────────────────────────────────────────────────
    # UPLOAD
    # ─────────────────────────────────────────────────

    def upload_df(
        self,
        df: pd.DataFrame,
        file_name: str,
        run_id: str,
        bucket: str = "raw",
    ) -> bool:
        """
        Upload 1 DataFrame lên MinIO dưới dạng Parquet.

        Args:
            df        : DataFrame cần upload
            file_name : tên file, VD "nguon1_sinh_vien.parquet"
            run_id    : folder timestamp, VD "2024-01-15_02-00"
            bucket    : "raw" hoặc "staging" (default "raw")

        Returns:
            True nếu upload thành công, False nếu lỗi.

        Object key trong MinIO = "{run_id}/{file_name}"
        VD: "2024-01-15_02-00/nguon1_sinh_vien.parquet"
        """
        if df is None or df.empty:
            logger.warning(f"  MinIO skip (empty): {file_name}")
            return True  # không phải lỗi, chỉ skip

        bucket_name = self._resolve_bucket(bucket)
        object_key  = f"{run_id}/{file_name}"

        try:
            # Dùng io.BytesIO để ghi Parquet vào RAM
            # Lý do không ghi ra file tạm: sạch hơn, không cần quản lý temp file
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")

            # buffer.tell() = vị trí con trỏ hiện tại = kích thước đã ghi
            size_kb = buffer.tell() / 1024

            # Quay về đầu buffer để boto3 đọc từ đầu khi upload
            buffer.seek(0)

            # upload_fileobj: upload từ file-like object (BytesIO)
            # Khác upload_file: upload từ đường dẫn file trên disk
            self._s3.upload_fileobj(buffer, bucket_name, object_key)

            logger.info(
                f"  MinIO ↑ [{bucket_name}] {object_key}"
                f" ({size_kb:.1f} KB | {len(df):,} rows)"
            )
            return True

        except Exception as e:
            logger.error(f"  MinIO upload lỗi [{object_key}]: {e}")
            return False

    def upload_all_extracted(
        self, data, run_id: str
    ) -> dict:
        """
        Upload toàn bộ ExtractedData lên MinIO bucket raw-data.

        Args:
            data   : ExtractedData instance (từ extract.py)
            run_id : timestamp folder

        Returns:
            dict: {"file_name": True/False, ...} — kết quả từng file
        """
        # Mapping: tên file → attribute trong ExtractedData
        # Prefix "nguon1_", "nguon2_", "nguon3_" để dễ nhìn trong MinIO Console
        upload_map = {
            "nguon1_sinh_vien.parquet":        getattr(data, "sinh_vien",        pd.DataFrame()),
            "nguon1_dang_ky.parquet":          getattr(data, "dang_ky_hoc_phan", pd.DataFrame()),
            "nguon1_diem.parquet":             getattr(data, "diem_hoc_phan",    pd.DataFrame()),
            "nguon1_hoc_phan.parquet":         getattr(data, "hoc_phan",         pd.DataFrame()),
            "nguon1_hoc_ky.parquet":           getattr(data, "hoc_ky_nam_hoc",   pd.DataFrame()),
            "nguon1_giang_vien.parquet":       getattr(data, "giang_vien",       pd.DataFrame()),
            "nguon1_khoa.parquet":             getattr(data, "khoa",             pd.DataFrame()),
            "nguon1_nganh.parquet":            getattr(data, "nganh",            pd.DataFrame()),
            "nguon1_lop_hanh_chinh.parquet":   getattr(data, "lop_hanh_chinh",   pd.DataFrame()),
            "nguon1_tong_hop_ket_qua.parquet": getattr(data, "tong_hop_ket_qua", pd.DataFrame()),
            "nguon2_ctsv.parquet":             getattr(data, "ctsv_data",        pd.DataFrame()),
            "nguon3_tai_chinh.parquet":        getattr(data, "tai_chinh_data",   pd.DataFrame()),
        }

        results = {}
        for file_name, df in upload_map.items():
            results[file_name] = self.upload_df(df, file_name, run_id, bucket="raw")

        success = sum(results.values())
        total   = len(results)
        logger.info(f"  MinIO upload xong: {success}/{total} files | run_id={run_id}")
        return results

    # ─────────────────────────────────────────────────
    # DOWNLOAD
    # ─────────────────────────────────────────────────

    def download_df(
        self,
        file_name: str,
        run_id: str,
        bucket: str = "raw",
    ) -> pd.DataFrame:
        """
        Download 1 file Parquet từ MinIO → DataFrame.

        Args:
            file_name : tên file, VD "nguon1_sinh_vien.parquet"
            run_id    : folder timestamp, VD "2024-01-15_02-00"
            bucket    : "raw" hoặc "staging"

        Returns:
            DataFrame, hoặc DataFrame rỗng nếu file không tồn tại.
        """
        bucket_name = self._resolve_bucket(bucket)
        object_key  = f"{run_id}/{file_name}"

        try:
            buffer = io.BytesIO()

            # download_fileobj: download từ S3 vào file-like object
            self._s3.download_fileobj(bucket_name, object_key, buffer)
            buffer.seek(0)

            df = pd.read_parquet(buffer, engine="pyarrow")
            logger.info(f"  MinIO ↓ [{bucket_name}] {object_key} ({len(df):,} rows)")
            return df

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                logger.warning(f"  MinIO không tìm thấy: {object_key}")
            else:
                logger.error(f"  MinIO download lỗi [{object_key}]: {e}")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"  MinIO download lỗi [{object_key}]: {e}")
            return pd.DataFrame()

    # ─────────────────────────────────────────────────
    # LIST
    # ─────────────────────────────────────────────────

    def list_runs(self, bucket: str = "raw") -> list:
        """
        Liệt kê tất cả run_id (folder theo timestamp) trong bucket.

        Returns:
            list[str] — VD: ["2024-01-15_02-00", "2024-01-14_02-00", ...]
                        Sắp xếp mới nhất lên đầu.
        """
        bucket_name = self._resolve_bucket(bucket)

        try:
            # list_objects_v2 với Delimiter="/" → chỉ liệt kê "folder" cấp 1
            # CommonPrefixes = danh sách folder, Delimiter ngăn đệ quy vào subfolder
            response = self._s3.list_objects_v2(
                Bucket=bucket_name,
                Delimiter="/",
            )

            # Mỗi CommonPrefix có dạng "2024-01-15_02-00/"
            # Cần bỏ dấu "/" ở cuối để lấy run_id
            runs = [
                p["Prefix"].rstrip("/")
                for p in response.get("CommonPrefixes", [])
            ]
            return sorted(runs, reverse=True)  # mới nhất lên đầu

        except Exception as e:
            logger.error(f"  MinIO list_runs lỗi: {e}")
            return []

    def get_latest_run_id(self, bucket: str = "raw") -> Optional[str]:
        """
        Lấy run_id của lần chạy mới nhất.

        Returns:
            str run_id hoặc None nếu chưa có lần chạy nào.
        """
        runs = self.list_runs(bucket)
        if not runs:
            logger.warning("  MinIO chưa có staging data nào.")
            return None
        logger.info(f"  MinIO run mới nhất: {runs[0]}")
        return runs[0]