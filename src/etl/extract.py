import os
import glob
import json as json_lib
from typing import Dict, List, Optional
from dataclasses import dataclass, field

import pandas as pd

from src.config.settings import (
    CSV_DATA_DIR,
    API_BASE_URL,
    API_JSON_DIR,
    API_TIMEOUT,
    API_MAX_RETRIES,
)
from src.config.database import source_engine
from src.utils.logger import get_logger
from src.utils.minio_client import MinIOClient

logger = get_logger("etl.extract")

@dataclass
class ExtractedData:
    khoa: pd.DataFrame = field(default_factory=pd.DataFrame)
    nganh: pd.DataFrame = field(default_factory=pd.DataFrame)
    giang_vien: pd.DataFrame = field(default_factory=pd.DataFrame)
    lop_hanh_chinh: pd.DataFrame = field(default_factory=pd.DataFrame)
    sinh_vien: pd.DataFrame = field(default_factory=pd.DataFrame)
    hoc_phan: pd.DataFrame = field(default_factory=pd.DataFrame)
    hoc_ky_nam_hoc: pd.DataFrame = field(default_factory=pd.DataFrame)
    dang_ky_hoc_phan: pd.DataFrame = field(default_factory=pd.DataFrame)
    diem_hoc_phan: pd.DataFrame = field(default_factory=pd.DataFrame)
    tong_hop_ket_qua: pd.DataFrame = field(default_factory=pd.DataFrame)
    ctsv_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    tai_chinh_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    extract_timestamp: str = ""

    def summary(self) -> Dict[str, int]:
        counts = {}
        for attr_name in [
            "khoa", "nganh", "giang_vien", "lop_hanh_chinh",
            "sinh_vien", "hoc_phan", "hoc_ky_nam_hoc",
            "dang_ky_hoc_phan", "diem_hoc_phan", "tong_hop_ket_qua",
            "ctsv_data", "tai_chinh_data",
        ]:
            df = getattr(self, attr_name)
            counts[attr_name] = len(df) if isinstance(df, pd.DataFrame) else 0
        return counts

class PostgreSQLExtractor:
    SOURCE_TABLES = [
        "khoa", "nganh", "giang_vien", "lop_hanh_chinh",
        "sinh_vien", "hoc_phan", "hoc_ky_nam_hoc",
        "dang_ky_hoc_phan", "diem_hoc_phan", "tong_hop_ket_qua",
    ]

    def __init__(self):
        self.engine = source_engine

    def _read_table(self, table_name: str, query: str = None) -> pd.DataFrame:
        try:
            if query:
                df = pd.read_sql(query, self.engine)
            else:
                df = pd.read_sql_table(table_name, self.engine)
            logger.info(f"  PostgreSQL | {table_name:<25s} → {len(df):>6,} records")
            return df
        except Exception as e:
            logger.error(f"  PostgreSQL | Lỗi đọc '{table_name}': {e}")
            return pd.DataFrame()

    def extract_students_from_postgres(self) -> pd.DataFrame:
        return self._read_table("sinh_vien")

    def extract_grades_from_postgres(self) -> pd.DataFrame:
        return self._read_table("diem_hoc_phan")

    def extract_enrollments_from_postgres(self) -> pd.DataFrame:
        return self._read_table("dang_ky_hoc_phan")

    def extract_all(self) -> Dict[str, pd.DataFrame]:
        logger.info("═" * 60)
        logger.info("NGUỒN 1 — PostgreSQL (Phòng Đào tạo) | Full Extract")
        logger.info("═" * 60)

        result = {}
        for table in self.SOURCE_TABLES:
            result[table] = self._read_table(table)

        total = sum(len(df) for df in result.values())
        logger.info(f"  PostgreSQL | TỔNG: {total:,} records từ {len(self.SOURCE_TABLES)} bảng")
        return result

    def extract_by_semester(self, ma_hoc_ky: str) -> Dict[str, pd.DataFrame]:
        logger.info(f"  PostgreSQL | Incremental cho HK: {ma_hoc_ky}")

        result = {}
        for table in ["khoa", "nganh", "giang_vien", "lop_hanh_chinh",
                       "sinh_vien", "hoc_phan", "hoc_ky_nam_hoc"]:
            result[table] = self._read_table(table)

        result["dang_ky_hoc_phan"] = self._read_table(
            "dang_ky_hoc_phan",
            f"SELECT * FROM dang_ky_hoc_phan WHERE ma_hoc_ky = '{ma_hoc_ky}'"
        )
        result["diem_hoc_phan"] = self._read_table(
            "diem_hoc_phan",
            f"""SELECT d.* FROM diem_hoc_phan d
                JOIN dang_ky_hoc_phan dk ON d.ma_dang_ky = dk.ma_dang_ky
                WHERE dk.ma_hoc_ky = '{ma_hoc_ky}'"""
        )
        result["tong_hop_ket_qua"] = self._read_table("tong_hop_ket_qua")
        return result

class CSVExtractor:
    EXPECTED_COLUMNS = [
        "ma_sinh_vien", "hoc_ky", "diem_ren_luyen", "xep_loai_rl",
        "loai_hoc_bong", "muc_tien_hb", "hinh_thuc_ky_luat", "ly_do_ky_luat",
    ]

    def __init__(self, csv_dir: str = None):
        self.csv_dir = csv_dir or CSV_DATA_DIR

    def extract_courses_from_csv(self, filepath: str) -> pd.DataFrame:
        return self._read_single_file(filepath)

    def _read_single_file(self, file_path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                file_path, encoding="utf-8",
                dtype={"ma_sinh_vien": str, "hoc_ky": str},
                na_values=["", "NULL", "null", "N/A"],
            )
            df.columns = df.columns.str.strip().str.lower()

            missing = set(self.EXPECTED_COLUMNS) - set(df.columns)
            if missing:
                logger.warning(f"  CSV | '{file_path}' thiếu cột: {missing}")

            logger.info(f"  CSV | {os.path.basename(file_path):<35s} → {len(df):>6,} records")
            return df
        except FileNotFoundError:
            logger.error(f"  CSV | Không tìm thấy: {file_path}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"  CSV | Lỗi đọc '{file_path}': {e}")
            return pd.DataFrame()

    def extract_by_semester(self, ma_hoc_ky: str) -> pd.DataFrame:
        candidates = [
            f"ctsv_{ma_hoc_ky}.csv",
            f"ctsv_{ma_hoc_ky.replace('-', '_')}.csv",
        ]
        for fname in candidates:
            fpath = os.path.join(self.csv_dir, fname)
            if os.path.exists(fpath):
                return self._read_single_file(fpath)

        logger.warning(f"  CSV | Không tìm thấy file cho HK: {ma_hoc_ky}")
        return pd.DataFrame()

    def extract_all(self) -> pd.DataFrame:
        logger.info("═" * 60)
        logger.info("NGUỒN 2 — CSV (Phòng CTSV) | Extract")
        logger.info("═" * 60)

        csv_files = sorted(glob.glob(os.path.join(self.csv_dir, "ctsv_*.csv")))
        csv_files = [f for f in csv_files if "all" not in os.path.basename(f).lower()]

        if not csv_files:
            logger.warning(f"  CSV | Không tìm thấy file trong: {self.csv_dir}")
            return pd.DataFrame()

        logger.info(f"  CSV | Tìm thấy {len(csv_files)} file(s)")

        all_dfs = []
        for fp in csv_files:
            df = self._read_single_file(fp)
            if not df.empty:
                all_dfs.append(df)

        if not all_dfs:
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True)

        before = len(combined)
        combined = combined.drop_duplicates(subset=["ma_sinh_vien", "hoc_ky"], keep="last")
        dupes = before - len(combined)
        if dupes > 0:
            logger.info(f"  CSV | Loại bỏ {dupes} bản ghi trùng")

        logger.info(f"  CSV | TỔNG: {len(combined):,} records từ {len(all_dfs)} file(s)")
        return combined

class APIExtractor:
    def __init__(self, base_url: str = None, json_dir: str = None):
        self.base_url = (base_url or API_BASE_URL).rstrip("/")
        self.json_dir = json_dir or API_JSON_DIR

    def extract_enrollments_from_api(self, ma_hoc_ky: str) -> pd.DataFrame:
        return self.extract_by_semester(ma_hoc_ky)

    def extract_by_semester(self, ma_hoc_ky: str) -> pd.DataFrame:
        df = self._try_http_api(ma_hoc_ky)
        if not df.empty:
            return df

        df = self._try_json_file(ma_hoc_ky)
        if not df.empty:
            return df

        logger.warning(f"  API | Không có dữ liệu cho HK: {ma_hoc_ky}")
        return pd.DataFrame()

    def _try_http_api(self, ma_hoc_ky: str) -> pd.DataFrame:
        try:
            import requests
            url = f"{self.base_url}/api/tai-chinh/sinh-vien"
            resp = requests.get(url, params={"hoc_ky": ma_hoc_ky}, timeout=API_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            records = data["data"] if isinstance(data, dict) and "data" in data else data
            df = pd.DataFrame(records)
            logger.info(f"  API | (HTTP) HK {ma_hoc_ky} → {len(df):>6,} records")
            return df
        except Exception:
            return pd.DataFrame()

    def _try_json_file(self, ma_hoc_ky: str) -> pd.DataFrame:
        candidates = [
            f"taichinh_{ma_hoc_ky.replace('-', '_')}.json",
            f"taichinh_{ma_hoc_ky}.json",
        ]
        for fname in candidates:
            fpath = os.path.join(self.json_dir, fname)
            if os.path.exists(fpath):
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        records = json_lib.load(f)
                    df = pd.DataFrame(records)
                    logger.info(f"  API | (JSON) {fname} → {len(df):>6,} records")
                    return df
                except Exception as e:
                    logger.error(f"  API | Lỗi đọc JSON: {e}")
        return pd.DataFrame()

    def extract_all_semesters(self, semester_list: List[str]) -> pd.DataFrame:
        all_dfs = []
        for hk in semester_list:
            df = self.extract_by_semester(hk)
            if not df.empty:
                all_dfs.append(df)

        if not all_dfs:
            all_file = os.path.join(self.json_dir, "taichinh_all.json")
            if os.path.exists(all_file):
                with open(all_file, "r", encoding="utf-8") as f:
                    records = json_lib.load(f)
                combined = pd.DataFrame(records)
                logger.info(f"  API | (taichinh_all.json) → {len(combined):>6,} records")
                return combined
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"  API | TỔNG: {len(combined):,} records từ {len(all_dfs)} HK")
        return combined

class DataExtractor:
    def __init__(self):
        self.pg  = PostgreSQLExtractor()
        self.csv = CSVExtractor()
        self.api = APIExtractor()
        self._last_run_id: str = ""

    def extract_full(self, semester_list: List[str] = None) -> ExtractedData:
        logger.info("==BẮT ĐẦU FULL EXTRACT==")
        logger.info("=" * 70)

        result = ExtractedData()
        result.extract_timestamp = pd.Timestamp.now().isoformat()

        pg_data = self.pg.extract_all()
        for table_name, df in pg_data.items():
            setattr(result, table_name, df)

        result.ctsv_data = self.csv.extract_all()

        if semester_list is None and not result.hoc_ky_nam_hoc.empty:
            semester_list = result.hoc_ky_nam_hoc["ma_hoc_ky"].tolist()
        if semester_list:
            result.tai_chinh_data = self.api.extract_all_semesters(semester_list)

        self._save_to_staging(result)

        logger.info("=" * 70)
        logger.info(" FULL EXTRACT HOÀN TẤT")
        for name, count in result.summary().items():
            logger.info(f"   {name:<25s}: {count:>8,}")
        logger.info("=" * 70)

        return result

    def extract_incremental(self, ma_hoc_ky: str) -> ExtractedData:
        logger.info(f" INCREMENTAL EXTRACT — {ma_hoc_ky}")

        result = ExtractedData()
        result.extract_timestamp = pd.Timestamp.now().isoformat()

        pg_data = self.pg.extract_by_semester(ma_hoc_ky)
        for table_name, df in pg_data.items():
            setattr(result, table_name, df)

        result.ctsv_data      = self.csv.extract_by_semester(ma_hoc_ky)
        result.tai_chinh_data = self.api.extract_by_semester(ma_hoc_ky)

        self._save_to_staging(result)

        logger.info(f" INCREMENTAL EXTRACT HOÀN TẤT — {ma_hoc_ky}")
        return result

    def _save_to_staging(self, data: ExtractedData) -> None:
        try:
            client = MinIOClient()
            run_id = MinIOClient.make_run_id()

            results = client.upload_all_extracted(data, run_id)

            self._last_run_id = run_id

            success = sum(results.values())
            total   = len(results)
            logger.info(
                f"  MinIO staging: {success}/{total} files OK"
                f" → run_id={run_id}"
            )

        except Exception as e:
            logger.warning(f"  MinIO staging thất bại (pipeline vẫn tiếp tục): {e}")

    def load_from_staging(self, run_id: str = None) -> ExtractedData:
        client = MinIOClient()

        if run_id is None:
            run_id = client.get_latest_run_id(bucket="raw")
            if run_id is None:
                raise FileNotFoundError(
                    "Không tìm thấy staging data trong MinIO. "
                    "Hãy chạy extract_full() ít nhất 1 lần trước."
                )

        logger.info(f"  Load from MinIO staging: run_id={run_id}")

        data = ExtractedData(
            khoa             = client.download_df("nguon1_khoa.parquet",             run_id),
            nganh            = client.download_df("nguon1_nganh.parquet",            run_id),
            giang_vien       = client.download_df("nguon1_giang_vien.parquet",       run_id),
            lop_hanh_chinh   = client.download_df("nguon1_lop_hanh_chinh.parquet",   run_id),
            sinh_vien        = client.download_df("nguon1_sinh_vien.parquet",        run_id),
            hoc_phan         = client.download_df("nguon1_hoc_phan.parquet",         run_id),
            hoc_ky_nam_hoc   = client.download_df("nguon1_hoc_ky.parquet",           run_id),
            dang_ky_hoc_phan = client.download_df("nguon1_dang_ky.parquet",          run_id),
            diem_hoc_phan    = client.download_df("nguon1_diem.parquet",             run_id),
            tong_hop_ket_qua = client.download_df("nguon1_tong_hop_ket_qua.parquet", run_id),
            ctsv_data        = client.download_df("nguon2_ctsv.parquet",             run_id),
            tai_chinh_data   = client.download_df("nguon3_tai_chinh.parquet",        run_id),
        )

        logger.info(f" Load from staging OK — run_id={run_id}")
        return data