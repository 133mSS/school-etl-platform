# src/etl/extract.py
"""
extract.py - Extract dữ liệu từ 3 nguồn độc lập:

  NGUỒN 1 — PostgreSQL (Phòng Đào tạo)
    Hệ thống học vụ chính thức. ETL đọc trực tiếp qua SQLAlchemy.
    Dữ liệu: sinh_vien, dang_ky, diem, hoc_phan, giang_vien, ...

  NGUỒN 2 — CSV (Phòng Công tác Sinh viên)
    Phòng CTSV dùng phần mềm riêng, không kết nối hệ thống học vụ.
    Mỗi học kỳ họ export file Excel gửi cho bộ phận phân tích.
    File: data/sources/ctsv_hoc_ky.csv
    Nội dung: diem_rl, xep_loai_rl, hoc_bong, ky_luat theo từng HK

  NGUỒN 3 — REST API (Portal Tài chính — vendor bên ngoài)
    Portal do công ty phần mềm cung cấp, trường không có quyền
    truy cập DB của họ — chỉ gọi được API được cấp phép.
    Endpoint: GET /api/tai-chinh/hoc-phi
    Nội dung: tinh trang hoc phi, no hoc phi, mien giam theo từng HK

Bài toán chỉ giải được khi join đủ 3 nguồn:
  - Phát hiện SV nguy cơ bỏ học (GPA thấp + DRL yếu + nợ HP)
  - Xét học bổng tự động (GPA + DRL + không kỷ luật + không nợ HP)
  - Tác động nợ học phí đến kết quả thi cuối kỳ
"""

import io
import time
import pandas as pd
import requests
from datetime import datetime
from minio import Minio

from src.config.settings import (
    SOURCE_DB_URL,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    MINIO_BUCKET_RAW
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

PORTAL_API_BASE = "http://localhost:5050"


# ══════════════════════════════════════════════════
# PHẦN 1 — POSTGRESQL EXTRACTOR (Phòng Đào tạo)
# ══════════════════════════════════════════════════

def extract_table(table_name: str, last_updated: str = None) -> pd.DataFrame:
    """
    Đọc một bảng từ Source DB ra DataFrame.

    Tham số:
        table_name   : tên bảng, ví dụ 'sinh_vien'
        last_updated : nếu có → incremental load (chỉ lấy bản ghi mới hơn)
                       nếu None → full load toàn bộ bảng
    """
    if last_updated:
        query = f"SELECT * FROM {table_name} WHERE ngay_tao > '{last_updated}'"
        logger.info(f"Incremental extract: {table_name} (sau {last_updated})")
    else:
        query = f"SELECT * FROM {table_name}"
        logger.info(f"Full extract: {table_name}")

    try:
        df = pd.read_sql(query, SOURCE_DB_URL)
        logger.info(f"  → {len(df):,} rows extracted từ {table_name}")
        return df
    except Exception as e:
        logger.error(f"  → Lỗi extract {table_name}: {e}")
        raise


def extract_all_tables() -> dict:
    """
    Extract toàn bộ bảng cần thiết từ PostgreSQL source.
    Trả về dict: { 'sinh_vien': DataFrame, ... }
    """
    logger.info("=" * 55)
    logger.info("NGUON 1 - PostgreSQL (Phong Dao tao)")
    logger.info("=" * 55)

    tables = [
        "co_so", "khoa", "nganh", "giang_vien",
        "lop_hanh_chinh", "sinh_vien", "hoc_phan",
        "hoc_ky_nam_hoc", "dang_ky_hoc_phan",
        "diem_hoc_phan", "tong_hop_ket_qua"
    ]
    data = {}
    for table in tables:
        data[table] = extract_table(table)

    logger.info(f"  -> PostgreSQL xong: {len(tables)} bang")
    return data


# ══════════════════════════════════════════════════
# PHẦN 2 — CSV EXTRACTOR (Phòng Công tác Sinh viên)
# ══════════════════════════════════════════════════

def extract_csv_ctsv(file_path: str = "data/sources/ctsv_hoc_ky.csv") -> pd.DataFrame:
    """
    Đọc file CSV từ Phòng Công tác Sinh viên.

    Format CSV (KHÁC PostgreSQL — Phòng CTSV tự quản lý):
        ma_sinh_vien  : join với dim_sinh_vien
        hoc_ky        : join với dim_hoc_ky (format: HK1-2021-22)
        diem_rl       : điểm rèn luyện 0-100
        xep_loai_rl   : Xuat sac / Tot / Kha / Trung binh / Yeu / Kem
        loai_hoc_bong : tên HB nếu có, rỗng nếu không
        muc_tien_hb   : số tiền HB (0 nếu không có)
        hinh_thuc_kl  : hình thức KL nếu có, rỗng nếu không
        ly_do_kl      : lý do KL nếu có

    Lý do là nguồn riêng:
        Phòng CTSV dùng phần mềm quản lý kỷ luật/khen thưởng riêng,
        không kết nối với hệ thống học vụ Phòng Đào tạo.
    """
    logger.info("=" * 55)
    logger.info("NGUON 2 - CSV (Phong Cong tac Sinh vien)")
    logger.info(f"  File: {file_path}")
    logger.info("=" * 55)

    try:
        df = pd.read_csv(file_path, encoding="utf-8")
        logger.info(f"  -> Doc duoc: {len(df):,} rows, {len(df.columns)} columns")

        # Validate cột bắt buộc
        required_cols = ["ma_sinh_vien", "hoc_ky", "diem_rl", "xep_loai_rl"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"CSV thieu cot bat buoc: {missing}")

        # Thống kê nhanh
        co_hb = (df["loai_hoc_bong"].fillna("") != "").sum()
        co_kl = (df["hinh_thuc_kl"].fillna("") != "").sum()
        logger.info(f"  -> Co hoc bong : {co_hb:,} records")
        logger.info(f"  -> Co ky luat  : {co_kl:,} records")

        return df

    except FileNotFoundError:
        logger.error(f"  -> Khong tim thay file: {file_path}")
        logger.error("     Chay: python scripts/generate_csv_api_sources.py")
        raise
    except Exception as e:
        logger.error(f"  -> Loi doc CSV: {e}")
        raise


# ══════════════════════════════════════════════════
# PHẦN 3 — REST API EXTRACTOR (Portal Tài chính)
# ══════════════════════════════════════════════════

def _check_api_health(base_url: str, timeout: int = 5) -> bool:
    """Kiểm tra API server còn sống. Trả về True nếu OK."""
    try:
        resp = requests.get(f"{base_url}/api/health", timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            logger.info(f"  -> API health: OK | records={data.get('records', '?')}")
            return True
        return False
    except requests.exceptions.ConnectionError:
        logger.error(f"  -> Khong ket noi duoc API: {base_url}")
        logger.error("     Chay: python scripts/mock_api_server.py")
        return False
    except Exception as e:
        logger.error(f"  -> API health check loi: {e}")
        return False


def _fetch_page(base_url: str, page: int, limit: int,
                hoc_ky: str = None, timeout: int = 30) -> dict:
    """Gọi 1 trang dữ liệu từ API /tai-chinh/hoc-phi."""
    params = {"page": page, "limit": limit}
    if hoc_ky:
        params["hoc_ky"] = hoc_ky

    resp = requests.get(
        f"{base_url}/api/tai-chinh/hoc-phi",
        params=params,
        timeout=timeout
    )
    resp.raise_for_status()
    return resp.json()


def extract_api_tai_chinh(
    base_url: str = PORTAL_API_BASE,
    hoc_ky: str = None,
    page_size: int = 500,
    max_retries: int = 3
) -> pd.DataFrame:
    """
    Extract toàn bộ dữ liệu tài chính từ Portal API.

    Xử lý tự động:
        - Health check trước khi extract
        - Pagination (500 records/trang)
        - Retry khi lỗi mạng (tối đa 3 lần, backoff 2s)
        - Gom tất cả trang thành 1 DataFrame

    Tham số:
        base_url   : URL API (mặc định localhost:5050)
        hoc_ky     : lọc theo HK cụ thể, None = lấy tất cả
        page_size  : số records/trang
        max_retries: số lần retry khi lỗi

    Lý do là nguồn riêng:
        Portal do vendor bên ngoài, trường không có quyền truy cập DB —
        chỉ gọi được REST API được cấp phép.

    Trả về DataFrame với cột:
        ma_sinh_vien, hoc_ky,
        hoc_phi_phai_dong, da_dong, con_no,
        duoc_mien_giam, ly_do_mien_giam, so_tien_mien_giam,
        ngay_dong_cuoi
    """
    logger.info("=" * 55)
    logger.info("NGUON 3 - REST API (Portal Tai chinh - vendor)")
    logger.info(f"  URL : {base_url}")
    logger.info(f"  Filter HK: {hoc_ky or 'Tat ca'}")
    logger.info("=" * 55)

    # Bước 1: Health check
    if not _check_api_health(base_url):
        raise ConnectionError(
            f"API khong kha dung: {base_url}\n"
            "Chay: python scripts/mock_api_server.py"
        )

    all_records = []
    page = 1

    # Bước 2: Pagination loop
    while True:
        for attempt in range(1, max_retries + 1):
            try:
                payload = _fetch_page(base_url, page, page_size, hoc_ky)
                break
            except requests.exceptions.Timeout:
                logger.warning(f"  -> Timeout trang {page}, thu lai {attempt}/{max_retries}")
                time.sleep(2 * attempt)
            except requests.exceptions.ConnectionError:
                logger.warning(f"  -> Mat ket noi trang {page}, thu lai {attempt}/{max_retries}")
                time.sleep(2 * attempt)
            except Exception as e:
                if attempt == max_retries:
                    logger.error(f"  -> Loi trang {page} sau {max_retries} lan: {e}")
                    raise
                time.sleep(2)

        records  = payload.get("data", [])
        all_records.extend(records)

        pagination = payload.get("pagination", {})
        total      = pagination.get("total", 0)
        has_next   = pagination.get("has_next", False)

        logger.info(f"  -> Trang {page}: {len(records)} records | Tong: {len(all_records)}/{total}")

        if not has_next:
            break
        page += 1

    if not all_records:
        logger.warning("  -> API tra ve 0 records")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    con_no    = (df["con_no"] > 0).sum() if "con_no" in df.columns else 0
    mien_giam = df["duoc_mien_giam"].sum() if "duoc_mien_giam" in df.columns else 0
    logger.info(f"  -> Extract xong: {len(df):,} records")
    logger.info(f"  -> Con no HP    : {con_no:,}")
    logger.info(f"  -> Duoc mien giam: {mien_giam:,}")

    return df


def extract_all_sources(
    csv_path: str = "data/sources/ctsv_hoc_ky.csv",
    api_base: str = PORTAL_API_BASE,
) -> dict:
    """
    Extract toàn bộ 3 nguồn trong 1 lần gọi.
    Dùng trong Airflow DAG task đầu tiên.

    Trả về:
        {
            "postgresql"    : { "sinh_vien": df, ... },  # 11 bảng
            "csv_ctsv"      : DataFrame,                 # Phòng CTSV
            "api_tai_chinh" : DataFrame,                 # Portal vendor
        }

    Lưu ý: nguồn 2 và 3 thất bại sẽ trả về DataFrame rỗng,
    KHÔNG làm hỏng toàn bộ pipeline (graceful degradation).
    """
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║  BAT DAU EXTRACT TU 3 NGUON              ║")
    logger.info("╚══════════════════════════════════════════╝")

    result = {}

    # Nguồn 1: PostgreSQL — bắt buộc phải thành công
    result["postgresql"] = extract_all_tables()

    # Nguồn 2: CSV CTSV — không ảnh hưởng pipeline nếu lỗi
    try:
        result["csv_ctsv"] = extract_csv_ctsv(csv_path)
    except Exception as e:
        logger.error(f"Nguon 2 (CSV CTSV) that bai: {e}")
        result["csv_ctsv"] = pd.DataFrame()

    # Nguồn 3: API Portal — không ảnh hưởng pipeline nếu lỗi
    try:
        result["api_tai_chinh"] = extract_api_tai_chinh(api_base)
    except Exception as e:
        logger.error(f"Nguon 3 (API Tai chinh) that bai: {e}")
        result["api_tai_chinh"] = pd.DataFrame()

    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║  EXTRACT HOAN THANH                      ║")
    logger.info(f"║  PostgreSQL   : {len(result['postgresql'])} bang")
    logger.info(f"║  CSV CTSV     : {len(result['csv_ctsv']):,} rows")
    logger.info(f"║  API Portal   : {len(result['api_tai_chinh']):,} rows")
    logger.info("╚══════════════════════════════════════════╝")

    return result


# ══════════════════════════════════════════════════
# PHẦN 4 — MINIO STAGING
# ══════════════════════════════════════════════════

def get_minio_client() -> Minio:
    """Tạo MinIO client dùng chung."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def upload_to_staging(df: pd.DataFrame, table_name: str) -> str:
    """
    Lưu DataFrame thành Parquet rồi upload lên MinIO staging.

    Path: raw/{table_name}/{YYYY-MM-DD}/data.parquet
    Ví dụ:
        raw/sinh_vien/2026-03-12/data.parquet
        raw/csv_ctsv/2026-03-12/data.parquet
        raw/api_tai_chinh/2026-03-12/data.parquet
    """
    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET_RAW):
        client.make_bucket(MINIO_BUCKET_RAW)
        logger.info(f"Da tao bucket: {MINIO_BUCKET_RAW}")

    today       = datetime.now().strftime("%Y-%m-%d")
    object_path = f"raw/{table_name}/{today}/data.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    size = buffer.getbuffer().nbytes

    client.put_object(
        bucket_name  = MINIO_BUCKET_RAW,
        object_name  = object_path,
        data         = buffer,
        length       = size,
        content_type = "application/octet-stream"
    )
    logger.info(f"  -> MinIO: {MINIO_BUCKET_RAW}/{object_path} ({size:,} bytes)")
    return object_path