"""
scripts/run_etl.py — Chạy pipeline ETL thủ công
=================================================
Dùng để:
  1. Test pipeline end-to-end trước khi cấu hình Airflow
  2. Chạy lại khi cần thiết (debug, reprocess)
  3. Kiểm tra MinIO staging hoạt động đúng

Cách dùng:
  # Full extract + transform + load
  python scripts/run_etl.py

  # Chỉ chạy 1 học kỳ cụ thể
  python scripts/run_etl.py --mode incremental --hoc-ky HK1-2024-25

  # Resume từ MinIO (không Extract lại, dùng staging có sẵn)
  python scripts/run_etl.py --mode resume

  # Resume từ run_id cụ thể
  python scripts/run_etl.py --mode resume --run-id 2024-01-15_02-00
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# ── Thêm project root vào sys.path để import src ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.etl.extract   import DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load      import DataLoader
from src.utils.logger  import get_logger

logger = get_logger("scripts.run_etl")


# ══════════════════════════════════════════════════
# CÁC MODE CHẠY
# ══════════════════════════════════════════════════

def run_full() -> dict:
    
    logger.info("=" * 70)
    logger.info("MODE: FULL — Extract + Transform + Load")
    logger.info("=" * 70)

    extractor   = DataExtractor()
    transformer = DataTransformer()
    loader      = DataLoader()

    # ── Bước 1: Extract ──
    logger.info("\n[BƯỚC 1/3] EXTRACT")
    extracted = extractor.extract_full()
    # → Sau bước này, MinIO bucket raw-data sẽ có folder mới
    # → extractor._last_run_id chứa run_id vừa lưu

    # ── Bước 2: Transform ──
    logger.info("\n[BƯỚC 2/3] TRANSFORM")
    transformed = transformer.transform_all(extracted)

    # ── Bước 3: Load ──
    logger.info("\n[BƯỚC 3/3] LOAD")
    stats = loader.load_all(transformed)

    return stats


def run_incremental(ma_hoc_ky: str) -> dict:
   
    logger.info("=" * 70)
    logger.info(f"MODE: INCREMENTAL — HK: {ma_hoc_ky}")
    logger.info("=" * 70)

    extractor   = DataExtractor()
    transformer = DataTransformer()
    loader      = DataLoader()

    logger.info("\n[BƯỚC 1/3] EXTRACT (incremental)")
    extracted = extractor.extract_incremental(ma_hoc_ky)

    logger.info("\n[BƯỚC 2/3] TRANSFORM")
    transformed = transformer.transform_all(extracted)

    logger.info("\n[BƯỚC 3/3] LOAD (incremental)")
    stats = loader.load_incremental(transformed, ma_hoc_ky)

    return stats


def run_resume(run_id: str = None) -> dict:
    
    logger.info("=" * 70)
    logger.info(f"MODE: RESUME — run_id={run_id or 'latest'}")
    logger.info("=" * 70)

    extractor   = DataExtractor()
    transformer = DataTransformer()
    loader      = DataLoader()

    # ── Bỏ qua Extract, đọc thẳng từ MinIO ──
    logger.info("\n[BƯỚC 1/3] LOAD FROM MINIO STAGING (bỏ qua Extract)")
    extracted = extractor.load_from_staging(run_id=run_id)
    # Nếu không tìm thấy staging → FileNotFoundError

    logger.info("\n[BƯỚC 2/3] TRANSFORM")
    transformed = transformer.transform_all(extracted)

    logger.info("\n[BƯỚC 3/3] LOAD")
    stats = loader.load_all(transformed)

    return stats


# ══════════════════════════════════════════════════
# IN KẾT QUẢ
# ══════════════════════════════════════════════════

def print_summary(stats: dict, start_time: datetime) -> None:
    
    duration = (datetime.now() - start_time).total_seconds()

    logger.info("\n" + "=" * 70)
    logger.info(" KẾT QUẢ PIPELINE")
    logger.info("=" * 70)
    logger.info(f"  Thời gian chạy: {duration:.1f} giây")
    logger.info(f"  Timestamp     : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    logger.info("  Records đã load vào warehouse:")

    total = 0
    for table, count in stats.items():
        logger.info(f"    {table:<30s}: {count:>8,}")
        total += count

    logger.info(f"    {'TỔNG':<30s}: {total:>8,}")
    logger.info("=" * 70)
    logger.info("==PIPELINE HOÀN TẤT==")
    logger.info("")
    logger.info("  Kiểm tra kết quả:")
    logger.info("  → MinIO Console  : http://localhost:9001")
    logger.info("    Bucket raw-data: xem file .parquet vừa upload")
    logger.info("  → pgAdmin        : http://localhost:5050")
    logger.info("    DB warehouse   : SELECT COUNT(*) FROM dim_sinh_vien;")
    logger.info("=" * 70)


# ══════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Chạy ETL pipeline thủ công",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python scripts/run_etl.py
  python scripts/run_etl.py --mode incremental --hoc-ky HK1-2024-25
  python scripts/run_etl.py --mode resume
  python scripts/run_etl.py --mode resume --run-id 2024-01-15_02-00
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "resume"],
        default="full",
        help="Chế độ chạy (default: full)",
    )
    parser.add_argument(
        "--hoc-ky",
        default=None,
        help="Mã học kỳ cho mode incremental, VD: HK1-2024-25",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="run_id MinIO cho mode resume, VD: 2024-01-15_02-00",
    )
    return parser.parse_args()


def main():
    args       = parse_args()
    start_time = datetime.now()

    logger.info(f" Bắt đầu lúc: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        if args.mode == "full":
            stats = run_full()

        elif args.mode == "incremental":
            if not args.hoc_ky:
                logger.error("Mode incremental cần --hoc-ky. VD: --hoc-ky HK1-2024-25")
                sys.exit(1)
            stats = run_incremental(args.hoc_ky)

        elif args.mode == "resume":
            stats = run_resume(run_id=args.run_id)

        print_summary(stats, start_time)

    except FileNotFoundError as e:
        # load_from_staging() không tìm thấy staging data
        logger.error(f"Không tìm thấy staging: {e}")
        
        sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Pipeline bị dừng thủ công (Ctrl+C)")
        sys.exit(0)

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Pipeline thất bại sau {duration:.1f}s: {e}")
     
        raise


if __name__ == "__main__":
    main()