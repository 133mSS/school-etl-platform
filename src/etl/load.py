from src.etl.aggregation import DataAggregator
from typing import Dict, List, Optional, Tuple
from datetime import date
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pandas as pd
import numpy as np
from sqlalchemy import text
from typing import Generator, List
from src.config.database import warehouse_engine, WarehouseSession
from src.config.settings import ETL_BATCH_SIZE
from src.models.warehouse_models import (
    WarehouseBase,
    DimSinhVien, DimHocPhan, DimGiangVien, DimHocKy,
    FactHocTap, FactDangKy, FactCtsv, FactTaiChinh,
    AggStudentSummary,
)
from src.etl.transform import TransformedData
from src.utils.logger import get_logger

logger = get_logger("etl.load")


class DataLoader:
    def __init__(self):
        self.engine     = warehouse_engine
        self.batch_size = ETL_BATCH_SIZE

        self._sv_key_cache: Dict[str, int] = {}
        self._hp_key_cache: Dict[str, int] = {}
        self._gv_key_cache: Dict[str, int] = {}
        self._hk_key_cache: Dict[str, int] = {}
        self._hp_tc_cache:  Dict[int, int]  = {}
    def _chunked_with_size(self, lst: list, size: int):
        """
        Generator chia list thành các batch với kích thước tùy chỉnh.
        
        Khác với _chunked() dùng self.batch_size cố định,
        hàm này cho phép caller chỉ định size linh hoạt.
        
        Ví dụ:
            list(self._chunked_with_size([1,2,3,4,5], 2))
            → [[1,2], [3,4], [5]]
        """
        for i in range(0, len(lst), size):
            yield lst[i : i + size]
    # ────────────────────────────────────────────────────────────────
    # LOAD ALL
    # ────────────────────────────────────────────────────────────────

    def load_all(self, data: TransformedData) -> dict:
        logger.info("==BẮT ĐẦU LOAD VÀO DATA WAREHOUSE==")
        logger.info("=" * 70)

        stats = {}
        self._ensure_schema()

        logger.info("── Bước 1: Load Dimension Tables ──")
        stats["dim_hoc_ky"]     = self._load_dim_hoc_ky(data.dim_thoi_gian)
        stats["dim_giang_vien"] = self._load_dim_giang_vien(data.dim_giang_vien)
        stats["dim_hoc_phan"]   = self._load_dim_hoc_phan(data.dim_hoc_phan)
        stats["dim_sinh_vien"]  = self._load_dim_sinh_vien(data.dim_sinh_vien)

        logger.info("── Bước 2: Build Surrogate Key Cache ──")
        self._build_key_caches()

        logger.info("── Bước 3: Load Fact Tables ──")
        stats["fact_hoc_tap"]   = self._load_fact_hoc_tap(data.fact_diem)
        # FIX: Thêm load fact_dang_ky (trước đây bị bỏ qua hoàn toàn)
        stats["fact_dang_ky"]   = self._load_fact_dang_ky(data.fact_dang_ky)
        stats["fact_ctsv"]      = self._load_fact_ctsv(data.fact_ren_luyen)
        stats["fact_tai_chinh"] = self._load_fact_tai_chinh(data.fact_tai_chinh)

        logger.info("── Bước 4: Build Aggregation ──")
        aggregator = DataAggregator()
        stats["agg_student_summary"] = aggregator.run_all()

        logger.info("=" * 70)
        logger.info("LOAD HOÀN TẤT")
        total = 0
        for name, count in stats.items():
            logger.info(f"  {name:<25s}: {count:>8,} records")
            total += count
        logger.info(f"  {'TỔNG':<25s}: {total:>8,} records")
        logger.info("=" * 70)

        return stats

    def load_incremental(self, data: TransformedData, ma_hoc_ky: str) -> dict:
        logger.info(f"INCREMENTAL LOAD — Học kỳ: {ma_hoc_ky}")

        stats = {}
        self._ensure_schema()

        stats["dim_hoc_ky"]     = self._load_dim_hoc_ky(data.dim_thoi_gian)
        stats["dim_giang_vien"] = self._load_dim_giang_vien(data.dim_giang_vien)
        stats["dim_hoc_phan"]   = self._load_dim_hoc_phan(data.dim_hoc_phan)
        stats["dim_sinh_vien"]  = self._load_dim_sinh_vien(data.dim_sinh_vien)

        self._build_key_caches()

        hk_key = self._hk_key_cache.get(ma_hoc_ky)
        if hk_key:
            self._delete_fact_by_hk(hk_key, ma_hoc_ky)

        stats["fact_hoc_tap"]        = self._load_fact_hoc_tap(data.fact_diem)
        stats["fact_dang_ky"]        = self._load_fact_dang_ky(data.fact_dang_ky)  # FIX
        stats["fact_ctsv"]           = self._load_fact_ctsv(data.fact_ren_luyen)
        stats["fact_tai_chinh"]      = self._load_fact_tai_chinh(data.fact_tai_chinh)

        aggregator = DataAggregator()
        stats["agg_student_summary"] = aggregator.run_all()

        logger.info(f"INCREMENTAL LOAD HOÀN TẤT — {ma_hoc_ky}")
        return stats

    # ────────────────────────────────────────────────────────────────
    # INTERNAL HELPERS
    # ────────────────────────────────────────────────────────────────

    def _ensure_schema(self):
        logger.info("Tạo schema warehouse từ warehouse_models.py...")
        WarehouseBase.metadata.create_all(self.engine)
        logger.info("  Schema warehouse OK")

    def _build_key_caches(self):
        logger.info("Building surrogate key caches...")

        df = pd.read_sql(
            "SELECT sinh_vien_key, ma_sinh_vien FROM dim_sinh_vien "
            "WHERE la_ban_hien_tai = TRUE",
            self.engine,
        )
        self._sv_key_cache = dict(zip(df["ma_sinh_vien"], df["sinh_vien_key"]))

        df = pd.read_sql(
            "SELECT hoc_phan_key, ma_hoc_phan, so_tin_chi FROM dim_hoc_phan",
            self.engine,
        )
        self._hp_key_cache = dict(zip(df["ma_hoc_phan"], df["hoc_phan_key"]))
        self._hp_tc_cache  = dict(zip(df["hoc_phan_key"], df["so_tin_chi"]))

        df = pd.read_sql(
            "SELECT giang_vien_key, ma_giang_vien FROM dim_giang_vien",
            self.engine,
        )
        self._gv_key_cache = dict(zip(df["ma_giang_vien"], df["giang_vien_key"]))

        df = pd.read_sql(
            "SELECT hoc_ky_key, ma_hoc_ky FROM dim_hoc_ky",
            self.engine,
        )
        self._hk_key_cache = dict(zip(df["ma_hoc_ky"], df["hoc_ky_key"]))

        logger.info(
            f"  SV={len(self._sv_key_cache)}, "
            f"HP={len(self._hp_key_cache)}, "
            f"GV={len(self._gv_key_cache)}, "
            f"HK={len(self._hk_key_cache)}"
        )

    def _lookup_sv_key(self, ma_sv: str) -> Optional[int]:
        return self._sv_key_cache.get(ma_sv)

    def _lookup_hp_key(self, ma_hp: str) -> Optional[int]:
        return self._hp_key_cache.get(ma_hp)

    def _lookup_gv_key(self, ma_gv: str) -> Optional[int]:
        return self._gv_key_cache.get(ma_gv) if ma_gv else None

    def _lookup_hk_key(self, ma_hk: str) -> Optional[int]:
        return self._hk_key_cache.get(ma_hk)

    # ────────────────────────────────────────────────────────────────
    # DIMENSION LOADERS
    # ────────────────────────────────────────────────────────────────

    def _load_dim_hoc_ky(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        session = WarehouseSession()
        count = 0
        try:
            for _, row in df.iterrows():
                ma_hk = row.get("ma_hoc_ky")
                if not ma_hk:
                    continue

                existing = session.query(DimHocKy).filter_by(ma_hoc_ky=ma_hk).first()

                if existing:
                    existing.nam_hoc       = row.get("nam_hoc", existing.nam_hoc)
                    existing.hoc_ky        = row.get("hoc_ky", existing.hoc_ky)
                    existing.ngay_bat_dau  = self._to_date(row.get("ngay_bat_dau"))
                    existing.ngay_ket_thuc = self._to_date(row.get("ngay_ket_thuc"))
                    existing.nam_bat_dau   = row.get("nam_bat_dau")
                    existing.nam_ket_thuc  = row.get("nam_ket_thuc")
                else:
                    obj = DimHocKy(
                        ma_hoc_ky     = ma_hk,
                        nam_hoc       = row.get("nam_hoc", ""),
                        hoc_ky        = row.get("hoc_ky", ""),
                        ngay_bat_dau  = self._to_date(row.get("ngay_bat_dau")),
                        ngay_ket_thuc = self._to_date(row.get("ngay_ket_thuc")),
                        nam_bat_dau   = row.get("nam_bat_dau"),
                        nam_ket_thuc  = row.get("nam_ket_thuc"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_hoc_ky     → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_hoc_ky | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_giang_vien(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        session = WarehouseSession()
        count = 0
        try:
            for _, row in df.iterrows():
                ma_gv = row.get("ma_giang_vien")
                if not ma_gv:
                    continue

                existing = session.query(DimGiangVien).filter_by(
                    ma_giang_vien=ma_gv
                ).first()

                if existing:
                    existing.ho_ten              = row.get("ho_ten", existing.ho_ten)
                    existing.chuc_danh           = row.get("chuc_danh", existing.chuc_danh)
                    existing.trang_thai_cong_tac = row.get(
                        "trang_thai_cong_tac", existing.trang_thai_cong_tac
                    )
                    existing.ma_khoa  = row.get("ma_khoa",  existing.ma_khoa)
                    existing.ten_khoa = row.get("ten_khoa", existing.ten_khoa)
                else:
                    obj = DimGiangVien(
                        ma_giang_vien       = ma_gv,
                        ho                  = row.get("ho", ""),
                        ten                 = row.get("ten", ""),
                        ho_ten              = row.get("ho_ten", ""),
                        email               = row.get("email"),
                        chuc_danh           = row.get("chuc_danh"),
                        trang_thai_cong_tac = row.get("trang_thai_cong_tac"),
                        ma_khoa             = row.get("ma_khoa"),
                        ten_khoa            = row.get("ten_khoa"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_giang_vien → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_giang_vien | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_hoc_phan(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        session = WarehouseSession()
        count = 0
        try:
            for _, row in df.iterrows():
                ma_hp = row.get("ma_hoc_phan")
                if not ma_hp:
                    continue

                existing = session.query(DimHocPhan).filter_by(
                    ma_hoc_phan=ma_hp
                ).first()

                if existing:
                    existing.ten_mon    = row.get("ten_mon",    existing.ten_mon)
                    existing.so_tin_chi = row.get("so_tin_chi", existing.so_tin_chi)
                    existing.ma_khoa    = row.get("ma_khoa",    existing.ma_khoa)
                    existing.ten_khoa   = row.get("ten_khoa",   existing.ten_khoa)
                else:
                    obj = DimHocPhan(
                        ma_hoc_phan      = ma_hp,
                        ma_mon           = row.get("ma_mon"),
                        ten_mon          = row.get("ten_mon", ""),
                        so_tin_chi       = row.get("so_tin_chi"),
                        so_gio_ly_thuyet = row.get("so_gio_ly_thuyet"),
                        so_gio_thuc_hanh = row.get("so_gio_thuc_hanh"),
                        hoc_ky_de_xuat   = row.get("hoc_ky_de_xuat"),
                        bat_buoc         = row.get("bat_buoc"),
                        ma_khoa          = row.get("ma_khoa"),
                        ten_khoa         = row.get("ten_khoa"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_hoc_phan   → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_hoc_phan | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_sinh_vien(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        session      = WarehouseSession()
        inserted     = 0
        updated      = 0
        cascaded_facts = 0

        scd2_cols = ["trang_thai_hoc_tap", "ma_nganh", "ma_lop", "ma_khoa"]

        # FIX: Tính today một lần trước vòng lặp thay vì trong mỗi iteration
        today = date.today()

        try:
            for _, row in df.iterrows():
                ma_sv = row.get("ma_sinh_vien")
                if not ma_sv:
                    continue

                current = session.query(DimSinhVien).filter(
                    DimSinhVien.ma_sinh_vien == ma_sv,
                    DimSinhVien.la_ban_hien_tai == True,
                ).first()

                if current is None:
                    obj = DimSinhVien(
                        ma_sinh_vien       = ma_sv,
                        ho                 = row.get("ho"),
                        ten                = row.get("ten"),
                        ho_ten             = row.get("ho_ten", ""),
                        ngay_sinh          = self._to_date(row.get("ngay_sinh")),
                        gioi_tinh          = row.get("gioi_tinh"),
                        email              = row.get("email"),
                        khoa_hoc           = row.get("khoa_hoc"),
                        trang_thai_hoc_tap = row.get("trang_thai_hoc_tap"),
                        ma_nganh           = row.get("ma_nganh"),
                        ten_nganh          = row.get("ten_nganh"),
                        ma_khoa            = row.get("ma_khoa"),
                        ten_khoa           = row.get("ten_khoa"),
                        ma_co_van          = row.get("ma_co_van"),
                        ten_co_van         = row.get("ten_co_van"),
                        ma_lop             = row.get("ma_lop"),
                        ten_lop            = row.get("ten_lop"),
                        la_ban_hien_tai    = True,
                        phien_ban          = 1,
                        ngay_hieu_luc      = today,
                        ngay_het_hieu_luc  = None,
                    )
                    session.add(obj)
                    inserted += 1
                else:
                    changed = self._check_scd2_changed(current, row, scd2_cols)

                    if changed:
                        old_key = current.sinh_vien_key
                        current.la_ban_hien_tai   = False
                        current.ngay_het_hieu_luc = today

                        new_obj = DimSinhVien(
                            ma_sinh_vien       = ma_sv,
                            ho                 = row.get("ho"),
                            ten                = row.get("ten"),
                            ho_ten             = row.get("ho_ten", ""),
                            ngay_sinh          = self._to_date(row.get("ngay_sinh")),
                            gioi_tinh          = row.get("gioi_tinh"),
                            email              = row.get("email"),
                            khoa_hoc           = row.get("khoa_hoc"),
                            trang_thai_hoc_tap = row.get("trang_thai_hoc_tap"),
                            ma_nganh           = row.get("ma_nganh"),
                            ten_nganh          = row.get("ten_nganh"),
                            ma_khoa            = row.get("ma_khoa"),
                            ten_khoa           = row.get("ten_khoa"),
                            ma_co_van          = row.get("ma_co_van"),
                            ten_co_van         = row.get("ten_co_van"),
                            ma_lop             = row.get("ma_lop"),
                            ten_lop            = row.get("ten_lop"),
                            la_ban_hien_tai    = True,
                            phien_ban          = current.phien_ban + 1,
                            ngay_hieu_luc      = today,
                            ngay_het_hieu_luc  = None,
                        )
                        session.add(new_obj)
                        session.flush()

                        new_key = new_obj.sinh_vien_key
                        n = self._cascade_sv_key_update(session, old_key, new_key)
                        cascaded_facts += n
                        updated += 1
                    else:
                        current.ho_ten     = row.get("ho_ten",     current.ho_ten)
                        current.email      = row.get("email",      current.email)
                        current.ten_nganh  = row.get("ten_nganh",  current.ten_nganh)
                        current.ten_khoa   = row.get("ten_khoa",   current.ten_khoa)
                        current.ten_lop    = row.get("ten_lop",    current.ten_lop)
                        current.ten_co_van = row.get("ten_co_van", current.ten_co_van)
                        current.ma_co_van  = row.get("ma_co_van",  current.ma_co_van)

            session.commit()
            logger.info(
                f"  dim_sinh_vien → {len(df):>6,} records "
                f"(new: {inserted}, SCD2: {updated}, "
                f"fact cascade: {cascaded_facts})"
            )
            return inserted + updated

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_sinh_vien | Lỗi: {e}")
            return 0
        finally:
            session.close()

    # ────────────────────────────────────────────────────────────────
    # SCD TYPE 2 HELPERS
    # ────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_scd2_val(val):
        if val is None:
            return None
        if isinstance(val, float) and np.isnan(val):
            return None
        s = str(val).strip()
        if s == "" or s.lower() in ("none", "nan"):
            return None
        return s

    def _check_scd2_changed(self, current, row, scd2_cols) -> bool:
        for col in scd2_cols:
            old_val = self._normalize_scd2_val(getattr(current, col, None))
            new_val = self._normalize_scd2_val(row.get(col))
            if new_val is not None and old_val != new_val:
                return True
        return False

    def _cascade_sv_key_update(
        self, session, old_key: int, new_key: int
    ) -> int:
        total = 0
        fact_models = [
            FactHocTap,
            FactDangKy,
            FactCtsv,
            FactTaiChinh,
            AggStudentSummary,
        ]
        for Model in fact_models:
            try:
                n = (
                    session.query(Model)
                    .filter(Model.sinh_vien_key == old_key)
                    .update(
                        {Model.sinh_vien_key: new_key},
                        synchronize_session=False,
                    )
                )
                total += n
            except Exception:
                pass
        return total

    # ────────────────────────────────────────────────────────────────
    # FACT LOADERS
    # ────────────────────────────────────────────────────────────────

    def _load_fact_hoc_tap(self, df: pd.DataFrame) -> int:
        if df.empty:
            logger.info("  fact_hoc_tap   → SKIP (empty)")
            return 0

        records = []
        for _, row in df.iterrows():
            ma_sv = row.get("ma_sinh_vien")
            ma_hp = row.get("ma_hoc_phan")
            ma_hk = row.get("ma_hoc_ky")
            ma_gv = row.get("ma_giang_vien")

            # Ép kiểu để tránh type warning
            if not all([ma_sv, ma_hp, ma_hk]):
                continue
            ma_sv = str(ma_sv)
            ma_hp = str(ma_hp)
            ma_hk = str(ma_hk)
            ma_gv = str(ma_gv) if ma_gv else None

            sv_key = self._lookup_sv_key(ma_sv)
            hp_key = self._lookup_hp_key(ma_hp)
            hk_key = self._lookup_hk_key(ma_hk)
            gv_key = self._lookup_gv_key(ma_gv) if ma_gv else None

            if not all([sv_key, hp_key, hk_key]):
                continue

            so_tc    = self._hp_tc_cache.get(hp_key)  # type: ignore[arg-type]
            diem_he4 = self._to_decimal(row.get("diem_he_4"))
            diem_cl  = (float(diem_he4) * so_tc) if diem_he4 and so_tc else None

            records.append({
                "sinh_vien_key"  : sv_key,
                "hoc_phan_key"   : hp_key,
                "giang_vien_key" : gv_key,
                "hoc_ky_key"     : hk_key,
                "ma_sinh_vien"   : ma_sv,
                "ma_hoc_phan"    : ma_hp,
                "ma_dang_ky"     : row.get("ma_dang_ky"),
                "diem_chuyen_can": self._to_decimal(row.get("diem_chuyen_can")),
                "diem_bai_tap"   : self._to_decimal(row.get("diem_bai_tap")),
                "diem_giua_ky"   : self._to_decimal(row.get("diem_giua_ky")),
                "diem_cuoi_ky"   : self._to_decimal(row.get("diem_cuoi_ky")),
                "diem_tong_ket"  : self._to_decimal(row.get("diem_tong_ket")),
                "diem_chu"       : row.get("diem_chu"),
                "diem_he_4"      : diem_he4,
                "dat_mon"        : row.get("dat_mon"),
                "hoc_lai"        : row.get("hoc_lai"),
                "so_tin_chi"     : so_tc,
                "diem_chat_luong": diem_cl,
                "nguon_du_lieu"  : "postgresql",
            })

        if not records:
            logger.info("  fact_hoc_tap   → SKIP (no valid records)")
            return 0

        try:
            total = 0
            for batch in self._chunked(records):
                stmt = pg_insert(FactHocTap).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["ma_sinh_vien", "ma_hoc_phan", "hoc_ky_key"],
                    set_={
                        "giang_vien_key" : stmt.excluded.giang_vien_key,
                        "diem_chuyen_can": stmt.excluded.diem_chuyen_can,
                        "diem_bai_tap"   : stmt.excluded.diem_bai_tap,
                        "diem_giua_ky"   : stmt.excluded.diem_giua_ky,
                        "diem_cuoi_ky"   : stmt.excluded.diem_cuoi_ky,
                        "diem_tong_ket"  : stmt.excluded.diem_tong_ket,
                        "diem_chu"       : stmt.excluded.diem_chu,
                        "diem_he_4"      : stmt.excluded.diem_he_4,
                        "dat_mon"        : stmt.excluded.dat_mon,
                        "hoc_lai"        : stmt.excluded.hoc_lai,
                        "so_tin_chi"     : stmt.excluded.so_tin_chi,
                        "diem_chat_luong": stmt.excluded.diem_chat_luong,
                    },
                )
                with self.engine.begin() as conn:
                    conn.execute(stmt)
                total += len(batch)

            logger.info(f"  fact_hoc_tap   → {total:>6,} records (upserted)")
            return total

        except Exception as e:
            logger.error(f"  fact_hoc_tap | Lỗi: {e}")
            return 0

    def _load_fact_dang_ky(self, df: pd.DataFrame) -> int:
    
        if df.empty:
            logger.info("  fact_dang_ky   → SKIP (empty)")
            return 0

        records = []
        for _, row in df.iterrows():
            ma_sv = row.get("ma_sinh_vien")
            ma_hp = row.get("ma_hoc_phan")
            ma_hk = row.get("ma_hoc_ky")
            ma_gv = row.get("ma_giang_vien")

            if not all([ma_sv, ma_hp, ma_hk]):
                continue
            ma_sv = str(ma_sv)
            ma_hp = str(ma_hp)
            ma_hk = str(ma_hk)
            ma_gv = str(ma_gv) if ma_gv else None

            sv_key = self._lookup_sv_key(ma_sv)
            hp_key = self._lookup_hp_key(ma_hp)
            hk_key = self._lookup_hk_key(ma_hk)
            gv_key = self._lookup_gv_key(ma_gv) if ma_gv else None

            if not all([sv_key, hp_key, hk_key]):
                continue

            so_tc = self._hp_tc_cache.get(hp_key)  # type: ignore[arg-type]

            records.append({
                "sinh_vien_key" : sv_key,
                "hoc_phan_key"  : hp_key,
                "giang_vien_key": gv_key,
                "hoc_ky_key"    : hk_key,
                "ma_sinh_vien"  : ma_sv,
                "ma_hoc_phan"   : ma_hp,
                "ma_dang_ky"    : self._to_int(row.get("ma_dang_ky")),
                "trang_thai"    : row.get("trang_thai", "Đã đăng ký"),
                "so_tin_chi"    : so_tc,
                "ngay_dang_ky"  : self._to_date(row.get("ngay_dang_ky")),
                "nguon_du_lieu" : "postgresql",
            })

        if not records:
            logger.info("  fact_dang_ky   → SKIP (no valid records)")
            return 0

        try:
            total = 0
            for batch in self._chunked(records):
                stmt = pg_insert(FactDangKy).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["ma_sinh_vien", "ma_hoc_phan", "hoc_ky_key"],
                    set_={
                        "trang_thai"    : stmt.excluded.trang_thai,
                        "ngay_dang_ky"  : stmt.excluded.ngay_dang_ky,
                        "giang_vien_key": stmt.excluded.giang_vien_key,
                        "so_tin_chi"    : stmt.excluded.so_tin_chi,
                    },
                )
                with self.engine.begin() as conn:
                    conn.execute(stmt)
                total += len(batch)

            logger.info(f"  fact_dang_ky   → {total:>6,} records (upserted)")
            return total

        except Exception as e:
            logger.error(f"  fact_dang_ky | Lỗi: {e}")
            return 0

    def _load_fact_ctsv(self, df: pd.DataFrame) -> int:
        """
        Load dữ liệu rèn luyện/học bổng/kỷ luật vào fact_ctsv.

        FIX CardinalityViolation: Thêm deduplication tại 2 tầng:
        - Tầng 1: Dedup trên DataFrame đầu vào (trước khi build records)
        - Tầng 2: Dedup trên list records (trước khi chia batch)
        Mục đích: Đảm bảo không có 2 record cùng (ma_sinh_vien, hoc_ky_key)
        trong bất kỳ batch nào gửi lên PostgreSQL.

        OPTIMIZED: Thay iterrows() bằng vectorized pandas operations →
        nhanh gấp 10-50× cho data lớn. Logic nghiệp vụ giữ nguyên 100%.
        """
        if df.empty:
            logger.info("  fact_ctsv      → SKIP (empty)")
            return 0

        expected_cols = [
            "ma_sinh_vien", "hoc_ky", "diem_ren_luyen",
            "xep_loai_rl", "loai_hoc_bong", "muc_tien_hb",
            "hinh_thuc_ky_luat", "ly_do_ky_luat",
        ]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            logger.warning(f"  fact_ctsv | Thiếu cột: {missing}")

        # ══════════════════════════════════════════════════════════
        # TẦNG 1: DEDUP TRÊN DATAFRAME ĐẦU VÀO  (giữ nguyên)
        # ══════════════════════════════════════════════════════════
        rows_before_dedup = len(df)
        df = df.drop_duplicates(
            subset=["ma_sinh_vien", "hoc_ky"],
            keep="last"
        ).copy()  # .copy() để tránh SettingWithCopyWarning khi assign cột mới
        rows_after_dedup = len(df)
        df_dupes_removed = rows_before_dedup - rows_after_dedup

        if df_dupes_removed > 0:
            logger.warning(
                f"  fact_ctsv | [DEDUP Tầng 1 - DataFrame] "
                f"Loại bỏ {df_dupes_removed} bản ghi trùng "
                f"({rows_before_dedup} → {rows_after_dedup})"
            )

        # ══════════════════════════════════════════════════════════
        # BUILD RECORDS LIST — VECTORIZED
        # ══════════════════════════════════════════════════════════
        # Thay iterrows() bằng các thao tác cột-vector, nhanh hơn 10-50×.
        # Vẫn xử lý đầy đủ: lookup SK, validate diem_rl, NULL handling.

        rows_input = len(df)

        # ── Bước 1: Filter ma_sinh_vien / hoc_ky NULL hoặc rỗng ──
        df = df.dropna(subset=["ma_sinh_vien", "hoc_ky"])
        df = df[(df["ma_sinh_vien"].astype(str) != "") &
                (df["hoc_ky"].astype(str) != "")]

        # Ép str để dùng cho lookup và DB
        df["ma_sinh_vien"] = df["ma_sinh_vien"].astype(str)
        df["hoc_ky"]       = df["hoc_ky"].astype(str)

        # ── Bước 2: Lookup surrogate key bằng vectorized .map() ──
        # (cache là dict → .map() rất nhanh, O(n))
        df["sinh_vien_key"] = df["ma_sinh_vien"].map(self._sv_key_cache)
        df["hoc_ky_key"]    = df["hoc_ky"].map(self._hk_key_cache)

        # Loại bỏ row không lookup được (sv hoặc hk không có trong cache)
        before_sk_filter = len(df)
        df = df.dropna(subset=["sinh_vien_key", "hoc_ky_key"])
        sk_lookup_failed = before_sk_filter - len(df)
        if sk_lookup_failed > 0:
            logger.warning(
                f"  fact_ctsv | Bỏ qua {sk_lookup_failed} rows: "
                f"không tìm thấy surrogate key (SV hoặc HK chưa có trong dim)"
            )

        # Ép int sau khi đã loại NULL
        df["sinh_vien_key"] = df["sinh_vien_key"].astype(int)
        df["hoc_ky_key"]    = df["hoc_ky_key"].astype(int)

        # ── Bước 3: Validate diem_ren_luyen ──
        # Convert sang numeric: chữ → NaN, NULL gốc → NaN
        raw_drl = df["diem_ren_luyen"] if "diem_ren_luyen" in df.columns else pd.Series(dtype=object)
        drl_numeric = pd.to_numeric(raw_drl, errors="coerce")

        # Detect "có giá trị nhưng không phải số" để log warning
        # (giống như iterrows cũ: raw_diem_rl is not None and diem_rl_val is None)
        not_null_mask = raw_drl.notna() & (raw_drl.astype(str).str.strip() != "")
        cannot_parse_mask = not_null_mask & drl_numeric.isna()
        skipped = int(cannot_parse_mask.sum())

        if skipped > 0:
            # Log tối đa 5 ví dụ — giống logic cũ
            examples = raw_drl[cannot_parse_mask].head(5).tolist()
            for ex in examples:
                logger.warning(
                    f"  fact_ctsv | diem_ren_luyen='{ex}' không phải số. Skip."
                )
            # Loại bỏ những row có giá trị invalid khỏi df
            df = df[~cannot_parse_mask]
            drl_numeric = drl_numeric[~cannot_parse_mask]

        df["diem_rl"] = drl_numeric.astype("Int64")  # nullable int

        # ── Bước 4: Convert các cột text — NULL-safe ──
        # Helper: trả về str hoặc None nếu NaN/empty
        def _safe_str_col(col_name: str) -> pd.Series:
            if col_name not in df.columns:
                return pd.Series([None] * len(df), index=df.index)
            s = df[col_name]
            result = s.where(s.notna() & (s.astype(str) != ""), None)
            return result.apply(lambda x: str(x) if x is not None else None)

        df["xep_loai_rl_val"]  = _safe_str_col("xep_loai_rl")
        df["loai_hb_val"]      = _safe_str_col("loai_hoc_bong")
        df["hinh_thuc_val"]    = _safe_str_col("hinh_thuc_ky_luat")
        df["ly_do_val"]        = _safe_str_col("ly_do_ky_luat")

        # ── Bước 5: muc_tien_hb — convert int, default 0 ──
        if "muc_tien_hb" in df.columns:
            df["muc_tien_val"] = (
                pd.to_numeric(df["muc_tien_hb"], errors="coerce")
                .fillna(0)
                .astype(int)
            )
        else:
            df["muc_tien_val"] = 0

        # ── Bước 6: Boolean flags ──
        df["co_hoc_bong"] = df["loai_hb_val"].notna()
        df["bi_ky_luat"]  = df["hinh_thuc_val"].notna()

        # ── Bước 7: Build records list — to_dict('records') nhanh hơn iterrows ──
        df_out = pd.DataFrame({
            "sinh_vien_key": df["sinh_vien_key"],
            "hoc_ky_key"   : df["hoc_ky_key"],
            "ma_sinh_vien" : df["ma_sinh_vien"],
            "ma_hoc_ky"    : df["hoc_ky"],
            "diem_rl"      : df["diem_rl"],
            "xep_loai_rl"  : df["xep_loai_rl_val"],
            "loai_hoc_bong": df["loai_hb_val"],
            "muc_tien_hb"  : df["muc_tien_val"],
            "hinh_thuc_kl" : df["hinh_thuc_val"],
            "ly_do_kl"     : df["ly_do_val"],
            "co_hoc_bong"  : df["co_hoc_bong"],
            "bi_ky_luat"   : df["bi_ky_luat"],
            "nguon_du_lieu": "csv_ctsv",
        })

        # Convert NaN → None để psycopg2 không gửi 'NaN' string lên Postgres
        df_out = df_out.astype(object).where(df_out.notna(), None)
        records = df_out.to_dict("records")

        if not records:
            logger.info("  fact_ctsv      → SKIP (no valid records)")
            return 0

        logger.info(
            f"  fact_ctsv | Vectorize OK: {rows_input} input → {len(records)} valid records"
        )

        # ══════════════════════════════════════════════════════════
        # TẦNG 2: DEDUP TRÊN RECORDS LIST  (giữ nguyên)
        # ══════════════════════════════════════════════════════════
        records_before = len(records)
        seen_keys = {}

        for rec in records:
            composite_key = (rec["ma_sinh_vien"], rec["hoc_ky_key"])
            seen_keys[composite_key] = rec

        deduped_records = list(seen_keys.values())
        records_after = len(deduped_records)
        list_dupes_removed = records_before - records_after

        if list_dupes_removed > 0:
            logger.warning(
                f"  fact_ctsv | [DEDUP Tầng 2 - Records List] "
                f"Loại bỏ thêm {list_dupes_removed} bản ghi trùng surrogate key "
                f"({records_before} → {records_after})"
            )

        # ══════════════════════════════════════════════════════════
        # BATCH UPSERT  (giữ nguyên)
        # ══════════════════════════════════════════════════════════
        effective_batch_size = max(self.batch_size, 5000)

        try:
            total = 0
            total_batches = (len(deduped_records) + effective_batch_size - 1) // effective_batch_size

            for batch_num, batch in enumerate(
                self._chunked_with_size(deduped_records, effective_batch_size), 1
            ):
                # Paranoid check (giữ nguyên)
                batch_keys = [(r["ma_sinh_vien"], r["hoc_ky_key"]) for r in batch]
                if len(batch_keys) != len(set(batch_keys)):
                    logger.error(
                        f"  fact_ctsv | Batch {batch_num}: vẫn còn duplicate! "
                        f"Đây là bug nghiêm trọng, kiểm tra lại logic transform."
                    )
                    seen = {}
                    for r in batch:
                        seen[(r["ma_sinh_vien"], r["hoc_ky_key"])] = r
                    batch = list(seen.values())

                stmt = pg_insert(FactCtsv).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["ma_sinh_vien", "hoc_ky_key"],
                    set_={
                        "diem_rl"      : stmt.excluded.diem_rl,
                        "xep_loai_rl"  : stmt.excluded.xep_loai_rl,
                        "loai_hoc_bong": stmt.excluded.loai_hoc_bong,
                        "muc_tien_hb"  : stmt.excluded.muc_tien_hb,
                        "hinh_thuc_kl" : stmt.excluded.hinh_thuc_kl,
                        "ly_do_kl"     : stmt.excluded.ly_do_kl,
                        "co_hoc_bong"  : stmt.excluded.co_hoc_bong,
                        "bi_ky_luat"   : stmt.excluded.bi_ky_luat,
                    },
                )
                with self.engine.begin() as conn:
                    conn.execute(stmt)
                total += len(batch)

                if batch_num % 5 == 0 or batch_num == total_batches:
                    logger.info(
                        f"  fact_ctsv | Tiến độ: {batch_num}/{total_batches} batches "
                        f"({total:,}/{len(deduped_records):,} records)"
                    )

            if skipped > 0:
                logger.warning(f"  fact_ctsv | Skipped {skipped} rows (dữ liệu không hợp lệ)")

            logger.info(
                f"  fact_ctsv      → {total:,} records (upserted) | "
                f"Đã loại: {df_dupes_removed + list_dupes_removed} duplicates"
            )
            return total

        except Exception as e:
            logger.error(f"  fact_ctsv | Lỗi nghiêm trọng: {e}")
            raise
    def _load_fact_tai_chinh(self, df: pd.DataFrame) -> int:
        """
        Load dữ liệu tài chính vào fact_tai_chinh.
        
        FIX InvalidDatetimeFormat: Xử lý NaT ở 2 tầng:
        - Tầng 1: _to_date() nhận diện pd.NaT → trả về None
        - Tầng 2: Làm sạch toàn bộ cột datetime trước iterrows()
        """
        if df.empty:
            logger.info("  fact_tai_chinh → SKIP (empty)")
            return 0

        # ── DEDUP phòng ngừa ─────────────────────────────────────────────
        rows_before = len(df)
        df = df.drop_duplicates(
            subset=["ma_sinh_vien", "hoc_ky"],
            keep="last"
        )
        if len(df) < rows_before:
            logger.warning(
                f"  fact_tai_chinh | [DEDUP] Loại bỏ {rows_before - len(df)} "
                f"bản ghi trùng ({rows_before} → {len(df)})"
            )

        # ── FIX: Làm sạch cột datetime — chuyển NaT → None ──────────────
        # Giải thích tại sao cần bước này dù _to_date() đã được vá:
        # Pandas DataFrame dùng kiểu nội bộ datetime64[ns] cho cột ngày.
        # Khi iterrows() đọc từng ô, pd.NaT đôi khi bị trả về dưới dạng
        # numpy.datetime64('NaT') thay vì pd.NaT — behavior không nhất quán.
        # Việc convert cột về object dtype trước đảm bảo None được truyền
        # đúng cách vào _to_date() và sau đó vào psycopg2.
        datetime_cols = ["ngay_dong_cuoi"]
        for col in datetime_cols:
            if col in df.columns:
                # Bước 1: Đảm bảo cột là kiểu datetime để pd.isnull() hoạt động
                df[col] = pd.to_datetime(df[col], errors="coerce")
                # Bước 2: Chuyển NaT → None, giữ nguyên giá trị hợp lệ
                # where(condition, other): giữ giá trị gốc nếu condition=True,
                # thay bằng `other` nếu condition=False
                df[col] = df[col].astype(object).where(df[col].notnull(), None)

        logger.info(
            f"  fact_tai_chinh | Đã làm sạch {len(datetime_cols)} cột datetime "
            f"(NaT → None)"
        )
        # ────────────────────────────────────────────────────────────────

        # ── Build records ────────────────────────────────────────────────
        records = []
        for _, row in df.iterrows():
            ma_sv = row.get("ma_sinh_vien")
            ma_hk = row.get("hoc_ky")

            if not all([ma_sv, ma_hk]):
                continue
            ma_sv = str(ma_sv)
            ma_hk = str(ma_hk)

            sv_key = self._lookup_sv_key(ma_sv)
            hk_key = self._lookup_hk_key(ma_hk)

            if not all([sv_key, hk_key]):
                continue

            records.append({
                "sinh_vien_key"    : sv_key,
                "hoc_ky_key"       : hk_key,
                "ma_sinh_vien"     : ma_sv,
                "ma_hoc_ky"        : ma_hk,
                "hoc_phi_phai_dong": self._to_int(row.get("hoc_phi_phai_dong")) or 0,
                "da_dong"          : self._to_int(row.get("da_dong")) or 0,
                "con_no"           : self._to_int(row.get("con_no")) or 0,
                "duoc_mien_giam"   : bool(row.get("duoc_mien_giam", False)),
                "ly_do_mien_giam"  : row.get("ly_do_mien_giam"),
                "so_tien_mien_giam": self._to_int(row.get("so_tien_mien_giam")) or 0,
                # _to_date() giờ đã xử lý được NaT sau khi vá Fix 1
                "ngay_dong_cuoi"   : self._to_date(row.get("ngay_dong_cuoi")),
                "nguon_du_lieu"    : "api_portal",
            })

        if not records:
            logger.info("  fact_tai_chinh → SKIP (no valid records)")
            return 0

        # ── DEDUP Tầng 2 trên records list ───────────────────────────────
        seen = {}
        for rec in records:
            seen[(rec["ma_sinh_vien"], rec["hoc_ky_key"])] = rec
        records = list(seen.values())

        # ── Batch Upsert ─────────────────────────────────────────────────
        try:
            total = 0
            for batch in self._chunked(records):
                stmt = pg_insert(FactTaiChinh).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["ma_sinh_vien", "hoc_ky_key"],
                    set_={
                        "hoc_phi_phai_dong": stmt.excluded.hoc_phi_phai_dong,
                        "da_dong"          : stmt.excluded.da_dong,
                        "con_no"           : stmt.excluded.con_no,
                        "duoc_mien_giam"   : stmt.excluded.duoc_mien_giam,
                        "ly_do_mien_giam"  : stmt.excluded.ly_do_mien_giam,
                        "so_tien_mien_giam": stmt.excluded.so_tien_mien_giam,
                        "ngay_dong_cuoi"   : stmt.excluded.ngay_dong_cuoi,
                    },
                )
                with self.engine.begin() as conn:
                    conn.execute(stmt)
                total += len(batch)

            logger.info(f"  fact_tai_chinh → {total:>6,} records (upserted)")
            return total

        except Exception as e:
            logger.error(f"  fact_tai_chinh | Lỗi: {e}")
            raise
    def _delete_fact_by_hk(self, hk_key: int, ma_hoc_ky: str):
        # FIX: Thêm fact_dang_ky vào danh sách xóa khi load incremental
        tables_and_cols = [
            ("fact_hoc_tap",   "hoc_ky_key"),
            ("fact_dang_ky",   "hoc_ky_key"),
            ("fact_ctsv",      "hoc_ky_key"),
            ("fact_tai_chinh", "hoc_ky_key"),
        ]
        with self.engine.begin() as conn:
            for table, col in tables_and_cols:
                try:
                    result = conn.execute(
                        text(f"DELETE FROM {table} WHERE {col} = :hk_key"),
                        {"hk_key": hk_key},
                    )
                    if result.rowcount > 0:
                        logger.info(
                            f"  {table} | Xóa {result.rowcount} records cũ (HK: {ma_hoc_ky})"
                        )
                except Exception as e:
                    logger.debug(f"  {table} | Skip delete: {e}")

    # ────────────────────────────────────────────────────────────────
    # TYPE CONVERSION HELPERS
    # ────────────────────────────────────────────────────────────────
    
    def _chunked(self, lst: list) -> list:
        for i in range(0, len(lst), self.batch_size):
            yield lst[i : i + self.batch_size]


    @staticmethod
    def _to_decimal(val):
        if val is None:
            return None
        if isinstance(val, float) and np.isnan(val):
            return None
        try:
            return round(float(val), 2)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_int(val) -> Optional[int]:
        if val is None:
            return None
        if isinstance(val, float) and np.isnan(val):
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None
    @staticmethod
    def _to_date(val) -> Optional[date]:
        """
        Chuyển đổi giá trị đầu vào thành kiểu date của Python.
        
        Xử lý các trường hợp đặc biệt:
        - None, float NaN  → trả về None (NULL trong SQL)
        - pd.NaT           → trả về None  ← FIX MỚI
        - date object      → giữ nguyên
        - pd.Timestamp     → lấy .date()
        - string           → parse rồi lấy .date()
        """
        # Trường hợp None
        if val is None:
            return None

        # Trường hợp float NaN (ví dụ: float('nan'))
        if isinstance(val, float) and np.isnan(val):
            return None

     
        if pd.isnull(val):
            return None
        # ────────────────────────────────────────────────────────────────

        if isinstance(val, date):
            return val

        if isinstance(val, pd.Timestamp):
            return val.date()

        try:
            return pd.to_datetime(val).date()
        except Exception:
            return None