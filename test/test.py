import glob, os
import pandas as pd
from pandas import DataFrame
from zmq import NULL

class MyCSVExtractor:
    REQUIRED_COLS = ["ma_sinh_vien", "hoc_ky", "diem_ren_luyen"]

    def __init__(self, csv_dir: str):
        self.csv_dir = csv_dir

    def read_one_file(self, filepath: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(filepath,encoding = "utf-8",dtype={"ma_sinh_vien": str, "hoc_ky": str, "diem_ren_luyen": int}, na_values=["", "NULL", "null"])
            df.columns = df.columns.str.strip().str.lower()

            missing = set(self.REQUIRED_COLS) - set(df.columns)

            if missing:
                print(f"cot thieu la {missing}")
            return df
        except Exception as e:
            print(f"loi la {e}")
            return pd.DataFrame()
    def read_all_file(self) -> pd.DataFrame:
        csv_files = sorted(glob.glob(os.path.join(self.csv_dir,"ctsv_*.csv")))
        csv_file = [
            f for f in csv_files
            if "all" not in os.path.basename(f).lower()
                ]

        all_dfs = []

        for fp in csv_file:
            df = self.read_one_file(fp)
            if not df.empty:
                all_dfs.append(df)
        if not all_dfs:
            return pd.DataFrame()

        results = pd.concat(all_dfs,ignore_index= True)

        before = len(results)
        results = results.drop_duplicates(
            subset=["ma_sinh_vien", "hoc_ky"],
            keep="last"
        )
        dupes = before - len(results)
        if dupes > 0:
            print(f"CSV loai bo {dupes} trung lap")
        return results  
    def validate_data(self, df: DataFrame) -> dict:
        result = {}
        result["total_records"] = len(df)
        result["null_ma_sv"] = df["ma_sinh_vien"].isna().sum()
        result["invalid_drl"] = df[
            (df["diem_ren_luyen"] < 0) |
            (df["diem_ren_luyen"]> 100)].shape[0]
        return result

extractor = MyCSVExtractor("data/csv/")
df = extractor.read_all_file()
res = extractor.validate_data(df)
print(res)