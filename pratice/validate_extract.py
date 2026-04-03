import pandas as pd
import glob

from pandas import read_csv

def read_csv_safe(filepath: str) -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]

    for x in encodings:
        try:
            df = pd.read_csv(filepath, encoding=x)
            print(f"Doc thanh cong: {x}")
            return df
        except UnicodeDecodeError:
            print(f" Encoding {x} that bai")
        except FileNotFoundError:
            print(f"khong tim thay file")
            return pd.DataFrame()
    print(f"khong the doc file voi bat ky encoding nao")
    return pd.DataFrame()

file = glob.glob("data/csv/*.csv")
dfs = []
for x in file:
    df = read_csv_safe(x)
    if not df.empty:
        dfs.append(df)
if dfs:
    final_dfs = pd.concat(dfs, ignore_index = True)
    print(final_dfs.head())