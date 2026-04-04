import pandas as pd
from pandas import DataFrame

def normalize_columns(df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower()

    normalize_mapping ={
        k.strip().lower(): v.strip().lower()
        for k, v in column_mapping.items()
        }

    df = df.rename(columns=normalize_mapping)
    return df

df = pd.DataFrame({
    " Diem_RL ": [80, 90],
    "Hoc_Bong": ["A", "B"],
    "Ten": ["An", "Binh"]
})

mapping = {
    "diem_rl": "diem_ren_luyen",
    "hoc_bong": "loai_hoc_bong"
}

df_new = normalize_columns(df, mapping)
print(df_new.columns)