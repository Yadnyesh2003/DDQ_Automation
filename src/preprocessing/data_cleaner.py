import pandas as pd
import numpy as np

def _convert_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError:
            pass
        if "date" in col.lower() or "timestamp" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)            
            except Exception:
                pass
        df[col] = df[col].replace(["nan", "NaN", "None", "NaT", ""], np.nan)
    return df


def _trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def _standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace("\n", "")
    )
    return df


def clean_dataframes(dataframes: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    cleaned = {}
    for name, df in dataframes.items():
        print(f"🧹 Cleaning dataframe: {name}")
        df = _standardize_column_names(df)
        df = _trim_strings(df)
        df = _convert_dtypes(df)
        df = df.reset_index(drop=True)
        cleaned[name] = df
        print(f"✅ Cleaned: {name} ({len(df)} rows, {len(df.columns)} columns)")
    return cleaned
