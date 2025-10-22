import os
import pandas as pd
from pathlib import Path

def read_input_files(input_path: str, required_files: list[str]) -> dict:
    input_dir = Path(input_path)
    if not input_dir.exists():
        raise FileNotFoundError(f"❌ Input folder not found at {input_dir.resolve()}")
    dataframes = {}
    for file_name in required_files:
        csv_path = input_dir / f"{file_name}.csv"
        xlsx_path = input_dir / f"{file_name}.xlsx"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            source = csv_path
        elif xlsx_path.exists():
            df = pd.read_excel(xlsx_path)
            source = xlsx_path
        else:
            print(f"⚠️  Warning: Required file '{file_name}' not found in input directory.")
            continue
        dataframes[file_name] = df
        print(f"✅ Loaded: {file_name} ({len(df)} rows) from {source.name}")
    if not dataframes:
        raise ValueError("❌ No valid input files were loaded. Please check input directory and file names.")
    return dataframes


def export_dataframe(df: pd.DataFrame, export_path: str, filename: str):
    export_dir = Path(export_path)
    export_dir.mkdir(parents=True, exist_ok=True)
    output_file = export_dir / filename
    df.to_csv(output_file, index=False)
    print(f"📁 Exported: {filename} → {output_file.resolve()}")
