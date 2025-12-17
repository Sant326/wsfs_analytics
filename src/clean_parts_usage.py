import pandas as pd
from pathlib import Path

RAW_PATH = "../data_raw/tonic_fabricate_db/parts_usage.csv"
OUTPUT_PATH = "../data_processed/part_usages_clean.csv"

def main():
    df = pd.read_csv(RAW_PATH)

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # IDs
    id_cols = [c for c in df.columns if c.endswith("_id")]
    for col in id_cols:
        df[col] = df[col].astype(str).str.strip()

    # Duplicados y NaN en ID clave
    if "part_usage" in df.columns:
        df = df.dropna(subset=["part_usage"])
        df = df.drop_duplicates(subset=["part_usage"])

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"part_usages limpio generado: {df.shape}")

if __name__ == "__main__":
    main()
