import pandas as pd
from pathlib import Path

RAW_PATH = "../data_raw/tonic_fabricate_db/parts_sales.csv"
OUTPUT_PATH = "../data_processed/part_sales_clean.csv"

def main():
    df = pd.read_csv(RAW_PATH)

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Fechas
    date_cols = [c for c in df.columns if "date" in c]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # IDs
    id_cols = [c for c in df.columns if c.endswith("_id")]
    for col in id_cols:
        df[col] = df[col].astype(str).str.strip()

    # Duplicados y NaN en ID clave
    if "sale_line_id" in df.columns:
        df = df.dropna(subset=["sale_line_id"])
        df = df.drop_duplicates(subset=["sale_line_id"])

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"part_sales limpio generado: {df.shape}")

if __name__ == "__main__":
    main()
