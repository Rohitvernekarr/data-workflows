"""Transform a local mapped Kjell file into a local Excel workbook."""

import argparse
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
SUPPORTED_EXTENSIONS = {".csv", ".xls", ".xlsx", ".xlsm", ".txt", ".tsv"}


def transform_file(file_path: str | Path) -> pd.DataFrame:
    """Read a local mapped file and apply the Kjell sales/inventory transformation."""
    extension = Path(file_path).suffix.lower()
    if extension == ".csv":
        Kjell_arlo_df = pd.read_csv(file_path)
        if len(Kjell_arlo_df.columns) == 1:
            Kjell_arlo_df = pd.read_csv(file_path, delimiter=";")
    elif extension in {".xls", ".xlsx", ".xlsm"}:
        Kjell_arlo_df = pd.read_excel(file_path)
    elif extension in {".txt", ".tsv"}:
        Kjell_arlo_df = pd.read_csv(file_path, sep="\t")
    else:
        raise ValueError(f"Unsupported mapped file extension: {extension}")

    Kjell_arlo_df.columns = Kjell_arlo_df.columns.str.strip()

    logger.info("INPUT COLUMNS: %s", Kjell_arlo_df.columns.tolist())

    # Fixed row order with type and SoldToName per column
    ROW_ORDER = [
        ("SalesStoreSE",  "Sales",     "Kjell SE"),
        ("SalesWebSE",    "Sales",     "Kjell SE"),
        ("InventorySE",   "Inventory", "Kjell SE"),
        ("SalesStoreNO",  "Sales",     "Kjell NO"),
        ("SalesWebNO",    "Sales",     "Kjell NO"),
        ("SalesAVCTotal", "Sales",     "Kjell DK"),
        ("InventoryNO",   "Inventory", "Kjell NO"),
    ]

    # Identity columns A–F carried forward as-is
    id_cols = Kjell_arlo_df.columns[:6].tolist()

    output_rows = []

    for _, row in Kjell_arlo_df.iterrows():

        for i, (col, col_type, sold_to) in enumerate(ROW_ORDER):
            is_first = (i == 0)
            new_row = {c: row[c] for c in id_cols}
            new_row["SalesStoreSE"]   = row.get("SalesStoreSE")   if is_first else None
            new_row["SalesWebSE"]     = row.get("SalesWebSE")     if is_first else None
            new_row["InventorySE"]    = row.get("InventorySE")    if is_first else None
            new_row["SalesStoreNO"]   = row.get("SalesStoreNO")   if is_first else None
            new_row["SalesWebNO"]     = row.get("SalesWebNO")     if is_first else None
            new_row["SalesAVCTotal"]  = row.get("SalesAVCTotal")  if is_first else None
            new_row["InventoryNO"]    = row.get("InventoryNO")    if is_first else None
            new_row["SalesTotal"]     = row.get("SalesTotal")     if is_first else None
            new_row["InventoryTotal"] = row.get("InventoryTotal") if is_first else None
            new_row["Sales"]          = row.get(col, 0) if col_type == "Sales"     else None
            new_row["Inventory"]      = row.get(col, 0) if col_type == "Inventory" else None
            new_row["SoldToName"]     = sold_to
            output_rows.append(new_row)


    # Build final output with exact column order
    output_df = pd.DataFrame(output_rows, columns=[
        "ItemNo", "Description", "Description2", "Model", "OemItemNo", "VendorItemNo",
        "SalesStoreSE", "SalesWebSE", "InventorySE", "SalesStoreNO", "SalesWebNO",
        "SalesAVCTotal", "InventoryNO", "SalesTotal", "InventoryTotal",
        "Sales", "Inventory", "SoldToName"
    ])


    output_df['OemItemNo'] = output_df['OemItemNo'].fillna('').astype(str).str.strip()
    output_df['Model'] = output_df['Model'].fillna('').astype(str).str.strip()

    mask = output_df['OemItemNo'].eq('') & output_df['Model'].ne('')
    output_df.loc[mask, 'OemItemNo'] = output_df.loc[mask, 'Model']



    # CALCULATE DATE FROM FILE NAME (TAKES MAX DATE)

    file_name = Path(file_path).stem
    date_parts = []

    # Existing formats
    for part in file_name.split("_"):
        for fmt in [
            "%Y%m%d",
            "%Y-%m-%d",
            "%m-%d-%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%m/%d/%Y",
            "%d/%m/%Y"
        ]:
            try:
                date_parts.append(datetime.strptime(part, fmt))
                break
            except ValueError:
                pass

    # New condition:
    # Matches dates like:
    # 2026_05_25
    # 2026 05 25
    # 2026-05-25
    for match in re.findall(r'(\d{4}[-_ ]\d{2}[-_ ]\d{2})', file_name):
        try:
            normalized = re.sub(r'[-_ ]', '-', match)
            date_parts.append(datetime.strptime(normalized, "%Y-%m-%d"))
        except ValueError:
            pass

    logger.info("FILE NAME: %s", file_name)
    logger.info("DATE PARTS: %s", date_parts)

    if date_parts:
        formatted_date = max(date_parts).strftime("%m/%d/%Y")
    else:
        formatted_date = ""

    output_df["DATE"] = formatted_date

    return output_df


def main(
    mapped_file_path: str | Path,
    output_file_path: str | Path | None = None,
) -> Path:
    """Transform a local input; leave transfers and staging cleanup to the caller."""
    local_path = Path(mapped_file_path)
    if local_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported mapped file extension: {local_path.suffix}")
    output_path = (
        Path(output_file_path) if output_file_path is not None
        else local_path.with_name(f"{local_path.stem}_Transformed.xlsx")
    )
    if output_path.resolve() == local_path.resolve():
        raise ValueError("Input and output paths must differ")
    output_df = transform_file(local_path)
    output_df.to_excel(output_path, index=False)
    logger.info("Saved transformed file locally: %s", output_path)
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mapped_file_path", nargs="?", default=os.environ.get("LOCAL_MAPPED_FILE_PATH"),
        help="Local input path (defaults to LOCAL_MAPPED_FILE_PATH from the pipeline)",
    )
    parser.add_argument(
        "--output", default=os.environ.get("LOCAL_TRANSFORMED_FILE_PATH"),
        help="Local output workbook path (defaults to LOCAL_TRANSFORMED_FILE_PATH)",
    )
    args = parser.parse_args()
    if not args.mapped_file_path:
        parser.error("mapped_file_path or LOCAL_MAPPED_FILE_PATH is required")
    logging.basicConfig(level=logging.INFO)
    main(args.mapped_file_path, args.output)
