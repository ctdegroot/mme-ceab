import sys
print(sys.path)

import argparse
from pathlib import Path
import warnings
from ceab.ingest import ingest_excel_data, insert_into_db

# Suppress openpyxl "Data Validation extension" warning
warnings.filterwarnings(
    "ignore",
    message="Data Validation extension is not supported and will be removed",
    module="openpyxl.worksheet._read_only"
)
warnings.filterwarnings(
    "ignore",
    message="Data Validation extension is not supported and will be removed",
    module="openpyxl.worksheet._reader"
)

VALID_PREFIXES = ("CHEM", "ECE", "ELI", "ES", "MME", "MSE")

def find_single_excel_file(folder: Path) -> Path | None:
    xlsx_files = list(folder.glob("*.xlsx"))
    if len(xlsx_files) == 0:
        print(f"⚠️  No Excel file found in {folder.name}. Skipping.")
        return None
    elif len(xlsx_files) > 1:
        raise ValueError(f"Multiple Excel files found in {folder.name}: {[f.name for f in xlsx_files]}")
    return xlsx_files[0]

def batch_ingest(parent_dir: Path):
    if not parent_dir.is_dir():
        raise NotADirectoryError(f"{parent_dir} is not a valid directory")

    processed = 0
    missing = 0
    errors = 0

    for child in sorted(parent_dir.iterdir()):
        if child.is_dir() and child.name.startswith(VALID_PREFIXES):
            relative_name = child.relative_to(parent_dir)
            try:
                xlsx_path = find_single_excel_file(child)
                if xlsx_path is None:
                    missing += 1
                    continue

                print(f"📥 Ingesting {relative_name / xlsx_path.name}")
                data = ingest_excel_data(xlsx_path)
                insert_into_db(data)
                processed += 1

            except Exception as e:
                print(f"❌ Error in '{relative_name}': {e}")
                errors += 1

    # Summary
    print("\n📊 Ingestion Summary")
    print(f"✅ Processed successfully: {processed}")
    print(f"⚠️  Missing Excel file   : {missing}")
    print(f"❌ Errors (multiple files, etc.): {errors}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch ingest CEAB Excel files from subfolders.")
    parser.add_argument("parent_dir", type=Path, help="Path to the parent directory containing subfolders.")
    args = parser.parse_args()

    batch_ingest(args.parent_dir)