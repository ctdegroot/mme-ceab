"""Review CEAB data files and report issues without touching the database.

This is a read-only companion to ``batch_ingest.py``. It walks the same folder
structure but, instead of inserting anything, it inspects each course's Excel
file and prints a clear, per-course list of problems (missing data file,
template-only/no data entered, missing measurement dates, invalid attribute /
indicator / deliverable type, missing maxScore, non-numeric scores, scores that
exceed their maxScore, mismatched measurement/data columns, etc.).

Use it to clean up spreadsheets *before* running the real ingest.

    python scripts/review_ingest.py "/path/to/2025-2026 CEAB Measurements"
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
from pathlib import Path
import warnings

import pandas as pd

# Suppress openpyxl "Data Validation extension" warning
warnings.filterwarnings(
    "ignore",
    message="Data Validation extension is not supported and will be removed",
)

#VALID_PREFIXES = ("CHEM", "ECE", "ELI", "ES", "MME", "MSE", "NMM", "PHYS", "STATS", "WRIT", "Internship")
VALID_PREFIXES = ("MME")

# These mirror the CheckConstraints in ceab/models.py. Keep them in sync.
VALID_ATTRIBUTES = {"KB", "PA", "I", "DES", "ITW", "ET", "CS", "PR", "IES", "EE", "EPM", "LL"}
VALID_DELIVERABLE_TYPES = {
    "Assignment", "Final Exam", "Lab", "Midterm Exam",
    "Presentation", "Project", "Quiz", "Test", "Course Grade",
}

SHEETS = {
    "instructor": "1 - Instructor",
    "course": "2 - Course",
    "measurement": "3 - Measurement",
    "data": "4 - Data",
}

# Severity levels for issues.
ERROR = "error"   # blocks ingest
WARN = "warn"     # worth a look, may not block

# A short, human-readable label for each issue category (used in the summary).
CATEGORY_LABELS = {
    "missing_file": "No Excel data file",
    "multiple_files": "Multiple Excel files",
    "missing_sheet": "Missing required sheet",
    "read_error": "Could not read file",
    "no_data": "Template only (no data entered)",
    "missing_date": "Measurement missing a date",
    "invalid_attribute": "Invalid attribute",
    "invalid_indicator": "Invalid indicator",
    "invalid_deliverable_type": "Invalid deliverable type",
    "missing_maxscore": "Missing maxScore",
    "missing_custom_bins": "Missing custom bin thresholds",
    "non_numeric_score": "Non-numeric score value",
    "score_exceeds_max": "Score exceeds maxScore",
    "ceab_out_of_range": "CEAB-scale score outside 1-4",
    "unmatched_data_column": "Data column with no matching measurement",
    "measurement_without_data": "Measurement with no data column",
}


class Issue:
    def __init__(self, level, category, message):
        self.level = level
        self.category = category
        self.message = message


def find_excel_files(folder: Path):
    """Return the non-narrative .xlsx files in a folder."""
    files = list(folder.glob("*.xlsx"))
    return [f for f in files if not f.name.startswith("narrative_")]


def _read_sheet(path, key, issues):
    """Read a named sheet; append a missing_sheet issue and return None on failure."""
    try:
        return pd.read_excel(path, sheet_name=SHEETS[key])
    except ValueError:
        issues.append(Issue(ERROR, "missing_sheet", f"Sheet '{SHEETS[key]}' not found"))
        return None


def review_measurements(df_meas, issues):
    """Validate the measurement sheet, row by row."""
    missing_dates = []
    for _, row in df_meas.iterrows():
        mid = row.get("measurementID", "<unknown>")

        if pd.isna(row.get("date")):
            missing_dates.append(mid)

        attribute = row.get("attribute")
        if pd.isna(attribute) or attribute not in VALID_ATTRIBUTES:
            issues.append(Issue(ERROR, "invalid_attribute",
                                f"{mid}: attribute '{attribute}' is not valid"))

        indicator = row.get("indicator")
        if pd.isna(indicator) or indicator not in (1, 2, 3, 4):
            issues.append(Issue(ERROR, "invalid_indicator",
                                f"{mid}: indicator '{indicator}' is not 1-4"))

        dtype = row.get("deliverableType")
        if pd.isna(dtype) or dtype not in VALID_DELIVERABLE_TYPES:
            issues.append(Issue(ERROR, "invalid_deliverable_type",
                                f"{mid}: deliverable type '{dtype}' is not valid"))

        scale = row.get("gradeScale")
        if scale == "Raw Scores (Standard Bins)":
            if pd.isna(row.get("maxScore")):
                issues.append(Issue(ERROR, "missing_maxscore", f"{mid}: maxScore is blank"))
        elif scale == "Raw Scores (Custom Bins)":
            required = ["maxScore", "minPercentScore2", "minPercentScore3", "minPercentScore4"]
            missing = [f for f in required if pd.isna(row.get(f))]
            if missing:
                issues.append(Issue(ERROR, "missing_custom_bins",
                                    f"{mid}: missing {', '.join(missing)}"))

    if missing_dates:
        n = len(missing_dates)
        total = len(df_meas)
        issues.append(Issue(ERROR, "missing_date",
                            f"{n} of {total} measurement(s) missing a date"))


def melt_scores(df_data):
    """Reshape the wide data sheet into (studentID, measurementID, score) rows.

    Returns None if the sheet has no 'studentID' column. Whitespace-only cells
    are treated as blank so an untouched template melts to zero rows.
    """
    if "studentID" not in df_data.columns:
        return None
    df_data = df_data.replace(r"^\s*$", pd.NA, regex=True)
    value_columns = [c for c in df_data.columns if c != "studentID"]
    return pd.melt(
        df_data, id_vars=["studentID"], value_vars=value_columns,
        var_name="measurementID", value_name="score",
    ).dropna(subset=["score"])


def review_data(df_meas, df_data, melted, issues):
    """Validate the data sheet against the measurement sheet (assumes data present)."""
    value_columns = [c for c in df_data.columns if c != "studentID"]
    measurement_ids = set(df_meas["measurementID"].dropna())
    data_ids = set(value_columns)

    for col in sorted(data_ids - measurement_ids):
        # Only flag columns that actually contain scores.
        if not melted.loc[melted["measurementID"] == col].empty:
            issues.append(Issue(ERROR, "unmatched_data_column",
                                f"Data column '{col}' has no matching measurement row"))

    for mid in sorted(measurement_ids - data_ids):
        issues.append(Issue(WARN, "measurement_without_data",
                            f"Measurement '{mid}' has no column in the data sheet"))

    # Per-measurement score checks.
    meas_by_id = {r["measurementID"]: r for _, r in df_meas.iterrows()}
    for mid, group in melted.groupby("measurementID"):
        raw = group["score"]
        coerced = pd.to_numeric(raw, errors="coerce")
        bad = sorted(set(raw[coerced.isna()].astype(str)))
        if bad:
            preview = ", ".join(repr(b) for b in bad[:5])
            more = f" (+{len(bad) - 5} more)" if len(bad) > 5 else ""
            issues.append(Issue(ERROR, "non_numeric_score",
                                f"{mid}: non-numeric score(s) {preview}{more}"))

        row = meas_by_id.get(mid)
        if row is None:
            continue
        numeric = coerced.dropna()
        if numeric.empty:
            continue

        scale = row.get("gradeScale")
        if scale in ("Raw Scores (Standard Bins)", "Raw Scores (Custom Bins)"):
            max_score = row.get("maxScore")
            if pd.notna(max_score) and max_score:
                over = numeric[(numeric > max_score) | (numeric < 0)]
                if not over.empty:
                    issues.append(Issue(ERROR, "score_exceeds_max",
                                        f"{mid}: {len(over)} score(s) outside 0-{max_score} "
                                        f"(e.g. {over.max():g}) — check maxScore or units"))
        elif scale == "CEAB (1-4)":
            out = numeric[(numeric < 1) | (numeric > 4)]
            if not out.empty:
                issues.append(Issue(WARN, "ceab_out_of_range",
                                    f"{mid}: {len(out)} score(s) outside 1-4 on a CEAB scale "
                                    f"(e.g. {out.max():g}) — wrong gradeScale?"))


def review_course(folder: Path):
    """Return a list of Issue objects for a single course folder."""
    issues = []
    files = find_excel_files(folder)

    if len(files) == 0:
        issues.append(Issue(WARN, "missing_file", "No Excel data file found in folder"))
        return issues
    if len(files) > 1:
        names = ", ".join(sorted(f.name for f in files))
        issues.append(Issue(ERROR, "multiple_files", f"Multiple Excel files: {names}"))
        return issues

    path = files[0]
    try:
        df_meas = _read_sheet(path, "measurement", issues)
        try:
            df_data = pd.read_excel(path, sheet_name=SHEETS["data"], skiprows=1)
        except ValueError:
            issues.append(Issue(ERROR, "missing_sheet", f"Sheet '{SHEETS['data']}' not found"))
            df_data = None
    except Exception as e:  # noqa: BLE001 - report any unexpected read failure
        issues.append(Issue(ERROR, "read_error", f"{type(e).__name__}: {e}"))
        return issues

    # Work out whether the data sheet actually contains any scores.
    melted = None
    if df_data is not None:
        melted = melt_scores(df_data)
        if melted is None:
            issues.append(Issue(ERROR, "missing_sheet", "Data sheet has no 'studentID' column"))

    # A file whose data sheet is empty is just an untouched template. Report that
    # single fact rather than a pile of complaints about placeholder rows.
    if melted is not None and melted.empty:
        issues.append(Issue(WARN, "no_data", "No scores entered (file is just the template)"))
        return issues

    if df_meas is not None:
        review_measurements(df_meas, issues)
    if df_meas is not None and df_data is not None and melted is not None:
        review_data(df_meas, df_data, melted, issues)

    return issues


def batch_review(parent_dir: Path):
    if not parent_dir.is_dir():
        raise NotADirectoryError(f"{parent_dir} is not a valid directory")

    results = []  # (course_name, [Issue, ...])
    for child in sorted(parent_dir.iterdir()):
        if child.is_dir() and child.name.startswith(VALID_PREFIXES):
            results.append((child.name, review_course(child)))

    print_report(parent_dir.name, results)


def print_report(title, results):
    print("=" * 64)
    print(f" CEAB Data Review — {title}")
    print("=" * 64)

    clean = []
    category_counts = {}

    for course, issues in results:
        if not issues:
            clean.append(course)
            continue

        has_error = any(i.level == ERROR for i in issues)
        icon = "❌" if has_error else "⚠️ "
        print(f"\n{icon} {course}")
        for issue in issues:
            bullet = "•" if issue.level == ERROR else "◦"
            print(f"      {bullet} {issue.message}")
            category_counts.setdefault(issue.category, set()).add(course)

    if clean:
        print("\n✅ No issues found:")
        for course in clean:
            print(f"      • {course}")

    # Summary
    total = len(results)
    with_issues = total - len(clean)
    print("\n" + "-" * 64)
    print(" Summary")
    print("-" * 64)
    print(f" Courses reviewed : {total}")
    print(f" Clean            : {len(clean)}")
    print(f" With issues      : {with_issues}")

    if category_counts:
        print("\n Issues by type (number of courses affected):")
        for category, courses in sorted(category_counts.items(),
                                        key=lambda kv: (-len(kv[1]), kv[0])):
            label = CATEGORY_LABELS.get(category, category)
            print(f"   {label:<38}: {len(courses)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Review CEAB Excel files and report issues (read-only, no database changes).")
    parser.add_argument("parent_dir", type=Path,
                        help="Path to the parent directory containing course subfolders.")
    args = parser.parse_args()

    batch_review(args.parent_dir)
