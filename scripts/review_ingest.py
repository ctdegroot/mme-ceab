"""Review CEAB data files and report issues without touching the database.

This is a read-only companion to ``batch_ingest.py``. It walks the same folder
structure but, instead of inserting anything, it inspects each course's Excel
file and prints a clear, per-course list of problems.

The checks here are kept in lock-step with the browser tool
(``tools/src/validator_core.mjs``) — if you add or change a rule in one, change
the other too. Both validate against the same lists (mirrored from
``ceab/models.py``).

    python scripts/review_ingest.py "/path/to/2025-2026 CEAB Measurements"
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
import argparse
import warnings
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

warnings.filterwarnings(
    "ignore",
    message="Data Validation extension is not supported and will be removed",
)

# Prefixes used to decide which folders are course folders (broader than the
# DB-valid set — e.g. Internship folders exist but aren't ingested).
FOLDER_PREFIXES = ("CHEM", "ECE", "ELI", "ES", "MME", "MSE", "NMM", "PHYS", "STATS", "WRIT", "Internship")

# Valid values (mirror ceab/models.py CheckConstraints). Intentionally broader
# than the spreadsheet's own drop-downs, which are out of date.
VALID_PREFIXES = ["MME", "ECE", "ES", "ELI", "MSE", "CHEM", "PHYS", "STATS", "WRIT", "NMM"]
VALID_SUFFIXES = ["A", "B", "F", "none"]
VALID_ATTRIBUTES = ["KB", "PA", "I", "DES", "ITW", "ET", "CS", "PR", "IES", "EE", "EPM", "LL"]
VALID_DELIVERABLE_TYPES = ["Assignment", "Final Exam", "Lab", "Midterm Exam",
                           "Presentation", "Project", "Quiz", "Test", "Course Grade"]
VALID_GRADE_SCALES = ["CEAB (1-4)", "Raw Scores (Standard Bins)", "Raw Scores (Custom Bins)"]
KNOWN_IMPROVEMENT_THEMES = ["GD&T", "Computational Tools", "Communication", "Manufacturing"]

SHEETS = {
    "instructor": "1 - Instructor",
    "course": "2 - Course",
    "measurement": "3 - Measurement",
    "data": "4 - Data",
}

ERROR, WARNING, INFO = "error", "warning", "info"


class Issue:
    def __init__(self, level, category, title, detail=""):
        self.level = level
        self.category = category
        self.title = title
        self.detail = detail


# ----- value helpers (mirror validator_core.mjs) -----
def is_blank(v):
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    try:
        if pd.isna(v):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(v, str) and v.strip() == ""


def txt(v):
    return "" if is_blank(v) else str(v).strip()


def as_num(v):
    if isinstance(v, bool):
        return float("nan")
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return float("nan")


def is_num(v):
    return not is_blank(v) and not pd.isna(as_num(v))


def col_letter(index):
    s, i = "", index + 1
    while i > 0:
        i, m = divmod(i - 1, 26)
        s = chr(65 + m) + s
    return s


def join_list(items):
    items = list(items)
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])} and {items[-1]}"


def cap_list(items, n):
    items = list(items)
    if len(items) > n:
        return f"{', '.join(items[:n])} and {len(items) - n} more"
    return join_list(items)


def nice_num(n):
    return str(int(n)) if float(n).is_integer() else str(round(n, 4))


def where(deliverable):
    return f" (“{deliverable}”)" if deliverable else ""


def blank_or(v, phrase):
    """'is blank' when v is empty, else '“value” <phrase>'."""
    return "is blank" if is_blank(v) else f"“{txt(v)}” {phrase}"


def serial_year(serial):
    return (datetime(1899, 12, 30) + timedelta(days=int(serial))).year


def find_excel_files(folder: Path):
    files = list(folder.glob("*.xlsx"))
    return [f for f in files if not f.name.startswith("narrative_")]


def read_grid(xls, sheet):
    """Read a sheet as a raw 2D grid (no header inference), NaN -> None.

    Mirrors how the browser tool sees the sheet, so both tools apply identical
    logic. Excel dates come back as pandas Timestamps; numbers as floats.
    """
    df = pd.read_excel(xls, sheet_name=sheet, header=None)
    return df.where(pd.notnull(df), None).values.tolist()


def header_map(row):
    m = {}
    for i, v in enumerate(row or []):
        if not is_blank(v):
            m[str(v).strip()] = i
    return m


# ----- per-sheet checks -----
def check_instructor(grid, add):
    if grid is None:
        return
    head = header_map(grid[0] if grid else [])
    any_row = False
    for r in range(1, len(grid)):
        row = grid[r]
        if not row or all(is_blank(v) for v in row):
            continue
        any_row = True
        get = lambda name: row[head[name]] if name in head and head[name] < len(row) else None
        # instructorID is auto-generated from the name by a formula, so we only
        # check the fields the instructor actually types.
        miss = [label for f, label in [("firstName", "first name"), ("lastName", "last name")]
                if is_blank(get(f))]
        if miss:
            add(ERROR, "missing_instructor",
                f"On the “1 - Instructor” tab, row {r + 1} is missing {join_list(miss)}",
                "Please fill in the instructor’s first name and last name.")
    if not any_row:
        add(ERROR, "missing_instructor",
            "The “1 - Instructor” tab has no instructor filled in",
            "Please enter the instructor’s first name and last name.")


def check_course(grid, add):
    if grid is None:
        return
    head = header_map(grid[0] if grid else [])
    any_row = False
    for r in range(1, len(grid)):
        row = grid[r]
        if not row or all(is_blank(v) for v in row):
            continue
        any_row = True
        at = f"On the “2 - Course” tab, row {r + 1}"
        get = lambda name: row[head[name]] if name in head and head[name] < len(row) else None

        # courseID and instructorID are auto-generated by formulas from the fields
        # below, so we validate those inputs rather than the generated columns.
        prefix = get("prefix")
        if is_blank(prefix) or txt(prefix) not in VALID_PREFIXES:
            add(ERROR, "invalid_prefix",
                f"{at}: the course prefix {'is blank' if is_blank(prefix) else f'“{txt(prefix)}” is not recognised'}",
                f"Use one of the valid subject codes: {', '.join(VALID_PREFIXES)}.")
        number = get("number")
        if is_blank(number) or pd.isna(as_num(number)) or not as_num(number).is_integer() \
                or as_num(number) < 1000 or as_num(number) > 9999:
            add(ERROR, "invalid_number",
                f"{at}: the course number {'is blank' if is_blank(number) else f'“{txt(number)}” is not valid'}",
                "The course number must be a 4-digit whole number between 1000 and 9999.")
        suffix = get("suffix")
        if is_blank(suffix) or txt(suffix) not in VALID_SUFFIXES:
            add(ERROR, "invalid_suffix",
                f"{at}: the course suffix {'is blank' if is_blank(suffix) else f'“{txt(suffix)}” is not valid'}",
                "The suffix must be A, B, F, or none (type the word “none” if the course has no letter).")
        year = get("academicYear")
        if is_blank(year) or not re.fullmatch(r"\d{4}/\d{2}", txt(year)):
            add(ERROR, "invalid_academic_year",
                f"{at}: the academic year {'is blank' if is_blank(year) else f'“{txt(year)}” is not in the right format'}",
                "Write the academic year as four digits, a slash, then two digits — for example 2025/26.")
        yip = get("yearInProgram")
        if is_blank(yip) or pd.isna(as_num(yip)) or not as_num(yip).is_integer() \
                or as_num(yip) < 1 or as_num(yip) > 4:
            add(ERROR, "invalid_year_in_program",
                f"{at}: the year in program {'is blank' if is_blank(yip) else f'“{txt(yip)}” is not valid'}",
                "The year in program must be a whole number from 1 to 4.")
    if not any_row:
        add(ERROR, "missing_course_field",
            "The “2 - Course” tab has no course filled in",
            "Please fill in the course details.")


def check_measurement(grid, add):
    head = header_map(grid[0] if grid else [])
    def hcol(name):
        return head[name] if name in head else -1
    date_letter = col_letter(hcol("date")) if hcol("date") >= 0 else "the date"
    max_letter = col_letter(hcol("maxScore")) if hcol("maxScore") >= 0 else "the maxScore"

    rows = []
    missing_dates = []
    for r in range(1, len(grid)):
        row = grid[r]
        if not row or all(is_blank(v) for v in row):
            continue
        get = lambda name: row[head[name]] if name in head and head[name] < len(row) else None
        m = {k: get(k) for k in ["measurementID", "attribute", "indicator", "deliverableType",
                                 "deliverableName", "date", "gradeScale", "maxScore",
                                 "minPercentScore2", "minPercentScore3", "minPercentScore4",
                                 "improvementTheme"]}
        m["excelRow"] = r + 1
        rows.append(m)
        at = f"On the “3 - Measurement” tab, row {m['excelRow']}{where(m['deliverableName'])}"

        if is_blank(m["deliverableName"]):
            add(ERROR, "missing_deliverable_name",
                f"On the “3 - Measurement” tab, row {m['excelRow']}: the deliverable name is blank",
                "Give this measurement a short name (for example, “Final Exam Q4” or “Lab 3”).")
        if is_blank(m["attribute"]) or txt(m["attribute"]) not in VALID_ATTRIBUTES:
            add(ERROR, "invalid_attribute",
                f"{at}: the Graduate Attribute {blank_or(m['attribute'], 'is not recognised')}",
                f"Choose a valid attribute from the drop-down list. The allowed codes are: {', '.join(VALID_ATTRIBUTES)}.")
        ind = as_num(m["indicator"])
        if is_blank(m["indicator"]) or pd.isna(ind) or not ind.is_integer() or int(ind) not in (1, 2, 3, 4):
            add(ERROR, "invalid_indicator",
                f"{at}: the Indicator {'is blank' if is_blank(m['indicator']) else 'is “' + txt(m['indicator']) + '”'}",
                "The indicator must be a whole number from 1 to 4. Please correct this cell.")
        if is_blank(m["deliverableType"]) or txt(m["deliverableType"]) not in VALID_DELIVERABLE_TYPES:
            add(ERROR, "invalid_deliverable_type",
                f"{at}: the Deliverable Type {blank_or(m['deliverableType'], 'is not recognised')}",
                f"Choose one from the drop-down list. The allowed types are: {', '.join(VALID_DELIVERABLE_TYPES)}.")

        # date: required + must look like a real date
        d = m["date"]
        if is_blank(d):
            missing_dates.append(str(m["excelRow"]))
        else:
            year = None
            if isinstance(d, (pd.Timestamp, datetime)):
                year = d.year
            elif isinstance(d, (int, float)) and not isinstance(d, bool):
                year = serial_year(d)
            else:
                add(ERROR, "invalid_date_value",
                    f"{at}: the date “{txt(d)}” isn’t a real date",
                    "Type the date into the cell using Excel’s date format (for example 2025-10-07), rather than as plain text.")
            if year is not None and (year < 2015 or year > 2035):
                add(WARNING, "invalid_date_value",
                    f"{at}: the date doesn’t look right (it reads as the year {year})",
                    "Please check this cell — it may have been typed as a plain number or a year by mistake. Enter it using Excel’s date format (for example 2025-10-07).")

        # gradeScale + downstream requirements
        scale = txt(m["gradeScale"])
        if is_blank(m["gradeScale"]) or scale not in VALID_GRADE_SCALES:
            add(ERROR, "invalid_grade_scale",
                f"{at}: the grade scale {blank_or(m['gradeScale'], 'is not recognised')}",
                "Choose one from the drop-down list: “CEAB (1-4)” if you already scored on the 1–4 scale, "
                "“Raw Scores (Standard Bins)” for raw marks, or “Raw Scores (Custom Bins)” for raw marks with your own thresholds.")
        elif scale == "Raw Scores (Standard Bins)":
            if is_blank(m["maxScore"]):
                add(ERROR, "missing_maxscore",
                    f"{at}: the grade scale is “Raw Scores (Standard Bins)” but the maxScore is blank",
                    f"Enter the maximum possible mark for this assessment in column {max_letter} (for example, if it was marked out of 30, type 30). "
                    "The tool needs this to convert raw marks to the 1–4 scale.")
            elif not is_num(m["maxScore"]) or as_num(m["maxScore"]) <= 0:
                add(ERROR, "invalid_max_score",
                    f"{at}: the maxScore “{txt(m['maxScore'])}” is not a valid number",
                    "The maximum score must be a number greater than 0.")
        elif scale == "Raw Scores (Custom Bins)":
            fields = [("maxScore", "the maximum score"), ("minPercentScore2", "the minimum % for a 2"),
                      ("minPercentScore3", "the minimum % for a 3"), ("minPercentScore4", "the minimum % for a 4")]
            missing = [label for f, label in fields if is_blank(m[f])]
            if missing:
                add(ERROR, "missing_custom_bins",
                    f"{at}: the grade scale is “Raw Scores (Custom Bins)” but some values are blank",
                    f"Please fill in: {join_list(missing)}. All four values are needed so the tool knows how to turn raw marks into 1–4 scores.")
            else:
                if not is_num(m["maxScore"]) or as_num(m["maxScore"]) <= 0:
                    add(ERROR, "invalid_max_score",
                        f"{at}: the maxScore “{txt(m['maxScore'])}” is not a valid number",
                        "The maximum score must be a number greater than 0.")
                p2, p3, p4 = as_num(m["minPercentScore2"]), as_num(m["minPercentScore3"]), as_num(m["minPercentScore4"])
                labels = [("minimum % for a 2", p2), ("minimum % for a 3", p3), ("minimum % for a 4", p4)]
                bad = [l for l, v in labels if pd.isna(v) or v < 0 or v > 100]
                if bad:
                    add(ERROR, "invalid_custom_bins",
                        f"{at}: some custom-bin percentages are out of range",
                        f"{join_list(bad)} must each be a number between 0 and 100.")
                elif not (p2 < p3 < p4):
                    add(ERROR, "invalid_custom_bins",
                        f"{at}: the custom-bin percentages are not in increasing order",
                        f"The thresholds must increase: the minimum % for a 2 ({nice_num(p2)}) must be less than for a 3 "
                        f"({nice_num(p3)}), which must be less than for a 4 ({nice_num(p4)}).")

        if not is_blank(m["improvementTheme"]) and txt(m["improvementTheme"]) not in KNOWN_IMPROVEMENT_THEMES:
            add(WARNING, "unusual_improvement_theme",
                f"{at}: the improvement theme “{txt(m['improvementTheme'])}” isn’t one of the usual options",
                f"This field is optional. If you meant one of the standard themes ({', '.join(KNOWN_IMPROVEMENT_THEMES)}), "
                "pick it from the drop-down; otherwise you can ignore this note.")

    if missing_dates:
        plural = len(missing_dates) > 1
        add(ERROR, "missing_date",
            f"{len(missing_dates)} measurement{'s are' if plural else ' is'} missing a date",
            f"On the “3 - Measurement” tab, fill in the date (column {date_letter}) for "
            f"row{'s' if plural else ''} {cap_list(missing_dates, 12)}. Use the date the assessment took place. "
            "If the assessment took place over multiple dates, use the earliest date.")
    return rows


def check_data(grid, meas_rows, add):
    dhead = [None if is_blank(v) else str(v).strip() for v in (grid[1] if len(grid) > 1 else [])]
    student_col = dhead.index("studentID") if "studentID" in dhead else -1
    if student_col < 0:
        add(ERROR, "missing_sheet",
            "The “4 - Data” tab doesn’t have a “studentID” heading",
            "On the “4 - Data” tab, the second row should start with a heading called “studentID”, "
            "followed by one column for each measurement. Please use the official template so these headings are correct.")
        return

    value_cols = {i: name for i, name in enumerate(dhead) if name and i != student_col}
    value_col_set = set(value_cols)

    scores_by_col = {}
    student_ids = []       # (id, excel_row)
    stray_cols = set()
    orphan_rows = []
    for r in range(2, len(grid)):
        row = grid[r] if grid[r] else []
        non_empty = [c for c in range(len(row)) if not is_blank(row[c])]
        if not non_empty:
            continue
        has_student = student_col < len(row) and not is_blank(row[student_col])
        if has_student:
            student_ids.append((txt(row[student_col]), r + 1))
        data_cells = [c for c in non_empty if c != student_col]
        if data_cells and not has_student:
            orphan_rows.append(str(r + 1))
        for c in data_cells:
            if c in value_col_set:
                scores_by_col.setdefault(value_cols[c], []).append(row[c])
            else:
                stray_cols.add(col_letter(c))

    # studentID must not be a plain number (student number typed by mistake)
    def normalize_id(s):
        n = as_num(s)
        if not pd.isna(n) and n.is_integer():
            return str(int(n))
        return s
    numeric_ids = [(sid, rownum) for sid, rownum in student_ids if re.fullmatch(r"\d+", normalize_id(sid))]
    if numeric_ids:
        plural = len(numeric_ids) > 1
        add(ERROR, "numeric_student_id",
            f"{len(numeric_ids)} student ID{'s look' if plural else ' looks'} like a student number instead of a username",
            "On the “4 - Data” tab, the studentID should be the Western login/username (letters and numbers, e.g. "
            f"“jsmith42”), not the 9-digit student number. Please fix the studentID in "
            f"row{'s' if plural else ''} {cap_list([str(r) for _, r in numeric_ids], 12)}.")

    # duplicate studentIDs
    seen, dups = set(), []
    for sid, _ in student_ids:
        if sid in seen and sid not in dups:
            dups.append(sid)
        seen.add(sid)
    if dups:
        plural = len(dups) > 1
        add(WARNING, "duplicate_student_id",
            "The same studentID appears more than once",
            f"On the “4 - Data” tab, {cap_list([f'“{d}”' for d in dups], 8)} appear"
            f"{'' if plural else 's'} on more than one row. Each student should have a single row. "
            "Please remove or merge the duplicates.")

    if stray_cols:
        plural = len(stray_cols) > 1
        add(ERROR, "stray_data",
            "There is data outside the labelled score columns on the “4 - Data” tab",
            f"Found values in column{'s' if plural else ''} {cap_list(sorted(stray_cols), 8)}, which don’t have a "
            "measurement heading in row 2. Scores must go only in the labelled columns. Please move or clear this stray "
            "data (use “Clear Contents”, don’t delete the columns).")

    if orphan_rows:
        plural = len(orphan_rows) > 1
        add(ERROR, "orphan_row",
            "Some rows have scores but no studentID",
            f"On the “4 - Data” tab, row{'s' if plural else ''} {cap_list(orphan_rows, 12)} contain marks but the "
            "studentID is blank. Add the missing studentID, or clear the row if it isn’t needed.")

    meas_ids = {m["measurementID"] for m in meas_rows if not is_blank(m["measurementID"])}
    meas_by_id = {m["measurementID"]: m for m in meas_rows}

    for name in sorted(scores_by_col):
        if name not in meas_ids:
            old = re.search(r"20\d\d/\d\d", name)
            hint = f" The “{old.group(0)}” in the name suggests it’s left over from a previous year." if old else ""
            add(ERROR, "unmatched_data_column",
                "On the “4 - Data” tab there is a column of scores that doesn’t match any measurement",
                f"The column labelled “{name}” has marks in it, but there is no matching row on the "
                f"“3 - Measurement” tab.{hint} Either delete this column, or add the matching measurement on the "
                "Measurement tab. Every column of scores must line up with a measurement.")
    for mid in sorted(meas_ids):
        if mid not in scores_by_col:
            m = meas_by_id.get(mid)
            add(WARNING, "measurement_without_data",
                f"A measurement on the “3 - Measurement” tab{where(m and m['deliverableName'])} has no scores",
                f"There is no column of marks for “{mid}” on the “4 - Data” tab. If you meant to collect "
                "data for it, add a column with that heading and enter the scores. If not, you can ignore this.")

    zero_cols, zero_total = [], 0
    for name in sorted(scores_by_col):
        raw = scores_by_col[name]
        numeric, bad = [], set()
        zeros = 0
        for v in raw:
            n = as_num(v)
            if pd.isna(n):
                bad.add(txt(v))
            else:
                numeric.append(n)
                if n == 0:
                    zeros += 1
        m = meas_by_id.get(name)
        label = f"“{m['deliverableName']}”" if m and m["deliverableName"] else f"“{name}”"

        if bad:
            bad_sorted = sorted(bad)
            preview = ", ".join(f"“{b}”" for b in bad_sorted[:5])
            more = f", and {len(bad_sorted) - 5} other value(s)" if len(bad_sorted) > 5 else ""
            add(ERROR, "non_numeric_score",
                f"On the “4 - Data” tab, the {label} column contains text where a score should be",
                f"Found: {preview}{more}. Score cells must contain numbers only. If a student didn’t complete the work, "
                "leave the cell blank (see the note about zeros) — don’t type words like “absent” or "
                "“academic consideration” into the score cells.")
        if zeros:
            zero_cols.append(label)
            zero_total += zeros

        if not m or not numeric:
            continue
        scale = txt(m["gradeScale"])
        if scale in ("Raw Scores (Standard Bins)", "Raw Scores (Custom Bins)"):
            if is_num(m["maxScore"]) and as_num(m["maxScore"]) > 0:
                mx = as_num(m["maxScore"])
                over = [s for s in numeric if s > mx or s < 0]
                if over:
                    plural = len(over) > 1
                    add(ERROR, "score_exceeds_max",
                        f"On the “4 - Data” tab, the {label} column has {len(over)} score{'s' if plural else ''} "
                        "outside the allowed range",
                        f"You set the maximum for this assessment to {nice_num(mx)}, but some marks are higher than that or "
                        f"below zero (for example, {nice_num(max(over))}). Either the maxScore on the Measurement tab is wrong, "
                        "or the marks were entered on a different scale (for example as percentages). Please check and correct one of them.")
        elif scale == "CEAB (1-4)":
            out = [s for s in numeric if s != 0 and (s < 1 or s > 4)]
            if out:
                plural = len(out) > 1
                add(WARNING, "ceab_out_of_range",
                    f"On the “4 - Data” tab, the {label} column is set to the “CEAB (1-4)” scale but has scores outside 1–4",
                    f"{len(out)} mark{'s are' if plural else ' is'} outside the 1–4 range (for example, {nice_num(max(out))}). "
                    "If these are raw marks out of a larger total, change the grade scale for this measurement to one of the "
                    "“Raw Scores” options on the Measurement tab.")

    if zero_total:
        plural = zero_total > 1
        add(WARNING, "zeros_present",
            f"There {'are' if plural else 'is'} {zero_total} zero{'s' if plural else ''} in the scores — please double-check {'them' if plural else 'it'}",
            "A blank cell is left out of the results, but a 0 is counted as a completed assessment with the lowest score. "
            "If a student did not complete the work, clear the cell (leave it blank) so it isn’t scored against them. "
            f"Only keep a 0 if the student genuinely attempted it and earned nothing. Zeros were found in: {cap_list(zero_cols, 8)}.")


def review_course(folder: Path):
    """Return a list of Issue objects for a single course folder."""
    issues = []
    add = lambda level, category, title, detail="": issues.append(Issue(level, category, title, detail))

    files = find_excel_files(folder)
    if len(files) == 0:
        add(WARNING, "missing_file", "No Excel data file found in folder", "")
        return issues
    if len(files) > 1:
        add(ERROR, "multiple_files", f"Multiple Excel files: {', '.join(sorted(f.name for f in files))}",
            "Keep exactly one CEAB spreadsheet in the folder so it's clear which to use.")
        return issues

    path = files[0]
    try:
        xls = pd.ExcelFile(path)
        present = set(xls.sheet_names)
        missing_tabs = [t for t in SHEETS.values() if t not in present]
        if missing_tabs:
            add(ERROR, "missing_sheet",
                f"The spreadsheet is missing the tab{'s' if len(missing_tabs) > 1 else ''}: "
                + ", ".join(f'“{t}”' for t in missing_tabs),
                "This doesn’t look like the official CEAB template. Please start from the blank CEAB template and try again.")
            return issues
        grids = {k: read_grid(xls, name) for k, name in SHEETS.items()}
    except Exception as e:  # noqa: BLE001
        add(ERROR, "read_error", "Sorry — this file couldn’t be read", f"{type(e).__name__}: {e}")
        return issues

    # Template only? (no scores anywhere) -> single friendly note.
    data_grid = grids["data"]
    dhead = [None if is_blank(v) else str(v).strip() for v in (data_grid[1] if len(data_grid) > 1 else [])]
    student_col = dhead.index("studentID") if "studentID" in dhead else -1
    any_data = False
    if student_col >= 0:
        for r in range(2, len(data_grid)):
            row = data_grid[r] or []
            if any(c != student_col and not is_blank(row[c]) for c in range(len(row))):
                any_data = True
                break
    if student_col >= 0 and not any_data:
        add(INFO, "no_data", "No student scores have been entered yet",
            "The “4 - Data” tab only contains the template headings. This is fine if you haven’t collected the "
            "data yet. Once you’ve entered student scores, run this check again.")
        return issues

    check_instructor(grids["instructor"], add)
    check_course(grids["course"], add)
    meas_rows = check_measurement(grids["measurement"], add)

    # If the measurements have real content but every auto-generated measurementID
    # is blank, Excel hasn't recalculated the formula columns (the Data-tab
    # headings will be blank too). Say so once, instead of a cascade of
    # "unmatched column" / "stray data" errors that all stem from the same cause.
    contentful = [m for m in meas_rows if not is_blank(m["deliverableName"]) and not is_blank(m["attribute"])]
    not_recalculated = len(contentful) > 0 and all(is_blank(m["measurementID"]) for m in meas_rows)
    if not_recalculated:
        add(ERROR, "formulas_not_calculated",
            "This file’s automatic ID columns are blank — it needs to be reopened in Excel",
            "The measurement ID column and the score-column headings on the “4 - Data” tab are filled in "
            "automatically by Excel formulas, but they’re currently blank — this usually means the file was saved "
            "before Excel recalculated. Everything you typed in looks fine. Please open the file in Excel, press F9 "
            "to recalculate (or just save it again), and then re-check it. This matters for submitting too: the "
            "data can’t be processed until those automatic columns fill in.")
    else:
        check_data(data_grid, meas_rows, add)
    return issues


def batch_review(parent_dir: Path):
    if not parent_dir.is_dir():
        raise NotADirectoryError(f"{parent_dir} is not a valid directory")
    results = []
    for child in sorted(parent_dir.iterdir()):
        if child.is_dir() and child.name.startswith(FOLDER_PREFIXES):
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
            bullet = "•" if issue.level == ERROR else ("◦" if issue.level == WARNING else "ℹ")
            print(f"      {bullet} {issue.title}")
            category_counts.setdefault(issue.category, set()).add(course)

    if clean:
        print("\n✅ No issues found:")
        for course in clean:
            print(f"      • {course}")

    total = len(results)
    print("\n" + "-" * 64)
    print(" Summary")
    print("-" * 64)
    print(f" Courses reviewed : {total}")
    print(f" Clean            : {len(clean)}")
    print(f" With issues      : {total - len(clean)}")
    if category_counts:
        print("\n Issues by type (number of courses affected):")
        for category, courses in sorted(category_counts.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            print(f"   {category:<28}: {len(courses)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Review CEAB Excel files and report issues (read-only, no database changes).")
    parser.add_argument("parent_dir", type=Path,
                        help="Path to the parent directory containing course subfolders.")
    args = parser.parse_args()
    batch_review(args.parent_dir)
