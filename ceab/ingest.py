import pandas as pd
from pathlib import Path
from sqlalchemy.exc import IntegrityError
from ceab.database import get_session, init_db
from ceab.models import Instructor, Course, Measurement, Data


class Sheets():
    """Class with static variables for the Excel sheet names."""
    instructor = "1 - Instructor"
    course = "2 - Course"
    measurement = "3 - Measurement"
    data = "4 - Data"

VALID_KEYS = {
    'instructor': ['instructorID', 'firstName', 'lastName'],
    'course': ['courseID', 'instructorID', 'prefix', 'number', 'suffix', 'academicYear', 'yearInProgram'],
    'measurement': ['measurementID', 'courseID', 'attribute', 'indicator', 'deliverableType', 'deliverableName', 'date', 'gradeScale', 'maxScore', 'minPercentScore2', 'minPercentScore3', 'minPercentScore4', 'improvementTheme'],
    'data': ['dataID', 'studentID', 'measurementID', 'score']
}

def convert_scores_to_ceab_scale(data_dict):
    """Converts scores in the data dictionary to CEAB (1-4) scale.
    
    Parameters
    ----------
    data_dict : dict
        Dictionary containing DataFrames for 'measurement' and 'data'.

    Returns
    -------
    dict
        Updated dictionary with converted scores in the 'data' DataFrame.
    """
    df_measurements = data_dict["measurement"]
    df_data = data_dict["data"]

    # CASE 1: Already on CEAB (1–4) scale -> round and clip
    mask = df_measurements["gradeScale"] == "CEAB (1-4)"
    for measurement_id in df_measurements.loc[mask, "measurementID"]:
        idx = df_data["measurementID"] == measurement_id
        df_data.loc[idx, "score"] = (
            df_data.loc[idx, "score"].round().astype(int).clip(lower=1, upper=4)
        )

    # CASE 2: Raw Scores (Standard Bins)
    mask = df_measurements["gradeScale"] == "Raw Scores (Standard Bins)"
    for _, row in df_measurements.loc[mask].iterrows():
        if pd.isna(row["maxScore"]):
            raise ValueError(f"Missing maxScore for measurement {row['measurementID']}")
        max_score = row["maxScore"]
        idx = df_data["measurementID"] == row["measurementID"]
        df_data.loc[idx, "score"] = pd.cut(
            df_data.loc[idx, "score"] / max_score * 100,
            bins=[0, 50, 60, 85, 100],
            labels=[1, 2, 3, 4],
            include_lowest=True
        ).astype(int)

    # CASE 3: Raw Scores (Custom Bins)
    mask = df_measurements["gradeScale"] == "Raw Scores (Custom Bins)"
    for _, row in df_measurements.loc[mask].iterrows():
        required = ["maxScore", "minPercentScore2", "minPercentScore3", "minPercentScore4"]
        for field in required:
            if pd.isna(row[field]):
                raise ValueError(f"{field} missing for measurement {row['measurementID']}")
        bins = [0, row["minPercentScore2"], row["minPercentScore3"], row["minPercentScore4"], 100]
        scores = [1, 2, 3, 4]
        idx = df_data["measurementID"] == row["measurementID"]
        df_data.loc[idx, "score"] = pd.cut(
            df_data.loc[idx, "score"] / row["maxScore"] * 100,
            bins=bins,
            labels=scores,
            include_lowest=True
        ).astype(int)

    data_dict["data"] = df_data
    return data_dict

def ingest_excel_data(data_file: str) -> dict:
    """Reads CEAB-format Excel file into a dictionary of DataFrames.
    
    Parameters
    ---------- 
    data_file : str
        Path to the Excel file.

    Returns
    -------
    dict
        Dictionary with keys 'instructor', 'course', 'measurement', and 'data' containing DataFrames.
    """
    data_file = Path(data_file)
    if data_file.suffix != ".xlsx":
        raise TypeError(f"Invalid file extension: {data_file.suffix}. Expected .xlsx")

    data_dict = {}
    for attr in ["instructor", "course", "measurement"]:
        sheet_name = getattr(Sheets, attr)
        try:
            df = pd.read_excel(data_file, sheet_name=sheet_name)
            data_dict[attr] = df
        except ValueError:
            raise ValueError(f"Sheet '{sheet_name}' not found in {data_file}")

    # Handle 'data' sheet (assumed to be in wide format)
    data_sheet = getattr(Sheets, "data")
    df = pd.read_excel(data_file, sheet_name=data_sheet, skiprows=1)

    # Replace any whitespace values with NaN
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    if "studentID" not in df.columns:
        raise ValueError("Expected 'studentID' column in data sheet")

    value_columns = [col for col in df.columns if col != "studentID"]
    melted = pd.melt(
        df,
        id_vars=["studentID"],
        value_vars=value_columns,
        var_name="measurementID",
        value_name="score"
    ).dropna()

    data_dict["data"] = melted

    data_dict = convert_scores_to_ceab_scale(data_dict)

    return data_dict

def row_to_instructor(row):
    return Instructor(
        instructorID=row["instructorID"],
        firstName=row["firstName"],
        lastName=row["lastName"]
    )

def row_to_course(row):
    return Course(
        courseID=row["courseID"],
        instructorID=row["instructorID"],
        prefix=row["prefix"],
        number=row["number"],
        suffix=row["suffix"],
        academicYear=row["academicYear"],
        yearInProgram=row["yearInProgram"]
    )

def row_to_measurement(row):
    return Measurement(
        measurementID=row["measurementID"],
        courseID=row["courseID"],
        attribute=row["attribute"],
        indicator=row["indicator"],
        deliverableType=row["deliverableType"],
        deliverableName=row["deliverableName"],
        date=row["date"],
        improvementTheme=row.get("improvementTheme", None)
    )

def row_to_data(row):
    return Data(
        measurementID=row["measurementID"],
        studentID=row["studentID"],
        score=row.get("score", None)
    )

def insert_into_db(data_dict, overwrite=False):
    """Inserts data from the dictionary into the database.
    
    Parameters
    ----------
    data_dict : dict
        Dictionary containing DataFrames for 'instructor', 'course', 'measurement', and 'data'.
    overwrite : bool, optional
        If True, overwrite existing records. Default is False.
    """
    init_db()
    session = get_session()

    try:
        # Replace NaN with None in all DataFrames before inserting into the database
        for key in data_dict:
            data_dict[key] = data_dict[key].where(pd.notnull(data_dict[key]), None)

        # Replace zero scores with None
        data_dict["data"]["score"] = data_dict["data"]["score"].replace(0, None)

        # Validate 'score' column
        if data_dict["data"]["score"].notnull().any():
            invalid_scores = data_dict["data"][~data_dict["data"]["score"].between(1, 4, inclusive="both")]
            if not invalid_scores.empty:
                print("⚠️ Invalid 'score' values found:")
                print(invalid_scores)
                raise ValueError("Invalid 'score' values found. Scores must be between 1 and 4.")

        # Instructors, Courses, Measurements (use merge for upserts)
        for _, row in data_dict["instructor"].iterrows():
            session.merge(row_to_instructor(row))

        for _, row in data_dict["course"].iterrows():
            session.merge(row_to_course(row))

        for _, row in data_dict["measurement"].iterrows():
            session.merge(row_to_measurement(row))

        # Data rows: Insert, skip duplicates (since we are using a composite primary key)
        for _, row in data_dict["data"].iterrows():
            data_obj = row_to_data(row)
            try:
                session.add(data_obj)
                session.flush()  # Forces INSERT so we can catch IntegrityError
            except IntegrityError:
                session.rollback()  # Reset failed INSERT
                continue  # Skip this row

        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()