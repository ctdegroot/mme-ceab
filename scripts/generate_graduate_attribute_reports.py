import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
import shutil
import openpyxl
from ceab import CEAB
from ceab import all_attributes

def generate_instructor_reports(academic_year: str, course_prefixes: list, num_past_years: int, destination: str, feedback_dir: str):
    """Generate graduate attribute reports for a given academic year.

    Parameters
    ----------
    academic_year : str
        The academic year to generate reports for, e.g., "2023-2024".
    course_prefixes : list
        A list of course prefixes to filter by, e.g., ["MME", "ES", "ELI"].
    num_past_years : int
        The number of past years to include in the report.
    destination : str
        The directory to save the reports.
    feedback_dir : str
        The directory where instructor feedback forms are stored.
    """
    ceab = CEAB()

    # Check that the feedback directory exists before collecting data.
    if not os.path.exists(feedback_dir):
        raise FileNotFoundError(f"Feedback directory {feedback_dir} does not exist.")

    # Collect all of the instructor feedback data.
    # It is assumed that the feedback directory contains subdirectories for each course.
    print(f"📥 Collecting instructor feedback data from {feedback_dir}...")
    feedback_data = {}
    for course in os.listdir(feedback_dir):
        course_path = os.path.join(feedback_dir, course)
        if os.path.isdir(course_path):
            # Check for .xlxs files in the course directory. If there is more than one, raise an error. If there are none, skip this course.
            xlsx_files = [f for f in os.listdir(course_path) if f.endswith('.xlsx')]
            if len(xlsx_files) > 1:
                raise ValueError(f"Expected exactly one .xlsx file in {course_path}, found {len(xlsx_files)}.")
            if not xlsx_files:
                print(f"⚠️  No feedback data found for {course}. Skipping.")
                feedback_data[course] = None
                continue
            feedback_file = os.path.join(course_path, xlsx_files[0])

            # Read the data from the Excel file and store it.
            feedback_data[course] = {}
            try:
                wb = openpyxl.load_workbook(feedback_file, data_only=True)
            except Exception as e:
                raise ValueError(f"Error reading {feedback_file}: {e}")

            narrative_sheet = wb["Narrative"]
            # Loop through the rows and collect the data. Start with row 11 and continue until an empty cell is found in column B. 
            row = 11
            while True:
                attribute = narrative_sheet[f"B{row}"].value
                if not attribute:
                    break
                feedback_data[course][attribute] = {
                    "Instructor Statement": narrative_sheet[f"E{row}"].value or "",
                    "Follow-up Required?": narrative_sheet[f"D{row}"].value or "",
                }
                row += 1
            wb.close()

    # Generate reports for each attribute.
    for attribute in all_attributes.keys():
        print(f"📥 Generating report for {attribute}...")
        try:
            print(f"📄 Generating report for attribute {attribute}...")
            pdf_file = ceab.generate_graduate_attribute_report(attribute, academic_year, course_prefixes, num_past_years, feedback_data)
            if not pdf_file:
                print(f"⚠️  Skipping {attribute} due to LaTeX error.")
                continue

            # Move the PDF to the destination directory.
            if destination == '.':
                print(f"✅ Saved to current directory. ")
                continue  # Skip moving if destination is current directory.
            destination_path = os.path.join(destination, os.path.basename(pdf_file))
            shutil.move(pdf_file, destination_path)
            print(f"✅ Saved to {destination_path}")
        except Exception as e:
            print(f"❌ Error generating report for {attribute}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate instructor reports for a given academic year.")
    parser.add_argument("--academic_year", type=str, help="The academic year to plot data for, e.g., '2023/24'.")
    parser.add_argument("--course_prefixes", type=str, help="Comma-separated list of course prefixes to filter by. Defaults to 'MME,ES,ELI'.", nargs='?', default='MME,ES,ELI')
    parser.add_argument("--num_past_years", type=int, help="Number of past years to include in the report. Defaults to 3.", default=3)
    parser.add_argument("--destination", type=str, help="The destination directory to save the documents. Defaults to current directory.", nargs='?', default='.')
    parser.add_argument("--feedback_dir", type=str, help="The directory where instructor feedback forms are stored.")
    args = parser.parse_args()
    args.course_prefixes = args.course_prefixes.split(',')

    generate_instructor_reports(
        academic_year=args.academic_year,
        course_prefixes=args.course_prefixes,
        num_past_years=args.num_past_years,
        destination=args.destination,
        feedback_dir=args.feedback_dir
    )