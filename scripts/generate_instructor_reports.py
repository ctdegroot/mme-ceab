import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
import shutil
from ceab import CEAB

def generate_instructor_reports(academic_year: str, destination: str):
    """Generate instructor reports for a given academic year.

    Parameters
    ----------
    academic_year : str
        The academic year to generate reports for, e.g., "2023-2024".
    destination : str
        The directory to save the reports.
    """
    ceab = CEAB()

    # Get all of the courses for the specified academic year
    courses = ceab.get_courses_by_academic_year(academic_year)

    if not courses:
        print(f"⚠️  No courses found for academic year {academic_year}.")
        return
    
    # Generate reports for each course
    for course in courses:
        try:
            print(f"📄 Generating report for {course}...")
            pdf_file = ceab.generate_course_report(course, academic_year)
            if not pdf_file:
                print(f"⚠️  Skipping {course} due to LaTeX error.")
                continue

            # Move the PDF to the destination directory
            destination_path = os.path.join(destination, course, os.path.basename(pdf_file))
            shutil.move(pdf_file, destination_path)
            print(f"✅ Saved to {destination_path}")
        except Exception as e:
            print(f"❌ Error generating report for {course}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate instructor reports for a given academic year.")
    parser.add_argument("academic_year", type=str, help="The academic year to plot data for, e.g., '2023-2024'.")
    parser.add_argument("destination", type=str, help="The destination directory to save the documents. Defaults to current directory.", nargs='?', default='.')
    args = parser.parse_args()

    generate_instructor_reports(args.academic_year, args.destination)