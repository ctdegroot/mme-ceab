import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
import shutil
from ceab import CEAB

def generate_feedback_forms(academic_year: str, destination: str):
    """Generate feedback forms for a given academic year.

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
    
    # Generate feedback form for each course
    for course in courses:
        try:
            print(f"📄 Generating feedback form for {course}...")
            xlsx_file = ceab.generate_course_feedback_form(course, academic_year)

            # Move the file to the destination directory
            destination_path = os.path.join(destination, course, os.path.basename(xlsx_file))
            shutil.move(xlsx_file, destination_path)
            print(f"✅ Saved to {destination_path}")
        except Exception as e:
            print(f"❌ Error feedback from for {course}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate instructor feedback forms for a given academic year.")
    parser.add_argument("academic_year", type=str, help="The academic year to generate reports for, e.g., '2023-2024'.")
    parser.add_argument("destination", type=str, help="The destination directory to save the forms. Defaults to current directory.", nargs='?', default='.')
    args = parser.parse_args()

    generate_feedback_forms(args.academic_year, args.destination)