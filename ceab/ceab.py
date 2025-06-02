from sqlalchemy.orm import Session
from ceab.database import get_session
from ceab.models import Instructor, Course, Measurement, Data
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import matplotlib.pyplot as plt
import subprocess


# Expected ranges for each score
expected_ranges = {
    '1': (0.00, 0.10),
    '2': (0.25, 0.50),
    '3': (0.40, 0.90),
    '4': (0.10, 0.30)
}

# Score names
score_names = {
    '1': 'Unacceptable',
    '2': 'Below Expectations',
    '3': 'Meets Expectations',
    '4': 'Exceeds Expectations'
}


class CEAB:
    """Class used to represent a CEAB measurements database."""

    def __init__(self):
        """Initialize the CEAB class with a database session."""
        self.session: Session = get_session()

    def get_table_as_dataframe(self, table_name: str) -> pd.DataFrame:
        """Fetch a table from the database and return it as a pandas DataFrame.

        Parameters
        ----------
        table_name : str
            Name of the table to fetch ('instructor', 'course', 'measurement', 'data').

        Returns
        -------
        pandas.DataFrame
            The table as a pandas DataFrame.
        """
        table_map = {
            "instructor": Instructor,
            "course": Course,
            "measurement": Measurement,
            "data": Data,
        }

        if table_name not in table_map:
            raise ValueError(f"Invalid table name: {table_name}")

        # Query the table and convert to a pandas DataFrame
        query = self.session.query(table_map[table_name])
        return pd.DataFrame([row.__dict__ for row in query.all()]).drop("_sa_instance_state", axis=1)

    def get_row_IDs_matching_criteria(self, table_name: str, criteria: dict) -> list:
        """Get the row IDs matching specific attributes for a given table.
        Note: the 'data' table uses a composite key and is therefore not including in this function.

        Parameters
        ----------
        table_name : str
            Name of the table to query ('instructor', 'course', 'measurement').
        criteria : dict
            Dictionary of attributes and their values to filter by.

        Returns
        -------
        list
            List of IDs that match the specified criteria.
        """
        table_map = {
            "instructor": Instructor,
            "course": Course,
            "measurement": Measurement,
        }

        if table_name not in table_map:
            raise ValueError(f"Invalid table name: {table_name}")

        # Build the query with the given criteria
        query = self.session.query(table_map[table_name])
        for key, value in criteria.items():
            query = query.filter(getattr(table_map[table_name], key) == value)

        # Return the IDs
        id_column = f"{table_name}ID"
        return [getattr(row, id_column) for row in query.all()]
    
    def get_scores_by_measurement_id(self, measurement_id: str) -> list:
        """Get scores for a specific measurement ID.

        Parameters
        ----------
        measurement_id : str
            The measurement ID to fetch scores for.

        Returns
        -------
        list
            List of scores for the specified measurement ID.
        """
        scores = self.session.query(Data.score).filter(Data.measurementID == measurement_id).all()
        return [v[0] for v in scores] # Extract values from tuples before returning

    def get_summary_table_by_course(self) -> pd.DataFrame:
        """Get a summary table of the data aggregated by course.

        Returns
        -------
        pandas.DataFrame
            Summary table of the data by course.
        """
        data_list = []

        # Query all unique course IDs
        course_ids = self.session.query(Course.courseID).distinct().all()

        for course_id, in course_ids:
            # Fetch course details
            course = self.session.query(Course).filter(Course.courseID == course_id).one()
            instructor = self.session.query(Instructor).filter(Instructor.instructorID == course.instructorID).one()
            course_code = f"{course.prefix.strip()} {course.number}{course.suffix.strip() if course.suffix.strip() != 'none' else ''}"
            instructor_name = f"{instructor.firstName} {instructor.lastName}"

            # Fetch measurements for the course
            measurements = self.session.query(Measurement).filter(Measurement.courseID == course_id).all()

            for measurement in measurements:
                # Fetch data for the measurement
                scores = self.get_scores_by_measurement_id(measurement.measurementID)

                # Compute statistics
                n_score_1 = scores.count(1)
                n_score_2 = scores.count(2)
                n_score_3 = scores.count(3)
                n_score_4 = scores.count(4)
                mean_score = sum(scores) / len(scores) if scores else None

                # Append to the data list
                data_list.append({
                    "course_code": course_code,
                    "instructor_name": instructor_name,
                    "attribute": measurement.attribute,
                    "indicator": measurement.indicator,
                    "n_score_1": n_score_1,
                    "n_score_2": n_score_2,
                    "n_score_3": n_score_3,
                    "n_score_4": n_score_4,
                    "mean_score": mean_score,
                })

        # Convert to a pandas DataFrame
        return pd.DataFrame(data_list)
    
    def plot_score_distributions(self, score_df: pd.DataFrame, course_code: str):
        """
        Plots score distributions as fractions for each unique attribute/indicator,
        grouped by academic year.

        Parameters
        ----------
        score_df : pandas.DataFrame
            DataFrame containing columns: attribute, indicator, academicYear, n_score_1 through n_score_4.
        course_code : str
            The course code to use in the plot filenames.
        """
        # Group by attribute-indicator combinations
        unique_combos = score_df[['attribute', 'indicator']].drop_duplicates()

        for _, row in unique_combos.iterrows():
            attr = row['attribute']
            ind = row['indicator']

            subset = score_df[(score_df['attribute'] == attr) & (score_df['indicator'] == ind)]

            score_labels = ['1', '2', '3', '4']
            bar_width = 0.2
            x = range(len(score_labels))

            plt.figure(figsize=(8, 5))

            # Draw expected ranges as translucent rectangles
            for i, label in enumerate(score_labels):
                low, high = expected_ranges[label]
                # Draw a horizontal band for the expected range
                plt.axhspan(
                    low, high,
                    xmin=(i + 0.05) / len(score_labels),  # Start just inside this bar group
                    xmax=(i + 0.95) / len(score_labels),  # End just before the next
                    color='gray', alpha=0.15, zorder=0
                )

            for i, (_, year_row) in enumerate(subset.iterrows()):
                counts = [
                    year_row['n_score_1'],
                    year_row['n_score_2'],
                    year_row['n_score_3'],
                    year_row['n_score_4']
                ]
                total = sum(counts) or 1  # Prevent division by zero
                fractions = [c / total for c in counts]
                plt.bar(
                    [pos + i * bar_width for pos in x],
                    fractions,
                    width=bar_width,
                    label=year_row['academic_year']
                )

            # Center the x-ticks in the middle of each grouped bar cluster
            num_years = len(subset)
            group_width = num_years * bar_width
            tick_positions = [pos + (group_width - bar_width) / 2 for pos in x]
            plt.xticks(tick_positions, score_labels)

            plt.xlabel("Score")
            plt.ylabel("Fraction of Students")
            plt.ylim(0, 1)
            plt.legend(title="Academic Year")
            plt.tight_layout()
            plt.savefig(f"{course_code.replace(' ', '_')}_{attr}{ind}.png")
            plt.close()

    def generate_course_report(self, course_code: str, academic_year: str):
        """Generate a report for a specific course.

        Parameters
        ----------
        course_code : str
            The course code to generate the report for.
        academic_year : str
            The academic year for which the report is generated.
        """
        # Generate the course prefix, number, and suffix from the course code.
        # The course code must be in the format "XYZ 1234A" where XYZ is the prefix, 
        # 1234 is the number, and A is the suffix.
        course_parts = course_code.split()
        if len(course_parts) != 2:
            raise ValueError("Invalid course code format. Expected format: 'XYZ 1234A'.")
        prefix = course_parts[0]
        number = int(course_parts[1][0:4] if len(course_parts[1]) > 4 else course_parts[1])
        suffix = course_parts[1][4:] if len(course_parts[1]) > 4 else "none"

        # Get all of the courseIDs that match the prefix, number, and suffix.
        course_ids = self.get_row_IDs_matching_criteria("course", {"prefix": prefix, "number": number, "suffix": suffix})
        if not course_ids:
            raise ValueError(f"No course found with code: {course_code}")
        print(f"Course IDs: {course_ids}")
        
        # Get all of the measurement data that match the courseIDs.
        measurements = self.session.query(Measurement).filter(Measurement.courseID.in_(course_ids)).all()

        # Get all of the unique combinations of attribute and indicator in the measurements.
        attr_ind_pairs = sorted({(m.attribute, m.indicator) for m in measurements})
        print(f"Attribute-Indicator pairs: {attr_ind_pairs}")

        # Get the score distributions and metadata for all of the measurementIDs.
        rows = []
        for m in measurements:
            scores = self.get_scores_by_measurement_id(m.measurementID)
            n1, n2, n3, n4 = scores.count(1), scores.count(2), scores.count(3), scores.count(4)

            rows.append({
                "attribute": m.attribute,
                "indicator": m.indicator,
                "academic_year": m.course.academicYear,
                "n_score_1": n1,
                "n_score_2": n2,
                "n_score_3": n3,
                "n_score_4": n4
            })
        scores = pd.DataFrame(rows)

        # Plot the score distributions for each attribute-indicator pair
        self.plot_score_distributions(scores, course_code)

        # Collect metadata for each attribute-indicator pair
        attr_ind_data = {}
        for attr, ind in attr_ind_pairs:
            # Get the measurements for this attribute-indicator pair
            measurements = self.session.query(Measurement).filter(
                Measurement.attribute == attr,
                Measurement.indicator == ind,
                Measurement.courseID.in_(course_ids)
            ).all()

            # Collect metadata
            attr_ind_data[f"{attr}{ind}"] = {}
            for m in measurements:
                # Calculate the fractions of scores
                scores = self.get_scores_by_measurement_id(m.measurementID)
                n1, n2, n3, n4 = scores.count(1), scores.count(2), scores.count(3), scores.count(4)
                n1_frac = n1 / len(scores)
                n2_frac = n2 / len(scores)
                n3_frac = n3 / len(scores)
                n4_frac = n4 / len(scores)

                # Ensure the fractions are within expected ranges
                notes = []
                if not (expected_ranges['1'][0] <= n1_frac <= expected_ranges['1'][1]):
                    notes.append(f"{n1_frac*100:.1f}\% of students received a score of 1 ({score_names['1']}); this is outside the normal range of {expected_ranges['1'][0]*100:.0f}-{expected_ranges['1'][1]*100:.0f}\%.")
                if not (expected_ranges['2'][0] <= n2_frac <= expected_ranges['2'][1]):
                    notes.append(f"{n2_frac*100:.1f}\% of students received a score of 2 ({score_names['2']}); this is outside the normal range of {expected_ranges['2'][0]*100:.0f}-{expected_ranges['2'][1]*100:.0f}\%.")
                if not (expected_ranges['3'][0] <= n3_frac <= expected_ranges['3'][1]):
                    notes.append(f"{n3_frac*100:.1f}\% of students received a score of 3 ({score_names['3']}); this is outside the normal range of {expected_ranges['3'][0]*100:.0f}-{expected_ranges['3'][1]*100:.0f}\%.")
                if not (expected_ranges['4'][0] <= n4_frac <= expected_ranges['4'][1]):
                    notes.append(f"{n4_frac*100:.1f}\% of students received a score of 4 ({score_names['4']}); this is outside the normal range of {expected_ranges['4'][0]*100:.0f}-{expected_ranges['4'][1]*100:.0f}\%.")

                attr_ind_data[f"{attr}{ind}"][m.measurementID] = {
                    "deliverableType": m.deliverableType,
                    "deliverableName": m.deliverableName,
                    "date": m.date.strftime("%Y-%m-%d"),
                    "academicYear": m.course.academicYear,
                    "notes": notes
                }
        print(f"Attribute-Indicator data: {attr_ind_data}")

        # Set up Jinja2 environment for report template
        env = Environment(loader=FileSystemLoader("."))
        template = env.get_template("/assets/instructor_report_template.tex")

        # Render LaTeX with data
        rendered_tex = template.render(course_code=course_code,
                                       academic_year=academic_year,
                                       attr_ind_pairs=attr_ind_pairs,
                                       attr_ind_data=attr_ind_data)
        
        # Save LaTeX output
        file_name = "instructor_report_{}".format(course_code.replace(' ', '_'))
        with open(f"{file_name}.tex", "w") as f:
            f.write(rendered_tex)

        print(f"LaTeX file generated: {file_name}.tex")

        # Compile LaTeX to PDF
        try:
            subprocess.run(["pdflatex", "-interaction=nonstopmode", f"{file_name}.tex"], check=True)
            print(f"PDF generated: {file_name}.pdf")
        except subprocess.CalledProcessError as e:
            print("Error during LaTeX compilation:", e)

    def close(self):
        """Close the database session."""
        self.session.close()