from sqlalchemy.orm import Session
from ceab.database import get_session
from ceab.models import Instructor, Course, Measurement, Data
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import openpyxl
import subprocess
import shutil
from datetime import datetime
import os
import re

def escape_latex(text):
    if not isinstance(text, str):
        return text
    replacements = {
        '\\': r'\textbackslash{}',
        '{': r'\{',
        '}': r'\}',
        '$': r'\$',
        '&': r'\&',
        '#': r'\#',
        '%': r'\%',
        '_': r'\_',
        '~': r'\textasciitilde{}',
        '^': r'\textasciicircum{}',
    }
    return re.sub(r'([\\{}$&#%_^~])', lambda m: replacements[m.group()], text)

all_attributes = {
    "KB"  : [1, 2, 3, 4],
    "PA"  : [1, 2, 3],
    "I"   : [1, 2, 3],
    "DES" : [1, 2, 3, 4],
    "ET"  : [1, 2, 3],
    "ITW" : [1, 2, 3],
    "CS"  : [1, 2, 3],
    "PR"  : [1, 2, 3],
    "IES" : [1, 2, 3],
    "EE"  : [1, 2, 3],
    "EPM" : [1, 2, 3, 4],
    "LL"  : [1, 2]
}

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

attribute_names = {
    "KB"  : "Knowledge Base",
    "PA"  : "Problem Analysis",
    "I"   : "Investigation",
    "DES" : "Design",
    "ET"  : "Use of Engineering Tools",
    "ITW" : "Individual and Team Work",
    "CS"  : "Communication Skills",
    "PR"  : "Professionalism",
    "IES" : "Impact of Engineering on Society and the Environment",
    "EE"  : "Ethics and Equity",
    "EPM" : "Economics and Project Management",
    "LL"  : "Lifelong Learning"
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
    
    def get_courses_by_academic_year(self, academic_year: str) -> list:
        """Get course codes for a specific academic year.

        Parameters
        ----------
        academic_year : str
            The academic year to filter by (e.g., '2023/24').

        Returns
        -------
        list
            List of course codes with measurements for the specified academic year.
        """
        # Get all of the courses that have measurement in the specified academic year.
        courses = self.session.query(Course.prefix, Course.number, Course.suffix) \
            .filter(Course.academicYear == academic_year).all()

        # Build and return formatted course codes
        course_codes = [
            f"{prefix.strip()} {number}{suffix.strip() if suffix.strip() != 'none' else ''}"
            for prefix, number, suffix in courses
        ]

        return course_codes
    
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

    def get_score_distribution_by_measurement_id(self, measurement_id: str, normalize: bool = False) -> dict:
        """Get the distribution of scores for a specific measurement ID.

        Parameters
        ----------
        measurement_id : str
            The measurement ID to fetch the score distribution for.
        normalize : bool, optional
            Whether to normalize the scores to fractions (default is False).

        Returns
        -------
        tuple
            Tuple with counts (or fractions, if normalize=True) of each score (1-4) for the specified measurement ID.
        """
        scores = self.get_scores_by_measurement_id(measurement_id)
        if not scores:
            return {'1': 0, '2': 0, '3': 0, '4': 0}
        total_scores = len(scores)
        return (
                scores.count(1) if not normalize else scores.count(1) / total_scores,
                scores.count(2) if not normalize else scores.count(2) / total_scores,
                scores.count(3) if not normalize else scores.count(3) / total_scores,
                scores.count(4) if not normalize else scores.count(4) / total_scores
            )

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
    
    def get_aggregate_scores_by_year(self, attribute: str, indicator: int, 
                                     academic_year: str, year_in_program: int) -> list:
        """Get aggregate score distributions for a specific attribute and indicator, grouped by academic year.

        Parameters
        ----------
        attribute : str
            The attribute to filter by (e.g., 'KB', 'PA').
        indicator : int
            The indicator to filter by (e.g., 1, 2, 3).
        academic_year : str
            The academic year to filter by (e.g., '2023/24').
        year_in_program : int
            The year in program to filter by (e.g., 1, 2, 3, 4).

        Returns
        -------
        list
            List containing the fractional distribution of scores (1-4) for the specified attribute and indicator,
            grouped by academic year.
        """
        # Query measurements matching the attribute and indicator
        measurements = self.session.query(Measurement).filter(
            Measurement.attribute == attribute,
            Measurement.indicator == indicator,
            Measurement.course.has(Course.academicYear == academic_year),
            Measurement.course.has(Course.yearInProgram == year_in_program)
        ).all()

        if not measurements:
            return None
        
        # Aggregate scores for each measurement
        score_distribution = {
            '1': 0,
            '2': 0,
            '3': 0,
            '4': 0
        }
        for measurement in measurements:
            scores = self.get_scores_by_measurement_id(measurement.measurementID)
            score_distribution['1'] += scores.count(1)
            score_distribution['2'] += scores.count(2)
            score_distribution['3'] += scores.count(3)
            score_distribution['4'] += scores.count(4)

        # Convert counts to fractions
        total_scores = sum(score_distribution.values())
        if total_scores == 0:
            return {'1': 0, '2': 0, '3': 0, '4': 0
            # Should print some kind of warning here
        }
        score_distribution['1'] /= total_scores
        score_distribution['2'] /= total_scores
        score_distribution['3'] /= total_scores
        score_distribution['4'] /= total_scores

        return score_distribution
    
    def plot_aggregate_scores(self, academic_year: str, destination: str = ".") -> None:
        """Plot aggregate scores for all attributes and indicators, grouped by academic year.

        Parameters
        ----------
        academic_year : str
            The academic year to filter by (e.g., '2023/24').
        destination : str
            The directory where the plot will be saved. Defaults to the current directory.
        """
        attr_ind_pairs = [(attr, ind) for attr, inds in all_attributes.items() for ind in inds]
        pair_labels = [f"{attr}{ind}" for attr, ind in attr_ind_pairs]
        x = np.arange(len(attr_ind_pairs))

        score_labels = ['1', '2', '3', '4']
        colors = colors = ['#4D4D4D', '#969696', '#92C5DE', '#0571B0']  # Colourblind-safe gradient

        fig, axes = plt.subplots(4, 1, figsize=(len(attr_ind_pairs) * 0.6, 8), sharex=True)
        fig.subplots_adjust(hspace=0.4)

        # Add invisible dummy bars to ensure legend is always correct
        for i, score in enumerate(score_labels):
            axes[0].bar(0, 0, color=colors[i], label=f"{score} - {score_names[score]}")

        for year_in_program in range(1, 5):
            ax = axes[year_in_program - 1]

            valid_x = []
            all_heights = {score: [] for score in score_labels}

            for j, (attr, ind) in enumerate(attr_ind_pairs):
                dist = self.get_aggregate_scores_by_year(attr, ind, academic_year, year_in_program)
                if dist is not None:
                    valid_x.append(j)
                    for score in score_labels:
                        all_heights[score].append(dist[score])

            # Now plot stacked bars for valid entries only
            bottoms = np.zeros(len(valid_x))
            for i, score in enumerate(score_labels):
                heights = all_heights[score]
                ax.bar(valid_x, heights, bottom=bottoms, color=colors[i], label=f"Score {score}" if year_in_program == 4 else None)
                bottoms += heights

            ax.set_ylabel(f"Year {year_in_program}", rotation=90, labelpad=30, va='center', fontsize=14)
            ax.set_ylim(0, 1)
            ax.set_yticks([0, 0.25, 0.5, 0.75, 1])
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            ax.tick_params(axis='x', which='both', bottom=False, labelbottom=(year_in_program == 4))

        axes[-1].set_xticks(x)
        axes[-1].set_xticklabels(pair_labels, rotation=45, ha='center', fontsize=14, fontweight='bold')
        axes[-1].set_xlim(-0.5, len(attr_ind_pairs) - 0.5)

        fig.suptitle(f"Score Distributions by Year in Program for Academic Year {academic_year}", fontsize=18)
        
        fig.legend(
            labels=[f"{s} - {score_names[s]}" for s in score_labels],
            loc='lower center',
            ncol=4,
            bbox_to_anchor=(0.5, -0.05),
            frameon=True,
            prop={'size': 14, 'weight': 'bold'}
        )
        plt.tight_layout()

        plt.savefig(f"{destination}/aggregate_scores_{academic_year.replace('/', '_')}.png", dpi=300, bbox_inches='tight')
        plt.close()

    def get_measurement_ids_by_indicator(self, attribute: str, indicator: int,
                                         year_in_program: int, academic_year: str) -> list:
        """Get measurement IDs for a specific attribute and indicator, filtered by academic year.

        Parameters
        ----------
        attribute : str
            The attribute to filter by (e.g., 'KB', 'PA').
        indicator : int
            The indicator to filter by (e.g., 1, 2, 3).
        year_in_program : int
            The year in program to filter by (e.g., 1, 2, 3, 4).
        academic_year : str
            The academic year to filter by (e.g., '2023/24').

        Returns
        -------
        list
            List of measurement IDs that match the specified criteria.
        """
        measurements = self.session.query(Measurement).filter(
            Measurement.attribute == attribute,
            Measurement.indicator == indicator,
            Measurement.course.has(Course.yearInProgram == year_in_program),
            Measurement.course.has(Course.academicYear == academic_year)
        ).all()

        return [m.measurementID for m in measurements]

    def plot_score_distribution(self, course_code: str, academic_year: str, attr: str, ind: int, show_expected_range: bool = False) -> str:
        """
        Plots a score distribution as fractions for a given course, attribute, and indicator, up to a given academic year.

        Parameters
        ----------
        course_code : str
            The course code to filter by (e.g., 'CEAB 123').
        academic_year : str
            The academic year to filter by (e.g., '2023/24').
        attr : str
            The attribute to filter by (e.g., 'KB', 'PA').
        ind : int
            The indicator to filter by (e.g., 1, 2, 3).
        show_expected_range : bool, optional
            Whether to show the expected ranges for each score as translucent rectangles (default is False).

        Returns
        -------
        str
            The the filename of the saved plot image.
  
        """
        # Define a colourblind-safe colour scheme for the scores
        COLORS = ['#332288', '#88CCEE', '#44AA99', '#117733']

        # Get the parts of the course code.
        prefix, number, suffix = self.get_course_prefix_number_suffix(course_code)

        # Get all of the measurements for the specified course code, attribute, and indicator.
        measurements = self.session.query(Measurement).filter(
            Measurement.course.has(Course.prefix == prefix),
            Measurement.course.has(Course.number == number),
            Measurement.course.has(Course.suffix == suffix),
            Measurement.attribute == attr,
            Measurement.indicator == ind
        ).all()
        
        # If no measurements are found, print a message and return.
        if not measurements:
            print(f"No measurements found for course {course_code}, attribute {attr}, indicator {ind}.")
            return

        # Get the scores and metadata and convert to a DataFrame
        rows = []
        for m in measurements:
            n1, n2, n3, n4 = self.get_score_distribution_by_measurement_id(m.measurementID, normalize=True)
            rows.append({
                "attribute": m.attribute,
                "indicator": m.indicator,
                "academic_year": m.course.academicYear,
                "frac_score_1": n1,
                "frac_score_2": n2,
                "frac_score_3": n3,
                "frac_score_4": n4
            })
        scores = pd.DataFrame(rows)

        # Extract and sort academic years by starting year
        scores = scores.assign(
            year_start=scores['academic_year'].str.extract(r'^(\d{4})').astype(int)
        ).sort_values(by='year_start')

        # Keep only those scores that are in the specified academic year or sooner
        year_limit = int(re.match(r'^(\d{4})', academic_year).group(1))
        scores = scores[scores['year_start'] <= year_limit]

        # Keep only the 4 most recent unique academic years in the data remaining
        scores = scores[scores['year_start'] > (year_limit - 4)]

        # Get the academic years in the data remaining
        recent_years = scores['academic_year'].unique()

        # Create color map, mapping each year to a color
        # Start at the end of the COLORS list to ensure the most recent year gets the last color
        color_map = {
            year: COLORS[i] for i, year in enumerate(reversed(recent_years))
        }

        # Set up the data for plotting
        score_labels = ['1', '2', '3', '4']
        bar_width = 0.2
        x = np.arange(len(score_labels))
        num_years = len(recent_years)
        group_width = num_years * bar_width
        tick_positions = x + (group_width - bar_width) / 2

        # Create the figure
        plt.figure(figsize=(8, 5))

        # Draw expected ranges as translucent rectangles, if requested
        if show_expected_range:
            for i, label in enumerate(score_labels):
                low, high = expected_ranges[label]
                plt.axhspan(
                    low, high,
                    xmin=(i + 0.05) / len(score_labels),
                    xmax=(i + 0.95) / len(score_labels),
                    color='gray', alpha=0.15, zorder=0
                )

        # Plot the score distributions for each year
        for i, year in enumerate(recent_years):
            year_data = scores[scores['academic_year'] == year]
            # Get the fractions for each score (if multiple mesurements, they are averaged)
            fracs = year_data[['frac_score_1', 'frac_score_2', 'frac_score_3', 'frac_score_4']].sum().tolist()
            total = sum(fracs) or 1
            fracs = [c / total for c in fracs]
            plt.bar(
                x + i * bar_width,
                fracs,
                width=bar_width,
                color=color_map[year],
                label=year
            )

        plt.xticks(tick_positions, score_labels)
        plt.xlabel("Score")
        plt.ylabel("Fraction of Students")
        plt.ylim(0, 1)
        plt.legend(title="Academic Year")
        plt.tight_layout()
        file_name = f"{course_code.replace(' ', '_')}_{attr}{ind}.png"
        plt.savefig(file_name)
        plt.close()

        return file_name

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
        # Define a colourblind-safe colour scheme for the scores
        COLORS = ['#332288', '#88CCEE', '#44AA99', '#117733']  # Blue, light blue, teal, green

        # Group by attribute-indicator combinations
        unique_combos = score_df[['attribute', 'indicator']].drop_duplicates()

        for _, row in unique_combos.iterrows():
            attr = row['attribute']
            ind = row['indicator']

            subset = score_df[(score_df['attribute'] == attr) & (score_df['indicator'] == ind)]

            # Extract and sort academic years by starting year
            subset = subset.assign(
                year_start=subset['academic_year'].str.extract(r'^(\d{4})').astype(int)
            ).sort_values(by='year_start')

            # Keep only the 4 most recent academic years
            # Step 1: Add a numeric year key
            subset = subset.assign(
                year_start=subset['academic_year'].str.extract(r'^(\d{4})').astype(int)
            )

            # Step 2: Get the 4 most recent unique academic years
            recent_years = (
                subset[['academic_year', 'year_start']]
                .drop_duplicates()
                .sort_values('year_start', ascending=False)
                .head(4)
                .sort_values('year_start')  # for left-to-right plotting
            )['academic_year'].tolist()

            # Step 3: Filter the full dataset to those 4 years
            subset = subset[subset['academic_year'].isin(recent_years)]

            # Step 4: Create color map
            color_map = {
                year: COLORS[i] for i, year in enumerate(recent_years)
            }

            score_labels = ['1', '2', '3', '4']
            bar_width = 0.2
            x = range(len(score_labels))

            plt.figure(figsize=(8, 5))

            # Draw expected ranges as translucent rectangles
            for i, label in enumerate(score_labels):
                low, high = expected_ranges[label]
                plt.axhspan(
                    low, high,
                    xmin=(i + 0.05) / len(score_labels),
                    xmax=(i + 0.95) / len(score_labels),
                    color='gray', alpha=0.15, zorder=0
                )

            for i, year in enumerate(recent_years):
                year_data = subset[subset['academic_year'] == year]
                counts = year_data[['n_score_1', 'n_score_2', 'n_score_3', 'n_score_4']].sum().tolist()
                total = sum(counts) or 1
                fractions = [c / total for c in counts]
                plt.bar(
                    [pos + i * bar_width for pos in x],
                    fractions,
                    width=bar_width,
                    color=color_map[year],
                    label=year
                )

            num_years = len(recent_years)
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

    def get_course_prefix_number_suffix(self, course_code: str) -> tuple:
        """Extract the prefix, number, and suffix from a course code.

        Parameters
        ----------
        course_code : str
            The course code to extract from, in the format "XYZ 1234A".

        Returns
        -------
        tuple
            A tuple containing the prefix (str), number (int), and suffix (str).
        """
        # Generate the course prefix, number, and suffix from the course code.
        # The course code must be in the format "XYZ 1234A" where XYZ is the prefix, 
        # 1234 is the number, and A is the suffix.
        parts = course_code.split()
        if len(parts) != 2:
            raise ValueError("Invalid course code format. Expected format: 'XYZ 1234A'.")
        
        prefix = parts[0]
        number = int(parts[1][0:4] if len(parts[1]) > 4 else parts[1])
        suffix = parts[1][4:] if len(parts[1]) > 4 else "none"
        
        return prefix, number, suffix
    
    def plot_course_scores(self, course_code: str, academic_year: str) -> None:
        """Plot the score distributions for a specific course code and academic year.

        Parameters
        ----------
        course_code : str
            The course code to plot scores for, in the format "XYZ 1234A".
        academic_year : str
            The academic year to filter the measurements by, in the format "2023/24".
        """
        prefix, number, suffix = self.get_course_prefix_number_suffix(course_code)

        # Get all of the courseIDs that match the prefix, number, and suffix.
        course_ids = self.get_row_IDs_matching_criteria("course", {"prefix": prefix, "number": number, "suffix": suffix})
        if not course_ids:
            raise ValueError(f"No course found with code: {course_code}")
        
        # Get all of the measurement data that match the courseIDs.
        measurements = self.session.query(Measurement).filter(Measurement.courseID.in_(course_ids)).all()

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

        # Sort the data by academic year (which is a string like '2023/24')
        scores = scores.assign(
            sort_key=scores['academic_year'].str.extract(r'^(\d{4})').astype(int)
        ).sort_values(by='sort_key').drop(columns='sort_key')

        # Plot the score distributions for each attribute-indicator pair
        self.plot_score_distributions(scores, course_code)

    def get_course_report_data(self, course_code: str, academic_year: str) -> tuple:
        """Get the data needed to generate a course report for a specific course code and academic year.
        
        Parameters
        ----------
        course_code : str
            The course code to generate the report for, in the format "XYZ 1234A".
        academic_year : str
            The academic year to filter the measurements by, in the format "2023/24".
        
        Returns
        -------
        attr_ind_pairs : list of tuple of (str, str)
            A list of (attribute, indicator) pairs that were measured in the specified academic year.

        attr_ind_data : dict of {str: dict of {int: dict}}
            A nested dictionary containing metadata for each measurement.
            The outer key is the attribute-indicator code (e.g., 'EE3').
            The inner keys are measurementIDs.
            The innermost dictionary contains metadata for each measurement (e.g., deliverable type, name, date, instructor, and notes).
        """
        prefix, number, suffix = self.get_course_prefix_number_suffix(course_code)

        # Get all of the courseIDs that match the prefix, number, and suffix.
        course_ids = self.get_row_IDs_matching_criteria("course", {"prefix": prefix, "number": number, "suffix": suffix})
        if not course_ids:
            raise ValueError(f"No course found with code: {course_code}")
        
        # Get all of the measurement data that match the courseIDs.
        measurements = self.session.query(Measurement).filter(Measurement.courseID.in_(course_ids)).all()

        # Get all of the unique combinations of attribute and indicator in the measurements.
        attr_ind_pairs = sorted({(m.attribute, m.indicator) for m in measurements})

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
                if len(scores) > 0:
                    n1_frac = n1 / len(scores)
                    n2_frac = n2 / len(scores)
                    n3_frac = n3 / len(scores)
                    n4_frac = n4 / len(scores)
                else:
                    n1_frac = n2_frac = n3_frac = n4_frac = 0.0

                # Ensure the fractions are within expected ranges for measurements in the specified academic year
                notes = []
                if m.course.academicYear == academic_year:
                    if not (expected_ranges['1'][0] <= n1_frac <= expected_ranges['1'][1]):
                        notes.append(f"{n1_frac*100:.1f}% of students received a score of 1 ({score_names['1']}) on ``{m.deliverableName}''; this is outside the normal range of {expected_ranges['1'][0]*100:.0f}-{expected_ranges['1'][1]*100:.0f}%.")
                    if not (expected_ranges['2'][0] <= n2_frac <= expected_ranges['2'][1]):
                        notes.append(f"{n2_frac*100:.1f}% of students received a score of 2 ({score_names['2']}) on ``{m.deliverableName}''; this is outside the normal range of {expected_ranges['2'][0]*100:.0f}-{expected_ranges['2'][1]*100:.0f}%.")
                    if not (expected_ranges['3'][0] <= n3_frac <= expected_ranges['3'][1]):
                        notes.append(f"{n3_frac*100:.1f}% of students received a score of 3 ({score_names['3']}) on ``{m.deliverableName}''; this is outside the normal range of {expected_ranges['3'][0]*100:.0f}-{expected_ranges['3'][1]*100:.0f}%.")
                    if not (expected_ranges['4'][0] <= n4_frac <= expected_ranges['4'][1]):
                        notes.append(f"{n4_frac*100:.1f}% of students received a score of 4 ({score_names['4']}) on ``{m.deliverableName}''; this is outside the normal range of {expected_ranges['4'][0]*100:.0f}-{expected_ranges['4'][1]*100:.0f}%.")

                attr_ind_data[f"{attr}{ind}"][m.measurementID] = {
                    "deliverableType": m.deliverableType,
                    "deliverableName": m.deliverableName,
                    "date": m.date.strftime("%Y-%m-%d"),
                    "academicYear": m.course.academicYear,
                    "instructor": f"{m.course.instructor.firstName} {m.course.instructor.lastName}",
                    "notes": notes
                }

                # Sort the data by academic year, which is a string like '2023/24'.
                attr_ind_data[f"{attr}{ind}"] = dict(
                    sorted(
                        attr_ind_data[f"{attr}{ind}"].items(),
                        key=lambda item: int(item[1]['academicYear'].split('/')[0])
                    )
                )

        # Filter out attribute-indicator pairs that were not measured in the specified academic year.
        attr_ind_data = {
            k: v for k, v in attr_ind_data.items()
            if any(d["academicYear"] == academic_year for d in v.values())
        }

        # Also update attr_ind_pairs to remove those that were filtered out above.
        attr_ind_pairs = [
            (attr, ind) for (attr, ind) in attr_ind_pairs
            if f"{attr}{ind}" in attr_ind_data
        ]

        return attr_ind_pairs, attr_ind_data

    def generate_course_report(self, course_code: str, academic_year: str) -> str:
        """Generate a report for a specific course.

        Parameters
        ----------
        course_code : str
            The course code to generate the report for.
        academic_year : str
            The academic year for which the report is generated.

        Returns
        -------
        str
            The file name of the generated PDF report.
        """
        # Get the data needed for the course report.
        attr_ind_pairs, attr_ind_data = self.get_course_report_data(course_code, academic_year)

        # Generate the plots required for the report.
        self.plot_course_scores(course_code, academic_year)

        # Set up Jinja2 environment for report template
        env = Environment(loader=FileSystemLoader("."))
        env.filters['escape_latex'] = escape_latex
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

        # Compile LaTeX to PDF (do twice to ensure all references are resolved)
        for _ in range(2):
            try:
                result = subprocess.run(
                    ["pdflatex", "-interaction=nonstopmode", f"{file_name}.tex"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                print(f"❌ LaTeX compilation failed for {file_name}.tex:")
                print(e.stderr.decode(errors='ignore'))
                return None
            
        # Remove the auxiliary files generated by LaTeX
        for ext in ['.aux', '.log', '.out', '.tex']:
            path = f"{file_name}{ext}"
            try:
                os.remove(path)
            except FileNotFoundError:
                # No problem — file just doesn't exist
                pass
            except Exception as e:
                print(f"Error removing {path}:", e)

        # Remove the generated .png files from the score distributions
        for attr, ind in attr_ind_pairs:
            png_file = f"{course_code.replace(' ', '_')}_{attr}{ind}.png"
            try:
                os.remove(png_file)
            except FileNotFoundError:
                # No problem — file just doesn't exist
                pass
            except Exception as e:
                print(f"Error removing {png_file}:", e)

        # Return the file name of the generated PDF report
        return f"{file_name}.pdf"
    
    def get_graduate_attribute_report_data(self, attribute: str, academic_year: str) -> tuple:
        """Get the data needed to generate a report for a graduate attribute and academic year.
        
        Parameters
        ----------
        attribute : str
            The graduate attribute abbreviation for which the report is generated.
        academic_year : str
            The academic year to filter the measurements by, in the format "2023/24".
        
        Returns
        -------
        measurement_data : dict
            A dictionary containing the metadata for each measurement.
            The keys are the indicator names, and the values are dictionaries with metadata.
        """
        measurement_data = {}

        # Loop through all of the indicators for the given attribute.
        for ind in all_attributes[attribute]:            
            # Get all of the measurements that match the graduate attribute and indicator for the academic year.
            measurements = self.session.query(Measurement).filter(
                Measurement.attribute == attribute,
                Measurement.indicator == ind,
                Measurement.course.has(Course.academicYear == academic_year)
            ).all()

            # If there are no measurements for this attribute and indicator, insert "None" and skip to the next one.
            if not measurements:
                measurement_data[ind] = None
                continue

            # Collect the metadata for each measurement.
            measurement_data[ind] = {}
            for m in measurements:   
                measurement_data[ind][m.measurementID] = {
                    "courseCode": f"{m.course.prefix.strip()} {m.course.number}{m.course.suffix.strip() if m.course.suffix.strip() != 'none' else ''}",
                    "instructor": f"{m.course.instructor.firstName} {m.course.instructor.lastName}",
                    "academicYear": m.course.academicYear,
                    "deliverableType": m.deliverableType,
                    "deliverableName": m.deliverableName,
                    "date": m.date.strftime("%Y-%m-%d")
                }

        return measurement_data

    def generate_graduate_attribute_report(self, attribute: str, academic_year: str) -> str:
        """Generate a report for a specific graduate attribute.

        Parameters
        ----------
        attribute : str
            The graduate attribute abbreviation for which the report is generated.
        academic_year : str
            The academic year for which the report is generated.

        Returns
        -------
        str
            The file name of the generated PDF report.
        """
        # Get the data needed for the course report.
        measurement_data = self.get_graduate_attribute_report_data(attribute, academic_year)

        # Generate the plots required for the report.
        plot_files = []
        for ind, data in measurement_data.items():
            # Check if there are no measurements for this attribute and indicator.
            # If so, print a message and skip to the next one.
            if data is None:
                print(f"No measurements found for attribute {attribute}, indicator {ind} in academic year {academic_year}.")
                continue

            # Get all of the unique code codes from the data.
            unique_course_codes = set(d['courseCode'] for d in data.values())

            # Loop through each course code and generate the score distribution plot.
            for course_code in unique_course_codes:
                file = self.plot_score_distribution(course_code, academic_year, attribute, ind, show_expected_range=True)
                plot_files.append(file)

        # Set up Jinja2 environment for report template
        env = Environment(loader=FileSystemLoader("."))
        env.filters['escape_latex'] = escape_latex
        template = env.get_template("/assets/graduate_attribute_report_template.tex")

        # Render LaTeX with data
        rendered_tex = template.render(attribute=attribute,
                                       attribute_name=attribute_names[attribute],
                                       academic_year=academic_year,
                                       measurement_data=measurement_data)
        
        # Save LaTeX output
        file_name = "graduate_attribute_report_{}".format(attribute)
        with open(f"{file_name}.tex", "w") as f:
            f.write(rendered_tex)

        # Compile LaTeX to PDF (do twice to ensure all references are resolved)
        for _ in range(2):
            try:
                result = subprocess.run(
                    ["pdflatex", "-interaction=nonstopmode", f"{file_name}.tex"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                print(f"❌ LaTeX compilation failed for {file_name}.tex:")
                print(e.stderr.decode(errors='ignore'))
                return None
            
        # Remove the auxiliary files generated by LaTeX
        for ext in ['.aux', '.log', '.out', '.tex']:
            path = f"{file_name}{ext}"
            try:
                os.remove(path)
            except FileNotFoundError:
                # No problem — file just doesn't exist
                pass
            except Exception as e:
                print(f"Error removing {path}:", e)

        # # Remove the generated .png files from the score distributions
        # for attr, ind in attr_ind_pairs:
        #     png_file = f"{course_code.replace(' ', '_')}_{attr}{ind}.png"
        #     try:
        #         os.remove(png_file)
        #     except FileNotFoundError:
        #         # No problem — file just doesn't exist
        #         pass
        #     except Exception as e:
        #         print(f"Error removing {png_file}:", e)

        # # Return the file name of the generated PDF report
        # return f"{file_name}.pdf"

    def generate_course_feedback_form(self, course_code: str, academic_year: str) -> str:
        """Generate a feedback form for a specific course.

        Parameters
        ----------
        course_code : str
            The course code to generate the report for.
        academic_year : str
            The academic year for which the report is generated.

        Returns
        -------
        str
            The file name of the generated Excel feedback form.
        """
        # Generate file name for the feedback form.
        file_name = f"narrative_{course_code.replace(' ', '_')}.xlsx"

        # Make a copy of the template feedback form.
        shutil.copy("assets/Narrative_Template.xlsx", file_name)

        # Load the template Excel file and access the required sheet.
        wb = openpyxl.load_workbook(file_name)
        ws = wb["Narrative"]

        # Get the course information.
        prefix, number, suffix = self.get_course_prefix_number_suffix(course_code)
        course_id = self.get_row_IDs_matching_criteria(
            "course",
            {"prefix": prefix, "number": number, "suffix": suffix, "academicYear": academic_year}
        )[0]
        instructor_first_name = self.session.query(Instructor.firstName).join(Course).filter(Course.courseID == course_id).scalar()
        instructor_last_name = self.session.query(Instructor.lastName).join(Course).filter(Course.courseID == course_id).scalar()

        # Input the course information into the template.
        ws["C2"] = instructor_first_name
        ws["C3"] = instructor_last_name
        ws["C4"] = course_code
        ws["C5"] = academic_year

        # Get the data needed for the feedback form.
        attr_ind_pairs, attr_ind_data = self.get_course_report_data(course_code, academic_year)

        # Fill in the feedback form with the attribute-indicator pairs and their metadata.
        row = 11  # Start filling from row 11
        for attr, ind in attr_ind_pairs:
            # Fill in the attribute/indicator pair.
            ws[f"B{row}"] = f"{attr}{ind}"

            # Get the metadata for this attribute-indicator pair
            metadata = attr_ind_data[f"{attr}{ind}"]

            # Generate the observations string for this attribute-indicator pair.
            observations = ""
            for _, data in metadata.items():
                # Only include data for the specified academic year.
                if data["academicYear"] != academic_year:
                    continue

                # Record the observations for this measurement.
                observations += f'- {data["deliverableName"]}\n'
                for note in data["notes"]:
                    observations += f'    - {note}\n'
                if not data["notes"]:
                    observations += "    - No potential issues noted.\n"

            # Add the notes to the Excel sheet.
            ws[f"C{row}"] = observations.replace("``", '"').replace("''", '"')

            # Set wrap text in notes box.
            ws[f"C{row}"].alignment = openpyxl.styles.Alignment(wrap_text=True, vertical="top")

            # Increment the row for the next attribute-indicator pair.
            row += 1

        # Save the workbook.
        wb.save(file_name)

        # Return the file name of the generated feedback form.
        return file_name

    def close(self):
        """Close the database session."""
        self.session.close()