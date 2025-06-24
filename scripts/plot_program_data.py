import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
from ceab import CEAB

def plot_program_data(academic_year: str):
    """Plot program data for a given academic year.

    Parameters
    ----------
    academic_year : str
        The academic year to plot data for, e.g., "2023-2024".
    """
    ceab = CEAB()
    ceab.plot_aggregate_scores(academic_year)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot program data for a given academic year.")
    parser.add_argument("academic_year", type=str, help="The academic year to plot data for, e.g., '2023-2024'.")
    args = parser.parse_args()

    plot_program_data(args.academic_year)