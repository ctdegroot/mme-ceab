import pandas as pd
from pathlib import Path
import warnings
from typing import Union, List, Dict, Optional

class Sheets():
    """Class with static variables for the Excel sheet names."""
    instructor = "1 - Instructor"
    course = "2 - Course"
    measurement = "3 - Measurement"
    data = "4 - Data"


class CEAB():
    """Class used to represent a CEAB measurements database as a set of pandas DataFrames.

    Parameters
    ----------
    data_file : str
        Name of the Excel file containing the CEAB data.
    """

    valid_keys = {
        'course' : ['instructorID', 'prefix', 'number', 'suffix', 'academicYear', 'yearInProgram'],
        'measurement' : ['measurementID', 'courseID',  'attribute', 'indicator',  'deliverableType', 'deliverableName', 'date', 'gradeScale', 'maxScore', 'minPercentScore2', 'minPercentScore3', 'minPercentScore4', 'improvementTheme']
    }

    def __init__(self, data_file=None):
        """Class initialization"""
        self._data = {}

        self._attribute_names = ['instructor', 'course', 'measurement', 'data']

        if data_file:
            # If file is provided, check that it is the correct type and read.
            # Note that the 'data' sheet has a different format so is not read here.
            data_file = Path(data_file)
            if data_file.suffix =='.xlsx':
                for attr in self._attribute_names:
                    if attr == 'data':
                        pass
                    else:
                        self._data[attr] = self.read_sheet(data_file, getattr(Sheets, attr))
            else:
                raise TypeError(f'A file with the invalid extension {data_file.suffix}, was passed to the CEAB constructor. \n'
                                f'Creating a CEAB object requires an Excel file with the extension .xlsx')

            # The 'data' table is input into the Excel sheet in wide format for convenience
            # and contains a header row with instructions. It must now be read by skipping the
            # header and converting to long form.
            data = self.read_sheet(data_file, getattr(Sheets, 'data'), skiprows=1)
            self._data['data'] = pd.melt(data, 
                id_vars=['studentID'], 
                value_vars=data.columns.tolist().remove('studentID'), 
                var_name='measurementID')

            # Drop NaNs and zeros from the 'data' table
            self._data['data'] = self._data['data'].dropna()
            self._data['data'] = self._data['data'][self._data['data']['value'] != 0]

            # Create a column for the 'dataID'
            self._data['data'].insert(0, 'dataID', self._data['data'].index)

            # For any measurements using 'Raw Scores (Standard Bins)', convert to CEAB scale
            measurements = self.get_row_IDs_matching_criteria('measurement', {'gradeScale' : 'Raw Scores (Standard Bins)'})
            

        else:
            # If no file is provided, create an empty DataFrame for each class attribute.
            for attr in self._attribute_names:
                self._data[attr] = pd.DataFrame()     

    def read_sheet(self, file_name, sheet_name, skiprows=None):
        """Read a sheet from an Excel file.

        Parameters
        ----------
        file_name : str, Path
            Name of the Excel file.
        sheet_name : str
            Name of the sheet to be read.

        Returns
        -------
        pandas.DataFrame
            The data contained in the specified Excel sheet.
        """
        warnings.simplefilter(action='ignore', category=UserWarning)
        return pd.read_excel(file_name, sheet_name, skiprows=skiprows)

    @property
    def instructor(self):
        """Get the 'intructor' table.

        Returns
        -------
        pandas.DataFrame
            The 'instructor' table, which contains information about course instructors.
        """
        return self._data['instructor']

    @property
    def course(self):
        """Get the 'course' table.

        Returns
        -------
        pandas.DataFrame
            The 'course' table, which contains information about course offerings.
        """
        return self._data['course']

    @property
    def measurement(self):
        """Get the 'measurement' table.

        Returns
        -------
        pandas.DataFrame
            The 'measurement' table, which contains information about the measurements taken
            for particular courses.
        """
        return self._data['measurement']

    @property
    def data(self):
        """Get the 'data' table.

        Returns
        -------
        pandas.DataFrame
            The 'data' table, which contains measurement data.
        """
        return self._data['data']

    @staticmethod
    def combine(first, second):
        """Combine the tables from two CEAB objects.

        Parameters
        ----------
        first : CEAB
            First CEAB object.
        second : CEAB
            Second CEAB object.

        Returns
        -------
        CEAB
            CEAB object containing tables combined from the two input arguments.
        """
        new = CEAB()
        for attr in new._attribute_names:
            new._data[attr] = pd.concat([getattr(first, attr), getattr(second, attr)])
            new._data[attr].drop_duplicates(subset=new._data[attr].columns[0], keep='first', inplace=True)
        return new

    def __add__(self, other):
        """Add two CEAB objects together"""
        return CEAB.combine(self, other)

    def get_row_IDs_matching_criteria(self, table_name, criteria):
        """Get the row IDs matching specific attributes for a given data table.

        Parameters
        ----------
        table_name : str
            Name of the table from which to extract data.
        criteria : dict
            Dictionary of attributes and the values to select.

        Returns
        -------
        list
            List of IDs that match the specified criteria.
        """
        # Check that the type of 'table_name' is 'str'
        if type(table_name) is not str:
            raise TypeError('A table_name of type {} was passed to function get_from_table while the type str was expected.'.format(type(criteria)))

        # Check that the type of 'criteria' is 'dict'
        if type(criteria) is not dict:
            raise TypeError('A criteria of type {} was passed to function get_from_table while the type dict was expected.'.format(type(criteria)))

        # Get the data table by name and filter by the criteria
        df = getattr(self, table_name)
        for key, value in criteria.items():
            if key not in CEAB.valid_keys[table_name]:
                raise ValueError('Invalid key {} given in function get_from_table.'.format(key))
            df = df[df[key] == value]

        # Return
        return df['{}ID'.format(table_name)].tolist()
