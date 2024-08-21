import pandas as pd
from pathlib import Path
import copy
import os
import re
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

    valid_keys_in = {
        'instructor' : ['instructorID', 'firstName', 'lastName'],
        'course' : ['courseID', 'instructorID', 'prefix', 'number', 'suffix', 'academicYear', 'yearInProgram'],
        'measurement' : ['measurementID', 'courseID',  'attribute', 'indicator',  'deliverableType', 'deliverableName', 'date', 'gradeScale', 'maxScore', 'minPercentScore2', 'minPercentScore3', 'minPercentScore4', 'improvementTheme'],
        'data' : ['dataID', 'studentID', 'measurementID', 'value']
    }

    def __init__(self, data_file=None):
        """Class initialization"""
        self._data = {}

        self._attribute_names = ['instructor', 'course', 'measurement', 'data']

        if data_file:
            # If file is provided, check that it is the correct type and read.
            # Note that the 'data' sheet has a different format so is not read here.
            data_file = Path(data_file)
            if data_file.suffix == '.xlsx':
                for attr in self._attribute_names:
                    if attr == 'data':
                        pass
                    else:
                        # Try to read the sheet; if it doesn't exist, raise an error
                        try:
                            self._data[attr] = self.read_sheet(data_file, getattr(Sheets, attr))
                        except ValueError:
                            raise ValueError(f'The sheet {getattr(Sheets, attr)} was not found in the Excel file {data_file}.')
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

            # Check that all columns exist and that no extra columns are added
            for attr in self._attribute_names:
                self.check_columns(attr)

            # Define the columns where NaNs are not allowed
            no_nan_columns = copy.deepcopy(self.valid_keys_in)
            for col in ['maxScore', 'minPercentScore2', 'minPercentScore3', 'minPercentScore4', 'improvementTheme']:
                no_nan_columns['measurement'].remove(col)

            # Check if there are any NaNs where there shouldn't be and provide a warning if there are
            for table, columns in no_nan_columns.items():
                for column in columns:
                    if self._data[table][column].isnull().values.any():
                        # Get the rows where the NaNs occur
                        rows = self._data[table][self._data[table][column].isnull()]
                        for index, row in rows.iterrows():
                            tableID = row[f'{table}ID']
                            warnings.warn(f'NaN found in column \'{column}\' of table \'{table}\'.\n'
                                          f'   The value of {table}ID is {tableID}.\n'
                                          f'   The data file is {data_file}.')
    
            # For any measurements using 'CEAB (1-4)' scale, round the data to the nearest integer
            measurements = self.get_row_IDs_matching_criteria('measurement', {'gradeScale' : 'CEAB (1-4)'})
            for measurement in measurements:
                self._data['data'].loc[self._data['data']['measurementID'] == measurement, 'value'] = self._data['data'].loc[self._data['data']['measurementID'] == measurement, 'value'].round().astype(int)

            # For any measurements using 'Raw Scores (Standard Bins)', convert to CEAB scale
            measurements = self.get_row_IDs_matching_criteria('measurement', {'gradeScale' : 'Raw Scores (Standard Bins)'})
            for measurement in measurements:
                # Check that the maxScore is provided
                row = self.measurement.loc[self.measurement['measurementID'] == measurement].iloc[0]
                if pd.isna(row['maxScore']):
                    raise ValueError(f'The maxScore is missing for measurement {measurement}.')

                # Set the bins and the corresponding scores
                bins = [0, 50, 60, 85, 100]
                scores = [1, 2, 3, 4]
                maxScore = row['maxScore']

                # Bin the data
                self._data['data'].loc[self._data['data']['measurementID'] == measurement, 'value'] = pd.cut(
                    self._data['data'].loc[self._data['data']['measurementID'] == measurement, 'value']/maxScore*100,
                    bins=bins,
                    labels=scores,
                    include_lowest=True
                ).astype(int)

            # For any measurements using 'Raw Scores (Custom Bins)', convert to CEAB scale
            measurements = self.get_row_IDs_matching_criteria('measurement', {'gradeScale' : 'Raw Scores (Custom Bins)'})
            for measurement in measurements:
                required_data = ['maxScore', 'minPercentScore2', 'minPercentScore3', 'minPercentScore4']
                # Check that the required data is provided
                row = self.measurement.loc[self.measurement['measurementID'] == measurement].iloc[0]
                for data in required_data:
                    if pd.isna(row[data]):
                        raise ValueError(f'The maxScore is missing for measurement {measurement}.')

                # Set the bins and the corresponding scores
                bins = [0, row['minPercentScore2'], row['minPercentScore3'], row['minPercentScore4'], 100]
                scores = [1, 2, 3, 4]
                maxScore = row['maxScore']

                # Bin the data
                self._data['data'].loc[self._data['data']['measurementID'] == measurement, 'value'] = pd.cut(
                    self._data['data'].loc[self._data['data']['measurementID'] == measurement, 'value']/maxScore*100,
                    bins=bins,
                    labels=scores,
                    include_lowest=True
                ).astype(int)


            # Check that all data are integers between 1 and 4
            out_of_range_values = self._data['data'][(self._data['data']['value'] < 1) | (self._data['data']['value'] > 4)]
            if not out_of_range_values.empty:
                # Get the rows where the out of range values occur
                for index, row in out_of_range_values.iterrows():
                    dataID = row['dataID']
                    measurementID = row['measurementID']
                    value = row['value']
                    warnings.warn(f'Value {value} out of range in dataID \'{dataID}\' for measurementID \'{measurementID}\'.\n'
                                  f'   The data file is {data_file}.')

            # Now that all data are converted, the columns relating to custom scales can be removed
            self._data['measurement'] = self._data['measurement'].drop(columns=['gradeScale', 'maxScore', 'minPercentScore2', 'minPercentScore3', 'minPercentScore4'])
            
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
        df = pd.read_excel(file_name, sheet_name, skiprows=skiprows)
        warnings.simplefilter(action='always', category=UserWarning)
        return df

    def check_columns(self, sheet):
        """Check that the columns in a sheet are valid.

        Parameters
        ----------
        sheet : pandas.DataFrame
            The sheet to be checked.
        """
        df = self._data[sheet]
        valid_columns = self.valid_keys_in[sheet]
        for column in df.columns:
            if column not in valid_columns:
                raise ValueError(f'Invalid column {column} in sheet {sheet}. Valid columns are {valid_columns}.')
        for column in valid_columns:
            if column not in df.columns:
                raise ValueError(f'Missing column {column} in sheet {sheet}. Valid columns are {valid_columns}.')

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
            try:
                df = df[df[key] == value]
            except KeyError:
                raise KeyError('Invalid key {} given in function get_from_table.'.format(key))

        # Return
        return df['{}ID'.format(table_name)].tolist()


def read_ceab_data(path: str, pattern=None) -> CEAB:
    """Read CEAB data from a given path.

    Parameters
    ----------
    path : str
        Path to a directory or file containing CEAB data.

    Returns
    -------
    CEAB
        CEAB object containing the data from the specified Excel file(s).
    """
    # If a regex pattern is provided, 'path' will be treated as a directory
    if pattern:
        ceab = None
        first_file = True
        for root, dirs, files in os.walk(path):
            for file in files:
                # Get the path of the file relative to top_dir
                rel_path = os.path.relpath(os.path.join(root, file), path)
                if re.match(pattern, rel_path):
                    if first_file:
                        ceab = CEAB(os.path.join(root, file))
                        first_file = False
                    else:
                        ceab += CEAB(os.path.join(root, file))
        return ceab

    # if a regex pattern is not provided, 'path' will be treated as a file
    else:
        return (CEAB(path))
