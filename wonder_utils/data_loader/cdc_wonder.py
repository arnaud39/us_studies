from typing import Dict, List, Any
from plotly.subplots import make_subplots
import plotly.graph_objects as go

import pandas as pd
import numpy as np
import os

from ..plots.blueprint import DataPloter


class Death_Data(DataPloter):
    """
    Available features to select:
        state, month, age_category, ICD chapter

    Available features to plot:
        deaths, suicide_proportion, suicide_per_100k

    The data pipeline works as following:"""

    def __init__(
        self,
        data_folder: str = "Data",
        indexer_columns: List[str] = [
            "state",
            "date",
            "age_group",
            "icd",
        ],
        drop_cols: List[str] = [
            "Year Code",
            "State Code",
            "Month",  # keep month code
            "ICD Chapter Code",
            "Crude Rate",
        ],
        reject_list: List[str] = [],
    ) -> None:

        super(SuicideData, self).__init__(
            data_folder=data_folder,
            indexer_columns=indexer_columns,
            drop_cols=drop_cols,
            reject_list=reject_list,
        )

    def file_to_dataframe(
        self,
        data_folder: str,
        file: str,
        rename_mapper: Dict[str, str] = {
            "Month Code": "date",
            "ICD Chapter": "icd",
            "Deaths": "deaths",
        },
    ) -> pd.DataFrame:
        """CDC Wonder txt file into dataframe
        Args:
            data_folder (str): where the data files are stored
            file (str): file that we want to process
            rename_mapper (_type_, optional): dictionnary to rename
                the columns.
                Defaults to {"Single Race 6": "race",
                             "Race": "race",
                             "Gender": "gender",
                             "Residence HHS Region Code": "hhs",
                             "HHS Region Code": "hhs",
                             "Population": "population",
                             "Year": "year", "Deaths": "deaths",
                             "Hispanic Origin": "ethnicity",
                             "Age Adjusted Rate": "age_adjusted_rate",}.

        Returns:
            pd.DataFrame: converted file into a pandas dataframe
        """
        process_line = lambda line: line.strip().replace('"', "").split("\t")

        lines = []
        with open(f"{data_folder}/{file}", "r") as f:
            for line in iter(lambda: f.readline().rstrip(), '"---"'):
                lines.append(process_line(line))
        # column is on the header, remove corner named notes
        res = pd.DataFrame(lines[1:], columns=lines[0][1:]).rename(
            columns=rename_mapper
        )
        # If Age Adjusted Rate is missing, fill with NaN
        if "age_adjusted_rate" not in res.columns:
            res["age_adjusted_rate"] = np.nan
        return res

    def processor(
        self,
        x: pd.DataFrame,
        force_numeric: List[str] = ["population", "Crude Rate"],
    ) -> pd.DataFrame:
        """Process dataframes: convert columns dtype, compute new features.
        Args:
            x (pd.DataFrame): dataframe that we want to process
            force_numeric (List[str], optional): force these columns
                into numerical columns.
                Defaults to ["population", "Crude Rate"].

        Returns:
            pd.DataFrame: processed dataframe

        """

        # Automatically convert numeric columns to float/int type
        if self.numeric_columns.empty:
            self.numeric_columns = x.apply(
                lambda s: pd.to_numeric(s.replace(np.nan, 0), errors="coerce")
                .notnull()
                .all()
            )

            # compare with the force numeric and prevent numeric columns
            self.convert_cols = [
                col
                for col, bool_ in self.numeric_columns.items()
                if (bool_ or col in force_numeric)  # and col != "year"
                # keep year as str to prevent unexpected ticks on plots
            ]

        x.columns = self.numeric_columns.index

        x.year = x.year.str.extract("(\d+)")
        # x = x.replace(
        #     {
        #         "Not Hispanic or Latino": "Non-Hispanic",
        #         "Hispanic or Latino": "Hispanic",
        #         "Not Applicable": np.nan,
        #         "Unreliable": np.nan,
        #         "Asian or Pacific Islander": "API",
        #         "Asian": "API",
        #         "Black or African American": "Black",
        #         "Native Hawaiian or Other Pacific Islander": "API",
        #     }
        # )

        x[self.convert_cols] = x[self.convert_cols].apply(
            pd.to_numeric, errors="coerce"
        )

        x = x.loc[
            ~pd.concat(
                [x.eq(forbidden).any(axis=1) for forbidden in self.reject_list], axis=1
            ).any(axis=1)
        ]

    def load_data(
        self, drop_cols: List[str] = [], identifier: str = "Data"
    ) -> Dict[str, pd.DataFrame]:
        """
        Automatically load all files containing identifier.
        This method is specific to the suicie_rate project and should be
        rewritten for other projects.

        Args:
            drop_cols (List[str], optional): Select what columns
                should be dropped. Defaults to [].
            identifier (str, optional): used to identify which files
                should be process. Defaults to "Data".

        Returns:
            Dict[str, pd.DataFrame]: dictionnary containing specific
                name of files and its associated dataframe.
        """
        available_files = os.listdir("data")
        raw_data = [file for file in available_files if identifier in file]

        data = {
            (entry.split()[-1].split(".")[0], entry.split()[-2]): entry
            for entry in raw_data
        }

        age_strats = set(key[0] for key in data.keys())  # {'10-19', 'Overall', ...}

        dataframes = {
            age_strat: pd.concat(
                map(
                    self.processor,
                    [
                        self.file_to_dataframe(self.data_folder, file).assign(
                            age_strat=age_strat
                        )
                        for key_tuple, file in data.items()
                        if key_tuple[0] == age_strat
                    ],
                ),
                axis=0,
            ).drop(columns=drop_cols)
            for age_strat in age_strats
        }
        return dataframes


class SuicideData(DataPloter):
    """
    Available features to select:
        race, year, hhs, ethnicity population, ethno_race, age_strat,
        ethno_race_4_cat, gender

    Available features to plot:
        deaths, suicide_proportion, suicide_per_100k, age_adjusted_rate

    The data pipeline works as following:"""

    def __init__(
        self,
        data_folder: str = "Data",
        indexer_columns: List[str] = [
            "hhs",
            "gender",
            "year",
            "race",
            "ethnicity",
            "age_strat",
            "ethno_race",
            "ethno_race_4_cat",
        ],
        drop_cols: List[str] = [
            "Year Code",
            "Gender Code",
            "HHS Region",
            "Race Code",
            "Crude Rate",
            "Hispanic Origin Code",
        ],
        reject_list: List[str] = [
            "American Indian or Alaska Native",
            "More than one race",
            "Native Hawaiian or Other Pacific Islander",
            "Not Stated",
        ],
    ) -> None:

        super(SuicideData, self).__init__(
            data_folder=data_folder,
            indexer_columns=indexer_columns,
            drop_cols=drop_cols,
            reject_list=reject_list,
        )

    def file_to_dataframe(
        self,
        data_folder: str,
        file: str,
        rename_mapper: Dict[str, str] = {
            "Single Race 6": "race",
            "Race": "race",
            "Gender": "gender",
            "Residence HHS Region Code": "hhs",
            "HHS Region Code": "hhs",
            "Population": "population",
            "Year": "year",
            "Deaths": "deaths",
            "Hispanic Origin": "ethnicity",
            "Age Adjusted Rate": "age_adjusted_rate",
        },
    ) -> pd.DataFrame:
        """CDC Wonder txt file into dataframe
        Args:
            data_folder (str): where the data files are stored
            file (str): file that we want to process
            rename_mapper (_type_, optional): dictionnary to rename
                the columns.
                Defaults to {"Single Race 6": "race",
                             "Race": "race",
                             "Gender": "gender",
                             "Residence HHS Region Code": "hhs",
                             "HHS Region Code": "hhs",
                             "Population": "population",
                             "Year": "year", "Deaths": "deaths",
                             "Hispanic Origin": "ethnicity",
                             "Age Adjusted Rate": "age_adjusted_rate",}.

        Returns:
            pd.DataFrame: converted file into a pandas dataframe
        """
        process_line = lambda line: line.strip().replace('"', "").split("\t")

        lines = []
        with open(f"{data_folder}/{file}", "r") as f:
            for line in iter(lambda: f.readline().rstrip(), '"---"'):
                lines.append(process_line(line))
        # column is on the header, remove corner named notes
        res = pd.DataFrame(lines[1:], columns=lines[0][1:]).rename(
            columns=rename_mapper
        )
        # If Age Adjusted Rate is missing, fill with NaN
        if "age_adjusted_rate" not in res.columns:
            res["age_adjusted_rate"] = np.nan
        return res

    def processor(
        self,
        x: pd.DataFrame,
        force_numeric: List[str] = ["population", "Crude Rate"],
    ) -> pd.DataFrame:
        """Process dataframes: convert columns dtype, compute new features.
        Args:
            x (pd.DataFrame): dataframe that we want to process
            force_numeric (List[str], optional): force these columns
                into numerical columns.
                Defaults to ["population", "Crude Rate"].

        Returns:
            pd.DataFrame: processed dataframe

        """

        # Automatically convert numeric columns to float/int type
        if self.numeric_columns.empty:
            self.numeric_columns = x.apply(
                lambda s: pd.to_numeric(s.replace(np.nan, 0), errors="coerce")
                .notnull()
                .all()
            )

            # compare with the force numeric and prevent numeric columns
            self.convert_cols = [
                col
                for col, bool_ in self.numeric_columns.items()
                if (bool_ or col in force_numeric)  # and col != "year"
                # keep year as str to prevent unexpected ticks on plots
            ]

        x.columns = self.numeric_columns.index

        x.year = x.year.str.extract("(\d+)")
        x = x.replace(
            {
                "Not Hispanic or Latino": "Non-Hispanic",
                "Hispanic or Latino": "Hispanic",
                "Not Applicable": np.nan,
                "Unreliable": np.nan,
                "Asian or Pacific Islander": "API",
                "Asian": "API",
                "Black or African American": "Black",
                "Native Hawaiian or Other Pacific Islander": "API",
            }
        )

        x[self.convert_cols] = x[self.convert_cols].apply(
            pd.to_numeric, errors="coerce"
        )

        x = x.loc[
            ~pd.concat(
                [x.eq(forbidden).any(axis=1) for forbidden in self.reject_list], axis=1
            ).any(axis=1)
        ]

        def create_ethno_4_cat(y: pd.Series) -> str:
            """based on ethinicty and race, create a ethno-race feature
            with only 4 categories (or 3 if the only races are Black and White)

            Args:
                y (pd.Series): single line series to create
                the feature

            Returns:
                str: ethno-race feature
            """
            if y.ethnicity == "Hispanic":
                return "Hispanic"
            if y.race in ("Black", "White"):
                return f"Non-hispanic {y.race}"
            return "Non-hispanic Others"

        x["ethno_race_4_cat"] = x[["race", "ethnicity"]].apply(
            lambda y: create_ethno_4_cat(y), axis=1
        )

        return x.assign(ethno_race=lambda x: x.race + " " + x.ethnicity)

    def load_data(
        self, drop_cols: List[str] = [], identifier: str = "Data"
    ) -> Dict[str, pd.DataFrame]:
        """
        Automatically load all files containing identifier.
        This method is specific to the suicie_rate project and should be
        rewritten for other projects.

        Args:
            drop_cols (List[str], optional): Select what columns
                should be dropped. Defaults to [].
            identifier (str, optional): used to identify which files
                should be process. Defaults to "Data".

        Returns:
            Dict[str, pd.DataFrame]: dictionnary containing specific
                name of files and its associated dataframe.
        """
        available_files = os.listdir("data")
        raw_data = [file for file in available_files if identifier in file]

        data = {
            (entry.split()[-1].split(".")[0], entry.split()[-2]): entry
            for entry in raw_data
        }

        age_strats = set(key[0] for key in data.keys())  # {'10-19', 'Overall', ...}

        dataframes = {
            age_strat: pd.concat(
                map(
                    self.processor,
                    [
                        self.file_to_dataframe(self.data_folder, file).assign(
                            age_strat=age_strat
                        )
                        for key_tuple, file in data.items()
                        if key_tuple[0] == age_strat
                    ],
                ),
                axis=0,
            ).drop(columns=drop_cols)
            for age_strat in age_strats
        }
        return dataframes
