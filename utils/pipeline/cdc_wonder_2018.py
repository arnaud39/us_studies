"""Automatic pipeline for CDC wonder grouped by months and UCD ICD chapter."""
from countries_code import code
from typing import List

import pandas as pd
import numpy as np
import os


def auto_wonder_2018_pipeline(
    dir_="data/CDC_2018",
    columns=["Occurrence State", "MMWR Week", "UCD - ICD Chapter", "Deaths"],
    observed_outcome=[
        "Diseases of the circulatory system",
        "Diseases of the respiratory system",
    ],
    short_outcome=["circulatory system", "respiratory system"],
    identifier="death",
    identifier_additionnal=["incident", "infectious"],
):
    """Pipepline for provisionnal data 2018 onwards on CDC wonder.
    Requests link
    - death: https://wonder.cdc.gov/controller/saved/D176/D296F145
    - infectious: https://wonder.cdc.gov/controller/saved/D176/D296F453

    Args:
        dir_ (str, optional): _description_. Defaults to "data/CDC_2018".
        columns (list, optional): _description_. Defaults to ["Occurrence State", "MMWR Week", "UCD - ICD Chapter", "Deaths"].
        observed_outcome (list, optional): _description_. Defaults to [ "Diseases of the circulatory system", "Diseases of the respiratory system", ].
        short_outcome (list, optional): _description_. Defaults to ["circulatory system", "respiratory system"].
        identifier (str, optional): _description_. Defaults to "death".
        identifier_additionnal (list, optional): _description_. Defaults to ["incident", "infectious"].

    Returns:
        _type_: _description_
    """

    files_ = [
        f"{dir_}/{data_file}"
        for data_file in os.listdir(dir_)
        if "txt" in data_file and identifier in data_file
    ]

    datasets = []
    datasets_deaths = {}

    for file in files_:

        df_outcomes_ = pd.read_csv(file, delimiter="\t")[columns].dropna()

        df_outcomes_["time"] = pd.to_datetime(
            df_outcomes_["MMWR Week"]
            .str.split("ending")
            .str[1]
            .str.replace(r"\(.*\)", "")
            .str.strip(),
            format="%B %d, %Y",
            errors="coerce",
        )

        df_outcomes_

        df_outcomes_ = df_outcomes_.dropna()

        df_outcomes_["date"] = df_outcomes_["time"]  # copy for plotting

        df_outcomes_["State"] = df_outcomes_["Occurrence State"].map(code)
        df_outcomes_ = df_outcomes_.drop(columns=["Occurrence State", "MMWR Week"])

        df_outcomes_.rename(
            columns={
                "State": "state",
                "UCD - ICD Chapter": "cause",
                "ICD Chapter": "cause",
                "Deaths": "deaths",
                "MMWR Year Code": "time",
            },
            inplace=True,
        )

        df_outcomes = df_outcomes_.set_index(["state", "time", "cause"])
        df_outcomes.sort_index(ascending=True, inplace=True)

        df_observed = pd.concat(
            [
                (
                    df_outcomes.loc[(slice(None), slice(None), outcome), :]
                    .reset_index()
                    .set_index(["state", "time"])
                    .rename(columns={"deaths": "{}deaths".format(short_outcome[:3])})
                    .drop("cause", axis=1)
                )
                for outcome, short_outcome in zip(observed_outcome, short_outcome)
            ],
            axis=1,
        ).fillna(0)

        # df_observed = df_observed.assign(
        #    totaldeaths=lambda x: x.cirdeaths + x.resdeaths
        # )
        df_observed = df_observed.assign(category=file.split(" ")[-1].split(".")[0])
        datasets.append(df_observed.loc[:, ~df_observed.columns.duplicated()])

    short_names = []

    for identifier_incident in identifier_additionnal:

        short_name = identifier_incident[:3] + "deaths"
        short_names.append(short_name)
        files_ = [
            f"{dir_}/{data_file}"
            for data_file in os.listdir(dir_)
            if "txt" in data_file and identifier_incident in data_file
        ]

        for file in files_:

            df_outcomes_ = pd.read_csv(file, delimiter="\t")[
                ["Occurrence State", "MMWR Week", "Deaths"]
            ].dropna()

            df_outcomes_["time"] = pd.to_datetime(
                df_outcomes_["MMWR Week"]
                .str.split("ending")
                .str[1]
                .str.replace(r"\(.*\)", "")
                .str.strip(),
                format="%B %d, %Y",
                errors="coerce",
            )

            df_outcomes_ = df_outcomes_.dropna()

            df_outcomes_["date"] = df_outcomes_["time"]  # copy for plotting

            df_outcomes_["State"] = df_outcomes_["Occurrence State"].map(code)
            df_outcomes_ = df_outcomes_.drop(columns=["Occurrence State", "MMWR Week"])

            df_outcomes_.rename(
                columns={
                    "State": "state",
                    "UCD - ICD Chapter": "cause",
                    "ICD Chapter": "cause",
                    "Deaths": "deaths",
                    "MMWR Year Code": "time",
                },
                inplace=True,
            )
            df_outcomes = df_outcomes_.set_index(["state", "time"])
            df_outcomes.sort_index(ascending=True, inplace=True)

            # all, 65+ ...
            category = file.split(" ")[-1].split(".")[0]
            df_observed = df_outcomes.assign(category=category).rename(
                {"deaths": short_name}, axis=1
            )

            if category not in datasets_deaths.keys():
                datasets_deaths[category] = []

            datasets_deaths[category].append(
                df_observed.loc[:, ~df_observed.columns.duplicated()]
            )

    res = (
        pd.concat(datasets)
        .reset_index()
        .set_index(["state", "time", "category"])
        .sort_index()
    )

    res[short_names] = np.NaN
    # return res, datasets_deaths

    res = (
        res.combine_first(
            (
                pd.concat(
                    [
                        pd.concat(
                            [
                                data.drop("date", axis=1)
                                .reset_index()
                                .set_index(["state", "time", "category"])
                                for data in sub_datasets
                            ],
                            axis=1,
                        )
                        for sub_datasets in datasets_deaths.values()
                    ],
                    axis=0,
                ).sort_index()
            )
        ).fillna(0)
        # .assign(totaldeaths=lambda x: x.cirdeaths + x.resdeaths + x.incdeaths + x.candeaths)
    )

    res["totaldeaths"] = res.sum(numeric_only=True, axis=1)

    res.date = pd.to_datetime(res.date, errors="coerce")
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']

    #convert float to int
    cols = res.select_dtypes(include=numerics).columns
    res[cols] = res[cols].astype(int)

    return res.dropna()
