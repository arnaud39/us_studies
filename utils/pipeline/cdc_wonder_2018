"""Automatic pipeline for CDC wonder grouped by months and UCD ICD chapter."""
from countries_code import code
from ast import Return
from typing import List

import pandas as pd
import os


def auto_wonder_2018_pipeline(
    dir_="data/CDC_2018",
    columns=["Occurrence State", "MMWR Week", "UCD - ICD Chapter", "Deaths"],
    observed_outcome=[
        "Diseases of the circulatory system",
        "Diseases of the respiratory system",
    ],
    short_outcome=["circulatory system", "respiratory system"],
    identifer="death",
):

    files_ = [
        f"{dir_}/{data_file}"
        for data_file in os.listdir(dir_)
        if "txt" in data_file and identifer in data_file
    ]

    datasets = []

    for file in files_:

        df_outcomes_ = pd.read_csv(file, delimiter="\t")[columns].dropna()

        df_outcomes_["time"] = pd.to_datetime(
            df_outcomes_["MMWR Week"]
            .str.split("ending")
            .str[1]
            .str.replace(r"\(.*\)", "")
            .str.strip(),
            format="%B %d, %Y",
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

        df_observed = df_observed.assign(
            totaldeaths=lambda x: x.cirdeaths + x.resdeaths
        )
        df_observed = df_observed.assign(category=file.split(" ")[-1].split(".")[0])
        datasets.append(df_observed.loc[:, ~df_observed.columns.duplicated()])

    return (
        pd.concat(datasets)
        .reset_index()
        .set_index(["state", "time", "category"])
        .sort_index()
    )
