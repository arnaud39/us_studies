"""Contains fucntions used to perform signal filtering."""
from typing import Any
from .filtering_methods import hpfilter

import pandas as pd

filter_joint = lambda df, column_, method, type="int", **kwargs: pd.concat(
    [
        method(df.loc[slice(None, 0)][column_], **kwargs)[1]
        if type == "int"
        else method(df.loc[slice(None, 0)][column_], **kwargs).trend,
        method(df.loc[slice(0, None)][column_], **kwargs)[1]
        if type == "int"
        else method(df.loc[slice(0, None)][column_], **kwargs).trend,
    ],
    axis=0,
)

filter_one = (
    lambda df, column_, method, type="int", **kwargs: method(df[column_], **kwargs)[1]
    if type == "int"
    else method(df[column_], **kwargs).trend
)


def filter(
    data: pd.DataFrame, filter_: Any = hpfilter, method: Any = filter_joint
) -> None:
    """data: first col is time, then the outcome as short text. Modify inplace!"""
    for short_outcome in data.columns[1:]:
        data["{}Filtered".format(short_outcome[:3])] = method(
            data, short_outcome, filter_
        )

        data["threshold"] = (data.index > 0).astype(int)
        data["date"] = data.index
