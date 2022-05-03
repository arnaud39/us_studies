import pandas as pd
import numpy as np


def process_data_rd(
    df: pd.DataFrame,
    state: str = "CA",
    disaster_date: str = "2015-9-22",
) -> pd.DataFrame:
    """Days from disaster date into data column as index."""
    df_ = df.loc[(state, slice(None)), :].copy()
    df_.loc[:, "date"] = (
        (df_.loc[:, "date"] - pd.to_datetime(disaster_date)) / np.timedelta64(1, "D")
    ).astype(int)

    return df_.reset_index().set_index("date").drop(columns="state")
