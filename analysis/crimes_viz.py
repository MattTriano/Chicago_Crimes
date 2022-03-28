from typing import Dict, List, Union, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd


def make_plot_of_arrest_rate_per_period(
    crime_descr: str,
    crime_df: pd.DataFrame,
    crime_col: str = "description",
    start_date: str = "2005-01-01",
    end_date: str = "today",
    frequency: str = "Year",
    arrest: bool = True,
    figsize: Tuple = (14, 6),
    more_label: str = "",
) -> None:
    lable_descr = crime_descr.title()

    VALID_CRIME_COL_VALUES = ["description", "primary_type"]
    if crime_col not in VALID_CRIME_COL_VALUES:
        if crime_descr in crime_df["description"].unique():
            crime_col = "description"
        elif crime_descr in crime_df["primary_type"].unique():
            crime_col = "primary_type"

    if isinstance(crime_descr, list):
        df = crime_df.loc[
            (crime_df[crime_col].isin(crime_descr))
            & (crime_df["date"] >= start_date)
            & (crime_df["date"] <= end_date)
        ].copy()
    else:
        df = crime_df.loc[
            (crime_df[crime_col] == crime_descr)
            & (crime_df["date"] >= start_date)
            & (crime_df["date"] <= end_date)
        ].copy()

    fig, ax = plt.subplots(sharex=True, figsize=figsize)
    count_df = df.groupby(
        [pd.Grouper(key="date", freq=freq_selector(frequency))]
    ).count()["case_number"]
    count_df.plot(
        ax=ax, kind="line", legend=None, label=f"{lable_descr} Cases", color="#0570b0"
    )
    arr_count_df = (
        df.loc[df["arrest"] == True]
        .groupby([pd.Grouper(key="date", freq=freq_selector(frequency))])
        .count()["case_number"]
    )
    arr_count_df.plot(
        ax=ax,
        kind="line",
        legend=None,
        label=f"{lable_descr} Cases w/ Arrest",
        color="#41ae76",
    )

    ax.set_ylabel(f"{lable_descr} Cases \n(per {frequency})", fontsize=18)
    ax.set_title(
        f"{more_label}{lable_descr} Cases per {frequency} from {start_date} to {end_date}",
        fontsize=18,
    )
    ax.set_xlabel("Date", fontsize=18)

    ax.set_ylim([0, 1.1 * max([count_df.max(), arr_count_df.max()])])
    ax.legend()
