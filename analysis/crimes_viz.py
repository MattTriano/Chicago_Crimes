from typing import Dict, List, Union, Optional, Tuple

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def freq_selector(freq: str = "year"):
    freq = freq.lower()
    if freq == "month":
        return "M"
    elif freq == "quarter":
        return "Q"
    else:
        return "Y"


def make_plot_of_arrest_rate_per_period(
    crime_descr: str,
    crime_df: pd.DataFrame,
    crime_col: str = "description",
    start_date: str = "2005-01-01",
    end_date: str = "today",
    frequency: str = "Year",
    arrest: bool = True,
    figsize: Tuple = (14, 6),
    more_crime_descr: str = "",
) -> None:
    label_descr = crime_descr.title()

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
    count_df = df.groupby([pd.Grouper(key="date", freq=freq_selector(frequency))]).count()[
        "case_number"
    ]
    count_df.plot(ax=ax, kind="line", legend=None, label=f"{label_descr} Cases", color="#0570b0")
    arr_count_df = (
        df.loc[df["arrest"] == True]
        .groupby([pd.Grouper(key="date", freq=freq_selector(frequency))])
        .count()["case_number"]
    )
    arr_count_df.plot(
        ax=ax,
        kind="line",
        legend=None,
        label=f"{label_descr} Cases w/ Arrest",
        color="#41ae76",
    )

    ax.set_ylabel(f"{label_descr} Cases \n(per {frequency})", fontsize=18)
    ax.set_title(
        f"{more_crime_descr}{label_descr} Cases per {frequency} from {start_date} to {end_date}",
        fontsize=18,
    )
    ax.set_xlabel("Date", fontsize=18)

    ax.set_ylim([0, 1.1 * max([count_df.max(), arr_count_df.max()])])
    ax.legend()


def make_choropleth_of_crime_counts_per_beat(
    df: pd.DataFrame,
    beats_gdf: gpd.GeoDataFrame,
    crime_descr: str = "HOMICIDE",
    crime_col="primary_type",
    start_date: str = "2001-01-01",
    more_crime_descr: str = "",
    end_date: str = "2022-03-15",
    figsize: Tuple = (10, 10),
    my_cmap: str = "YlGn",
    scale: float = 0.6,
    tight: bool = True,
    title_fs: Optional = None,
) -> None:
    df = df.loc[
        (df["date"] >= start_date)
        & (df["date"] <= end_date)
        & (df[crime_col] == crime_descr.upper())
    ].copy()
    count_df = (
        df.groupby(["beat", crime_col])
        .count()["case_number"]
        .unstack(fill_value=0)
        .stack()
        .reset_index()
    )
    count_df.rename({0: "Count"}, axis=1, inplace=True)

    map_df = pd.merge(
        left=beats_gdf,
        right=count_df.loc[count_df[crime_col] == crime_descr.upper()],
        right_on="beat",
        left_on="beat_num",
        how="left",
    )
    map_df["Count"] = map_df["Count"].fillna(0)
    vmin = map_df["Count"].min()
    vmax = map_df["Count"].max()
    fig, ax = plt.subplots(figsize=figsize)
    base = beats_gdf.plot(figsize=figsize, color="white", edgecolor="black", linewidth=2, ax=ax)
    map_df.plot(column="Count", ax=base, edgecolor="grey", linewidth=0.4, cmap=my_cmap)
    _ = ax.axis("off")
    title = (
        f"{more_crime_descr}{crime_descr} cases per police beat\nfrom {start_date} to {end_date}"
    )
    if len(title) >= 60:
        title = (
            f"{more_crime_descr}{crime_descr} cases\nper police "
            + f"beat from {start_date} to {end_date}"
        )
        if not title_fs:
            title_fs = 22
    if not title_fs:
        title_fs = 25
    _ = ax.set_title(title, fontdict={"fontsize": title_fs, "fontweight": "3"})

    sm = plt.cm.ScalarMappable(cmap=my_cmap, norm=plt.Normalize(vmin=vmin, vmax=vmax))
    sm._A = []
    cbar = fig.colorbar(sm, shrink=scale)
    if tight:
        plt.tight_layout()


def make_heatmap_of_crime_frequency(
    df: pd.DataFrame,
    crime_descr: str,
    crime_col: str = "primary_type",
    x_ax: str = "Month",
    y_ax: str = "Weekday",
    more_crime_descr: str = "",
    start_date: str = "2001-01-01",
    end_date: str = "today",
    cmap: str = "YlGn",
    fig_width: float = 14,
    force_tall_xy: bool = False,
) -> None:
    if not force_tall_xy:
        if df[x_ax].nunique() < df[y_ax].nunique():
            temp_ax = x_ax
            x_ax = y_ax
            y_ax = temp_ax

    tmp_df = df.loc[
        (df["date"] >= start_date) & (df["date"] <= end_date) & (df[crime_col] == crime_descr)
    ].copy()
    tmp_counts = tmp_df.groupby([tmp_df[y_ax], tmp_df[x_ax]])["id"].count()
    tmp_counts = tmp_counts.unstack(level=1, fill_value=0)

    aspect = tmp_counts.shape[1] / tmp_counts.shape[0]
    figsize = (fig_width, fig_width / aspect * 0.95)

    fig, ax = plt.subplots(figsize=figsize)
    ax = sns.heatmap(
        tmp_counts, ax=ax, annot=False, fmt="d", cmap=cmap, cbar_kws={"aspect": 10, "pad": 0.02}
    )
    ax.tick_params(rotation=0, labelsize=fig_width)
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(labelsize=fig_width)
    ax.set_xlabel(x_ax, fontsize=fig_width * 1.3)
    ax.set_ylabel(y_ax, fontsize=fig_width * 1.3)
    title = (
        f"Frequency of crime: '{more_crime_descr}{crime_descr}' by {y_ax}"
        + f" and {x_ax} from {start_date} to {end_date}"
    )
    if len(title) > 70:
        title = (
            f"Frequency of crime: '{more_crime_descr}{crime_descr}'"
            + f" by\n{y_ax} and {x_ax} from {start_date} to {end_date}"
        )
    ax.set_title(title, fontsize=fig_width * 1.5)
    plt.tight_layout()
