import io
import os
import re
from typing import Dict, List, Union, Optional
from urllib.request import urlretrieve

import geopandas as gpd
import pandas as pd
from pandas.api.types import CategoricalDtype, is_datetime64_any_dtype
import requests
from shapely.geometry import Point


def get_project_root_dir() -> os.path:
    if "__file__" in globals().keys():
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath("__file__")))
    else:
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(".")))
    assert ".git" in os.listdir(root_dir)
    return root_dir


def setup_project_structure(project_root_dir: os.path = get_project_root_dir()) -> None:
    os.makedirs(os.path.join(project_root_dir, "data_raw"), exist_ok=True)
    os.makedirs(os.path.join(project_root_dir, "data_clean"), exist_ok=True)
    os.makedirs(os.path.join(project_root_dir, "analysis"), exist_ok=True)
    os.makedirs(os.path.join(project_root_dir, "output"), exist_ok=True)


def extract_csv_from_url(
    file_path: os.path, url: str, force_repull: bool = False, return_df: bool = True
) -> pd.DataFrame:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if not os.path.isfile(file_path) or force_repull:
        urlretrieve(url, file_path)
    if return_df:
        return pd.read_csv(file_path)


def extract_file_from_url(
    file_path: os.path,
    url: str,
    data_format: str,
    force_repull: bool = False,
    return_df: bool = True,
) -> pd.DataFrame:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if not os.path.isfile(file_path) or force_repull:
        urlretrieve(url, file_path)
    if return_df:
        if data_format in ["csv", "zipped_csv"]:
            return pd.read_csv(file_path)
        elif data_format in ["shp", "geojson"]:
            return gpd.read_file(file_path)


def make_point_geometry(df: pd.DataFrame, long_col: str, lat_col: str) -> pd.Series:
    latlong_df = df[[long_col, lat_col]].copy()
    df["geometry"] = pd.Series(map(Point, latlong_df[long_col], latlong_df[lat_col]))
    return df


def typeset_datetime_column(dt_series: pd.Series, dt_format: Optional[str]) -> pd.Series:
    dt_series = dt_series.copy()
    if not is_datetime64_any_dtype(dt_series):
        if dt_format is not None:
            dt_series = pd.to_datetime(dt_series, format=dt_format)
        else:
            dt_series = pd.to_datetime(dt_series)
    return dt_series


def engineer_hour_of_day_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Hour"] = df[date_col].dt.hour.astype(str).str.zfill(2)
    hours = [str(i).zfill(2) for i in range(0, 24)]
    hour_categories = CategoricalDtype(categories=hours, ordered=True)
    df[f"{label}Hour"] = df[f"{label}Hour"].astype(hour_categories)
    return df


def engineer_day_of_week_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Weekday"] = df[date_col].dt.dayofweek
    weekday_map = {0: "MON", 1: "TUE", 2: "WED", 3: "THUR", 4: "FRI", 5: "SAT", 6: "SUN"}
    weekdays = list(weekday_map.values())
    weekday_categories = CategoricalDtype(categories=weekdays, ordered=True)
    df[f"{label}Weekday"] = df[f"{label}Weekday"].map(weekday_map).astype(weekday_categories)
    return df


def engineer_day_of_year_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Day"] = df[date_col].dt.dayofyear
    days = [i for i in range(1, 367)]
    day_categories = CategoricalDtype(categories=days, ordered=True)
    df[f"{label}Day"] = df[f"{label}Day"].astype(day_categories)
    return df


def engineer_week_of_year_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Week"] = df[date_col].dt.isocalendar().week
    weeks = [i for i in range(1, 54)]
    week_categories = CategoricalDtype(categories=weeks, ordered=True)
    df[f"{label}Week"] = df[f"{label}Week"].astype(week_categories)
    return df


def engineer_month_of_year_feature(
    df: pd.DataFrame, date_col: str, label: str = ""
) -> pd.DataFrame:
    df[f"{label}Month"] = df[date_col].dt.month.astype(str).str.zfill(2)
    months = [str(i).zfill(2) for i in range(1, 13)]
    month_categories = CategoricalDtype(categories=months, ordered=True)
    df[f"{label}Month"] = df[f"{label}Month"].astype(month_categories)
    return df


def standardize_categorical_integer_column_values(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    df[col_name] = df[col_name].astype("Int16").astype("string").str.zfill(2).astype("category")
    return df


def drop_columns(df: pd.DataFrame, columns_to_drop: List) -> pd.DataFrame:
    assert all(
        [col in df.columns for col in columns_to_drop]
    ), "columns_to_drop include missing columns"
    df = df.drop(columns=columns_to_drop)
    return df


def coerce_simple_category_columns(df: pd.DataFrame, category_columns: List[str]) -> pd.DataFrame:
    for category_column in category_columns:
        df[category_column] = df[category_column].astype("category")
    return df


def typeset_ordered_categorical_feature(series: pd.Series) -> pd.Series:
    series = series.copy()
    series_categories = list(series.unique())
    series_categories.sort()
    series = series.astype(CategoricalDtype(categories=series_categories, ordered=True))
    return series


def get_number_of_results_for_socrata_query(
    filter_str: str, api_call_base: str, count_col: str = "id"
) -> int:
    api_call = f"{api_call_base}?$select=count({count_col})&{filter_str}"
    resp = requests.get(api_call)
    if resp.status_code == 200:
        result_count_str = resp.content.decode("utf-8").replace('"', "")
        counts = re.findall(r"\n([\d]*)\n", result_count_str)
        if len(counts) > 0:
            result_count = int(counts[0])
            return result_count
    else:
        return requests.HTTPError


def make_api_call_for_socrata_csv_data(api_call: str) -> pd.DataFrame:
    resp = requests.get(api_call)
    if resp.status_code == 200:
        return pd.read_csv(io.StringIO(resp.content.decode("utf-8")))
    else:
        return requests.HTTPError
