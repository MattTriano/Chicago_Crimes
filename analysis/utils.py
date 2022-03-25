import os
from typing import Dict, List, Union, Optional
from urllib.request import urlretrieve

import pandas as pd
from pandas.api.types import CategoricalDtype
import geopandas as gpd


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

def engineer_hour_of_day_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Hour"] = df[date_col].dt.hour.astype(str).str.zfill(2)
    hours = [str(i).zfill(2) for i in range(0, 24)]
    hour_categories = CategoricalDtype(categories=hours, ordered=True)
    df[f"{label}Hour"] = df[f"{label}Hour"].astype(hour_categories)
    return df

def engineer_day_of_week_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Weekday"] = df[date_col].dt.dayofweek
    return df

def engineer_day_of_year_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Day"] = df[date_col].dt.dayofyear
    return df

def engineer_week_of_year_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Week"] = df[date_col].dt.isocalendar().week
    return df


def engineer_month_of_year_feature(df: pd.DataFrame, date_col: str, label: str = "") -> pd.DataFrame:
    df[f"{label}Month"] = df[date_col].dt.month.astype(str).str.zfill(2)
    months = [str(i).zfill(2) for i in range(1, 13)]
    month_categories = CategoricalDtype(categories=months, ordered=True)
    df[f"{label}Month"] = df[f"{label}Month"].astype(month_categories)
    return df