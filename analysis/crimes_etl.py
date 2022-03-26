import os

import pandas as pd
import geopandas as gpd

from utils import (
    get_project_root_dir,
    extract_file_from_url,
    make_point_geometry,
    engineer_hour_of_day_feature,
    engineer_day_of_week_feature,
    engineer_day_of_year_feature,
    engineer_week_of_year_feature,
    engineer_month_of_year_feature,
    drop_columns,
    coerce_simple_category_columns
)

def load_raw_chicago_crimes_data(root_dir: os.path = get_project_root_dir(), force_repull: bool=False) -> pd.DataFrame:
    crimes_df = extract_file_from_url(
        file_path=os.path.join(root_dir, "data_raw", "Crimes_-_2001_to_present.csv"),
        url="https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD",
        data_format="csv",
        force_repull=force_repull,
        return_df=True
    )
    return crimes_df


def transform_chicago_crimes_date_columns(crimes_df: pd.DataFrame) -> pd.DataFrame:
    for date_col in ["Date", "Updated On"]:
        crimes_df[date_col] = pd.to_datetime(crimes_df[date_col], format="%m/%d/%Y %I:%M:%S %p")
    return crimes_df

def standardize_categorical_integer_column_values(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    df[col_name] = df[col_name].astype("Int16").astype("string").str.zfill(2).astype("category")
    return df

def transform_chicago_crimes_data(crimes_df: pd.DataFrame) -> gpd.GeoDataFrame:
    crimes_df = make_point_geometry(df=crimes_df, long_col="Longitude", lat_col="Latitude")
    crimes_gdf = gpd.GeoDataFrame(crimes_df)
    crimes_gdf = transform_chicago_crimes_date_columns(crimes_df=crimes_gdf)
    crimes_gdf = engineer_hour_of_day_feature(df=crimes_gdf, date_col="Date")
    crimes_gdf = engineer_month_of_year_feature(df=crimes_gdf, date_col="Date")
    crimes_gdf = engineer_day_of_week_feature(df=crimes_gdf, date_col="Date")
    crimes_gdf = engineer_week_of_year_feature(df=crimes_gdf, date_col="Date")
    crimes_gdf = engineer_day_of_year_feature(df=crimes_gdf, date_col="Date")
    crimes_gdf = standardize_categorical_integer_column_values(df=crimes_gdf, col_name="District")
    crimes_gdf = standardize_categorical_integer_column_values(df=crimes_gdf, col_name="Ward")
    crimes_gdf = standardize_categorical_integer_column_values(df=crimes_gdf, col_name="Community Area")
    crimes_gdf = coerce_simple_category_columns(
        df=crimes_gdf, category_columns=["IUCR", "Primary Type", "Description", "Location Description", "FBI Code",  "Year"]
    )
    crimes_gdf = drop_columns(df=crimes_gdf, columns_to_drop=["X Coordinate", "Y Coordinate", "Location"])
    return crimes_gdf

def load_clean_chicago_crimes_data(
    root_dir: os.path = get_project_root_dir(), force_repull: bool = False,
    force_remake: bool = False
) -> gpd.GeoDataFrame:
    file_name = "Crimes_-_2001_to_present"
    clean_file_path = os.path.join(root_dir, "data_clean", f"{file_name}.parquet.gzip")
    if not os.path.isfile(clean_file_path) or force_remake:
        crimes_gdf = transform_chicago_crimes_data(
            crimes_df=load_raw_chicago_crimes_data(root_dir=root_dir, force_repull=force_repull)
        )
        crimes_gdf.to_parquet(clean_file_path, compression="gzip")
    else:
        crimes_gdf = pd.read_parquet(clean_file_path)
    return crimes_gdf
