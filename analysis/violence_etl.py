import os

import pandas as pd
import geopandas as gpd

from utils import (
    get_project_root_dir,
    extract_file_from_url,
    make_point_geometry,
    map_column_to_boolean_values,
    engineer_hour_of_day_feature,
    engineer_day_of_week_feature,
    engineer_day_of_year_feature,
    engineer_week_of_year_feature,
    engineer_month_of_year_feature,
    standardize_column_names,
    transform_date_columns,
    drop_columns,
    typeset_simple_category_columns,
    typeset_ordered_categorical_column,
    make_api_call_for_socrata_csv_data,
    get_number_of_results_for_socrata_query,
    typeset_ordered_categorical_feature,
    standardize_mistakenly_int_parsed_categorical_series,
)


def load_raw_chicago_homicide_and_shooting_data(
    root_dir: os.path = get_project_root_dir(), force_repull: bool = False
) -> pd.DataFrame:
    df = extract_file_from_url(
        file_path=os.path.join(
            root_dir,
            "data_raw",
            "Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings.csv",
        ),
        url="https://data.cityofchicago.org/api/views/gumc-mgzr/rows.csv?accessType=DOWNLOAD",
        data_format="csv",
        force_repull=force_repull,
        return_df=True,
    )
    return df


def preprocess_homicide_and_nfs_data(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_column_names(df=df)
    df = df.drop(columns=["location"])
    return df


def typreset_homicide_and_nfs_data(df: pd.DataFrame) -> pd.DataFrame:
    df = transform_date_columns(df=df, date_cols=["date", "updated"])
    df["zip_code"] = standardize_mistakenly_int_parsed_categorical_series(series=df["zip_code"])
    df["area"] = standardize_mistakenly_int_parsed_categorical_series(series=df["area"])
    df["ward"] = standardize_mistakenly_int_parsed_categorical_series(series=df["ward"], zerofill=2)
    df["district"] = standardize_mistakenly_int_parsed_categorical_series(
        series=df["district"], zerofill=2
    )
    df["beat"] = standardize_mistakenly_int_parsed_categorical_series(series=df["beat"])
    df["state_house_district"] = standardize_mistakenly_int_parsed_categorical_series(
        series=df["state_house_district"], zerofill=2
    )
    df["state_senate_district"] = standardize_mistakenly_int_parsed_categorical_series(
        series=df["state_senate_district"], zerofill=2
    )
    df = map_column_to_boolean_values(df=df, input_col="gunshot_injury_i", true_values=["YES"])
    df = typeset_simple_category_columns(
        df=df,
        category_columns=[
            "victimization_primary",
            "incident_primary",
            "community_area",
            "street_outreach_organization",
            "sex",
            "race",
            "age",
            "victimization_fbi_cd",
            "incident_fbi_cd",
            "victimization_fbi_descr",
            "incident_fbi_descr",
            "victimization_iucr_cd",
            "incident_iucr_cd",
            "victimization_iucr_secondary",
            "incident_iucr_secondary",
            "location_description",
        ],
    )
    df["month"] = typeset_ordered_categorical_column(
        series=df["month"].astype(str).str.zfill(2),
        ordered_category_values=[str(i).zfill(2) for i in range(1, 13)],
    )
    df["day_of_week"] = typeset_ordered_categorical_column(
        series=df["day_of_week"], ordered_category_values=[i for i in range(1, 8)]
    )
    df["hour"] = typeset_ordered_categorical_column(
        series=df["hour"].astype(str).str.zfill(2),
        ordered_category_values=[str(i).zfill(2) for i in range(1, 25)],
    )
    return df


def engineer_basic_date_features_for_homicide_and_nfs_data(df: pd.DataFrame) -> pd.DataFrame:
    df = engineer_day_of_week_feature(df=df, date_col="date")
    df = engineer_day_of_year_feature(df=df, date_col="date")
    df = engineer_week_of_year_feature(df=df, date_col="date")
    return df


def transform_homicide_and_nfs_data(df: pd.DataFrame) -> pd.DataFrame:
    df = preprocess_homicide_and_nfs_data(df=df)
    df = typreset_homicide_and_nfs_data(df=df)
    df = engineer_basic_date_features_for_homicide_and_nfs_data(df=df)
    return df


def load_clean_chicago_homicides_and_nonfatal_shootings_data(
    root_dir: os.path = get_project_root_dir(),
    force_repull: bool = False,
    force_remake: bool = False,
) -> gpd.GeoDataFrame:
    file_name = "Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings"
    clean_file_path = os.path.join(root_dir, "data_clean", f"{file_name}.parquet.gzip")
    if not os.path.isfile(clean_file_path) or force_remake:
        df = transform_homicide_and_nfs_data(
            df=load_raw_chicago_homicide_and_shooting_data(
                root_dir=root_dir, force_repull=force_repull
            )
        )
        df.to_parquet(clean_file_path, compression="gzip")
    else:
        df = pd.read_parquet(clean_file_path)
    gdf = make_point_geometry(df=df, long_col="longitude", lat_col="latitude")
    gdf = gpd.GeoDataFrame(gdf, crs="EPSG:4326")
    return gdf
