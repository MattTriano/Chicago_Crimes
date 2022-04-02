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
    typeset_simple_category_columns,
    make_api_call_for_socrata_csv_data,
    get_number_of_results_for_socrata_query,
    typeset_datetime_column,
    typeset_ordered_categorical_feature,
    standardize_mistakenly_int_parsed_categorical_series,
)


def load_raw_chicago_crimes_data(
    root_dir: os.path = get_project_root_dir(), force_repull: bool = False
) -> pd.DataFrame:
    crimes_df = extract_file_from_url(
        file_path=os.path.join(root_dir, "data_raw", "Crimes_-_2001_to_present.csv"),
        url="https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD",
        data_format="csv",
        force_repull=force_repull,
        return_df=True,
    )
    return crimes_df


def transform_chicago_crimes_date_columns(
    crimes_df: pd.DataFrame, dt_format: str = "%m/%d/%Y %I:%M:%S %p"
) -> pd.DataFrame:
    for date_col in ["date", "updated_on"]:
        crimes_df[date_col] = typeset_datetime_column(
            dt_series=crimes_df[date_col], dt_format=dt_format
        )
    return crimes_df


def transform_chicago_crimes_data(crimes_df: pd.DataFrame) -> gpd.GeoDataFrame:
    crimes_df.columns = [col.lower().replace(" ", "_") for col in crimes_df.columns]
    crimes_df = drop_columns(
        df=crimes_df, columns_to_drop=["x_coordinate", "y_coordinate", "location"]
    )
    crimes_df = make_point_geometry(df=crimes_df, long_col="longitude", lat_col="latitude")
    crimes_gdf = gpd.GeoDataFrame(crimes_df)
    crimes_gdf = transform_chicago_crimes_date_columns(crimes_df=crimes_gdf)
    crimes_gdf = engineer_hour_of_day_feature(df=crimes_gdf, date_col="date")
    crimes_gdf = engineer_month_of_year_feature(df=crimes_gdf, date_col="date")
    crimes_gdf = engineer_day_of_week_feature(df=crimes_gdf, date_col="date")
    crimes_gdf = engineer_week_of_year_feature(df=crimes_gdf, date_col="date")
    crimes_gdf = engineer_day_of_year_feature(df=crimes_gdf, date_col="date")
    crimes_gdf["year"] = typeset_ordered_categorical_feature(series=crimes_gdf["year"])
    crimes_gdf["district"] = standardize_mistakenly_int_parsed_categorical_series(
        series=crimes_gdf["district"], zerofill=2
    )
    crimes_gdf["ward"] = standardize_mistakenly_int_parsed_categorical_series(
        series=crimes_gdf["ward"], zerofill=2
    )
    crimes_gdf["community_area"] = standardize_mistakenly_int_parsed_categorical_series(
        series=crimes_gdf["community_area"], zerofill=2
    )
    crimes_gdf = typeset_simple_category_columns(
        df=crimes_gdf,
        category_columns=[
            "iucr",
            "primary_type",
            "description",
            "location_description",
            "fbi_code",
        ],
    )
    return crimes_gdf


def load_clean_chicago_crimes_data(
    root_dir: os.path = get_project_root_dir(),
    force_repull: bool = False,
    force_remake: bool = False,
) -> gpd.GeoDataFrame:
    file_name = "Crimes_-_2001_to_present"
    clean_file_path = os.path.join(root_dir, "data_clean", f"{file_name}.parquet.gzip")
    if not os.path.isfile(clean_file_path) or force_remake:
        crimes_gdf = transform_chicago_crimes_data(
            crimes_df=load_raw_chicago_crimes_data(root_dir=root_dir, force_repull=force_repull)
        )
        crimes_gdf.to_parquet(clean_file_path, compression="gzip")
    else:
        crimes_gdf = gpd.read_parquet(clean_file_path)
    return crimes_gdf


def get_chicago_crimes_data_since_latest_record(
    crimes_gdf: gpd.GeoDataFrame,
    table_id: str = "ijzp-q8t2",
    filter_col: str = "updated_on",
    socrata_domain: str = "data.cityofchicago.org",
    count_col: str = "id",
) -> pd.DataFrame:
    api_call_base = f"https://{socrata_domain}/resource/{table_id}.csv"
    latest_update = crimes_gdf[filter_col].max()
    latest_update_str = latest_update.strftime(format="%Y-%m-%dT%H:%M:%S.000")
    filter_str = f"$where={filter_col}>'{latest_update_str}'"

    result_count = get_number_of_results_for_socrata_query(
        filter_str=filter_str, api_call_base=api_call_base, count_col=count_col
    )

    df_parts = []
    for i in range((result_count // 1000) + 1):
        offset = i * 1000
        if result_count < (offset + 1000):
            limit = result_count % 1000
        else:
            limit = 1000
        pagination_str = f"$limit={limit}&$offset={offset}&$order={count_col}"
        api_call = f"{api_call_base}?{filter_str}&{pagination_str}"
        df_parts.append(make_api_call_for_socrata_csv_data(api_call))
    recent_crimes_df = pd.concat(df_parts)
    recent_crimes_df = recent_crimes_df.reset_index(drop=True)
    recent_crimes_df = transform_chicago_crimes_date_columns(
        crimes_df=recent_crimes_df, dt_format="%Y-%m-%dT%H:%M:%S.000"
    )
    return recent_crimes_df
