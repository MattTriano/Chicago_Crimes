import pandas as pd
import geopandas as gpd


def transform_chicago_crimes_date_columns(crimes_df: pd.DataFrame) -> pd.DataFrame:
    for date_col in ["Date", "Updated On"]:
        crimes_df[date_col] = pd.to_datetime(crimes_df[date_col], format="%m/%d/%Y %I:%M:%S %p")
    return crimes_df



def standardize_categorical_integer_column_values(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    df[col_name] = df[col_name].astype("Int16").astype("string").str.zfill(2).astype("category")
    return df