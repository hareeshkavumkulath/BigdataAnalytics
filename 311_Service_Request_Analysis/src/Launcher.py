# Main Launcher script

import Constants
import Utilities as utilFor311
from StasticalAnalysis import Analysis
from data_cleaning import DataCleaner


def get_cleaned_data():
    df_311 = utilFor311.read_data_from_csv(Constants.FILENAME)
    df_311 = DataCleaner.remove_space_from_col_names(df_311)
    df_311 = DataCleaner.drop_unwanted_cols(df_311, Constants.DROP_COLS1)
    df_311 = DataCleaner.drop_below_threshold(df_311)
    df_311 = DataCleaner.filter_frequent_request_types(df_311)
    df_311 = DataCleaner.capitalize_cols(df_311, Constants.CAPITALIZE_COLS)
    df_311 = DataCleaner.format_zip_code(df_311)
    df_311 = DataCleaner.update_burrow_city_from_zip_code(df_311)
    df_311 = DataCleaner.drop_unwanted_cols(df_311, Constants.DROP_COLS2)
    df_311 = DataCleaner.drop_empty_null_values(df_311)
    df_311 = DataCleaner.calculate_time_to_resolve_in_seconds(df_311)
    cleaned_df = DataCleaner.create_separate_day_month_year_col(df_311)
    return cleaned_df


def run_analysis(cleaned_df):
    Analysis.monthly_hourly_analysis(cleaned_df)


if __name__ == "__main__":
    cleaned_311_df = get_cleaned_data()
    run_analysis(cleaned_311_df)
