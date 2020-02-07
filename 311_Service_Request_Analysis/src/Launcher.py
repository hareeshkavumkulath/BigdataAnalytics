import Utilities as utilFor311
from data_cleaning import DataCleaner

filename = "./311dataset/311_Service_Requests_Apr-Aug-2019.csv"
df_311 = utilFor311.read_data_from_csv(filename)
df_311 = DataCleaner.remove_space_from_col_names(df_311)
df_311 = DataCleaner.drop_unwanted_cols(df_311)
df_311 = DataCleaner.capitalize_burrow_city(df_311)
df_311 = DataCleaner.format_zip_code(df_311)
df_311 = DataCleaner.update_burrow_city_from_zip_code(df_311)
df_311 = DataCleaner.drop_empty_null_values(df_311)
df_311 = DataCleaner.calculate_time_to_resolve_in_seconds(df_311)
missing_value_count_df = DataCleaner.get_missing_value_count(df_311)
utilFor311.print_df_row_as_dict(missing_value_count_df.collect()[0])
