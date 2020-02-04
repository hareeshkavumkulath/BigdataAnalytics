import Utilities as utilFor311
from data_cleaning import DataCleaner

filename = "./311dataset/311_Service_Requests_Apr-Aug-2019.csv"
df_311 = utilFor311.read_data_from_csv(filename)
df_311 = DataCleaner.drop_unwanted_cols(df_311)
df_311 = DataCleaner.remove_space_from_col_names(df_311)
df_311 = DataCleaner.calculate_time_to_resolve(df_311)
missing_value_count_df = DataCleaner.get_missing_value_count(df_311)
utilFor311.print_df_row_as_dict(missing_value_count_df.collect()[0])