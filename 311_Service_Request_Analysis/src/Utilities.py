# Python source script for common Utility Functions

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from data_cleaning import DataCleaner

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("NYC 311 Data Analysis") \
        .config('spark.sql.codegen.wholeStage', 'false') \
        .getOrCreate()
    return spark


def read_data_from_csv(filename):
    spark = init_spark()
    nyc_311_df = spark.read.format("csv").option("header", "true").load(filename)
    return nyc_311_df


def get_df_row_as_dict(df_row):
    return df_row.asDict()


def print_df_row_as_dict(df_row):
    for col in get_df_row_as_dict(df_row):
        print(col + ":" + str(df_row[col]))


def create_x_y_coordinates_for_group_by_results(group_by_result, col_name):
    '''
    Converts Group By Result (after collect()) into list of X , Y coordinates, which can then be used for Visualizations
    '''
    x = [entry.__getitem__(col_name) for entry in group_by_result]
    y = [entry.__getitem__("count") for entry in group_by_result]
    return x, y


def plot_chart_x_y(x, y, title, x_label, y_label, x_ticks=None, y_ticks=None):
    plt.figure(figsize=(12, 8))

    plt.bar(x, y, align='center', color='grey', alpha=.5)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    if x_ticks is not None:
        plt.xticks(x_ticks)
    if y_ticks is not None:
        plt.yticks(y_ticks)
    plt.title(title)

    plt.show()


def print_valid_entry_count_for_each_col(cleaned_df):
    missing_value_count_df = DataCleaner.get_missing_value_count(cleaned_df)
    print_df_row_as_dict(missing_value_count_df.collect()[0])