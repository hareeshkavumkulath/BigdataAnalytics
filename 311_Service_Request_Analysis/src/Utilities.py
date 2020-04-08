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


def rearrange_cols(df_311):
    return df_311.select('Unique_Key', 'Closing_timestamp', 'Creation_timestamp',
                         'time_to_resolve_in_hrs',
                         'Agency', 'Agency_Name', 'Open_Data_Channel_Type', 'Status',
                         'Complaint_Type', 'Borough', 'Creation_Month',
                         'Creation_Day', 'Creation_Hour',
                         'Closing_Month', 'Closing_Day', 'Closing_Hour',
                         'Issue_Category', 'Incident_Zip', 'City', 'Latitude',
                         'Longitude', 'Created_Date',
                         'Creation_Time', 'Closed_Date', 'Closing_Time')


def get_df_row_as_dict(df_row):
    return df_row.asDict()


def print_valid_entry_count_for_each_col(cleaned_df):
    missing_value_count_df = DataCleaner.get_missing_value_count(cleaned_df)
    print_df_row_as_dict(missing_value_count_df.collect()[0])


def print_df_row_as_dict(df_row):
    for col in get_df_row_as_dict(df_row):
        print(col + ":" + str(df_row[col]))


def list_to_dict(L):
    return {k: v for d in L for k, v in d.items()}


def create_x_y_coordinates_for_group_by_results(group_by_result, col_x_name, col_y_name):
    '''
    Converts Group By Result (after collect()) into list of X , Y coordinates, which can then be used for Visualizations
    '''
    x = [entry.__getitem__(col_x_name) for entry in group_by_result]
    y = [entry.__getitem__(col_y_name) for entry in group_by_result]
    return x, y


def plot_chart_x_y(x, y, title, x_label, y_label, fig_num, results_folder, x_ticks=None, x_ticks_labels=None,
                   x_tick_rotation=0, y_ticks=None, y_ticks_labels=None, y_tick_rotation=0):
    plt.figure(num=fig_num, figsize=(8, 4))

    plt.bar(x, y, align='center', color='blue', alpha=.5)

    plt.xlabel(x_label)
    plt.ylabel(y_label)

    if x_ticks is not None:
        if x_ticks_labels is not None:
            plt.xticks(x_ticks, x_ticks_labels, rotation=x_tick_rotation)
        else:
            plt.xticks(x_ticks, rotation=x_tick_rotation)
    else:
        plt.xticks(rotation=x_tick_rotation)

    if y_ticks is not None:
        if y_ticks_labels is not None:
            plt.yticks(y_ticks, y_ticks_labels, rotation=y_tick_rotation)
        else:
            plt.yticks(y_ticks, rotation=y_tick_rotation)
    else:
        plt.yticks(rotation=y_tick_rotation)

    plt.title(title)
    plt.savefig(results_folder + str(fig_num) + '.png', bbox_inches="tight")


def prepare_plot(df, col_x_name, col_y_name, title, x_label, y_label, fig_num, results_folder, x_ticks=None,
                 x_ticks_labels=None, x_tick_rotation=0,
                 y_ticks=None, y_ticks_labels=None, y_tick_rotation=0, is_count=True):
    if is_count:
        df_groupby_col = df.groupby(col_x_name).count().orderBy(col_x_name).collect()
    else:
        df_groupby_col = df.groupby(col_x_name).mean(col_y_name).withColumnRenamed("avg(" + col_y_name + ")",
                                                                                   col_y_name).orderBy(
            col_x_name).collect()

    x, y = create_x_y_coordinates_for_group_by_results(df_groupby_col, col_x_name, col_y_name)
    plot_chart_x_y(x, y, title, x_label, y_label, fig_num, results_folder, x_ticks, x_ticks_labels, x_tick_rotation,
                   y_ticks, y_ticks_labels, y_tick_rotation)
