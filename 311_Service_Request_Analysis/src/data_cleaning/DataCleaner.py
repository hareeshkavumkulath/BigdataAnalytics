# Python source script for any Data Cleaning related functions
from pyspark.sql.functions import when, col, count, isnull, upper, substring, to_timestamp, unix_timestamp
from pyspark.sql.types import IntegerType

import Constants
import Utilities as utilFor311


def get_missing_value_count(df_311):
    return df_311.select([count(when((isnull(c) | (col(c) == '')), c)).alias(c) for c in df_311.columns])


def drop_unwanted_cols(df_311):
    df_311 = df_311.drop(*Constants.DROP_COLS)
    df_size = df_311.count()
    missing_value_count_df = get_missing_value_count(df_311)
    missing_counts_dict = utilFor311.get_df_row_as_dict(missing_value_count_df.collect()[0])
    drop_list = [k for k, v in missing_counts_dict.items() if (v / df_size) >= Constants.DROP_THRESHOLD]
    if len(drop_list) > 0:
        return df_311.drop(*drop_list)
    return df_311


def remove_space_from_col_names(df_311):
    return (df_311.withColumnRenamed('Unique Key', 'Unique_Key').withColumnRenamed('Created Date', 'Created_Date')
            .withColumnRenamed('Closed Date', 'Closed_Date')
            .withColumnRenamed('Due Date', 'Due_Date').withColumnRenamed('Address Type', 'Address_Type')
            .withColumnRenamed('Location Type', 'Location_Type').withColumnRenamed('Incident Zip', 'Incident_Zip')
            .withColumnRenamed('Complaint Type', 'Complaint_Type')
            .withColumnRenamed('Open Data Channel Type', 'Open_Data_Channel_Type'))


def drop_empty_null_values(df_311):
    df_311 = df_311.filter((col('City').isNotNull()) & (col('City') != "") & (col('Closed_Date').isNotNull()) & (
            col('Closed_Date') != ""))
    return df_311


def update_burrow_city_from_zip_code(df_311):
    areas = ['City', 'Borough']
    for area in areas:
        df_311 = df_311.withColumn(area, when(
            (col("Incident_Zip") > 10450.0) & (col("Incident_Zip") < 10475.0) & (col(area).isNull()),
            "BRONX").otherwise(col(area)))
        df_311 = df_311.withColumn(area, when(
            (col("Incident_Zip") > 11200.0) & (col("Incident_Zip") < 11240.0) & (col(area).isNull()),
            "BROOKLYN").otherwise(col(area)))
        df_311 = df_311.withColumn(area, when(
            (col("Incident_Zip") > 10000.0) & (col("Incident_Zip") < 10280.0) & (col(area).isNull()),
            "MANHATTAN").otherwise(col(area)))
        df_311 = df_311.withColumn(area, when(
            (col("Incident_Zip") > 10300.0) & (col("Incident_Zip") < 10315.0) & (col(area).isNull()),
            "STATEN ISLAND").otherwise(col(area)))
        df_311 = df_311.withColumn(area, when(
            (col("Incident_Zip") > 11350.0) & (col("Incident_Zip") < 11700.0) & (col(area).isNull()),
            "QUEENS").otherwise(col(area)))
    return df_311


def capitalize_cols(df_311, cols):
    for c in cols:
        df_311 = df_311.withColumn(c, upper(col(c)))
    return df_311


def format_zip_code(df_311):
    df_311 = df_311.withColumn('Incident_Zip', substring('Incident_Zip', 1, 5).cast('float'))
    return df_311


def calculate_time_to_resolve_in_seconds(df_311):
    time_fmt = "dd/MM/yyyy HH:mm:ss"
    time_fmt2 = "MM/dd/yyyy HH:mm:ss"
    time_diff = when(to_timestamp(df_311.Closed_Date, time_fmt).isNull(), unix_timestamp('Closed_Date',
                                                                                         format=time_fmt2) - unix_timestamp(
        'Created_Date', format=time_fmt)).otherwise(
        unix_timestamp('Closed_Date', format=time_fmt) - unix_timestamp('Created_Date', format=time_fmt))
    return df_311.withColumn("time_to_resolve", time_diff)


def create_separate_day_month_year_col(df_311):
    df_with_year = df_311.withColumn('Creation_Year', substring('Created_Date', 7, 4)).withColumn(
        'Closing_Year', substring('Closed_Date', 7, 4))

    time_fmt = "dd/MM/yyyy HH:mm:ss"
    change_format_month = when(to_timestamp(df_311.Closed_Date, time_fmt).isNull(),
                               substring('Closed_Date', 1, 2)).otherwise(substring('Closed_Date', 4, 2))
    change_format_day = when(to_timestamp(df_311.Closed_Date, time_fmt).isNull(),
                             substring('Closed_Date', 4, 2)).otherwise(substring('Closed_Date', 1, 2))

    df_with_month = df_with_year.withColumn('Creation_Month', substring('Created_Date', 4, 2))
    df_with_month = df_with_month.withColumn('Closing_Month', change_format_month)
    df_with_month = df_with_month.withColumn("Creation_Month", when(col("Creation_Month") == '01', 'Jan')
                                             .when(col("Creation_Month") == '02', 'Feb')
                                             .when(col("Creation_Month") == '03', 'Mar')
                                             .when(col("Creation_Month") == '04', 'Apr')
                                             .when(col("Creation_Month") == '05', 'May')
                                             .when(col("Creation_Month") == '06', 'Jun')
                                             .when(col("Creation_Month") == '07', 'Jul')
                                             .when(col("Creation_Month") == '08', 'Aug')
                                             .when(col("Creation_Month") == '09', 'Sep')
                                             .when(col("Creation_Month") == '10', 'Oct')
                                             .when(col("Creation_Month") == '11', 'Nov')
                                             .when(col("Creation_Month") == '12', 'Dec')
                                             .otherwise('Unspecified'))

    df_with_day = df_with_month.withColumn('Creation_Day', substring('Created_Date', 1, 2))
    df_with_day = df_with_day.withColumn('Closing_Day', change_format_day)

    df_with_time = df_with_day.withColumn('Creation_Time', substring('Created_Date', 12, 11)).withColumn(
        'Closing_Time', substring('Closed_Date', 12, 11))

    df_with_time = df_with_time.withColumn('Creation_Hour', when(substring('Creation_Time', 10, 2) == 'AM',
                                                                 substring('Creation_Time', 1, 2)).otherwise(
        substring('Creation_Time', 1, 2).cast(IntegerType()) + 12))

    df_with_year_month_day = df_with_time.withColumn('Closing_Hour', when(substring('Closing_Time', 10, 2) == 'AM',
                                                                          substring('Closing_Time', 1, 2)).otherwise(
        substring('Closing_Time', 1, 2).cast(IntegerType()) + 12))

    return df_with_year_month_day