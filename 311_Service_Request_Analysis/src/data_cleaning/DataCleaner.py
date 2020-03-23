# Python source script for any Data Cleaning related functions
from pyspark.sql.functions import when, col, count, isnull, upper, substring, to_timestamp, unix_timestamp, lit
from pyspark.sql.types import IntegerType

import Constants
import Utilities as utilFor311


def get_missing_value_count(df_311):
    return df_311.select([count(when((isnull(c) | (col(c) == '')), c)).alias(c) for c in df_311.columns])


def drop_unwanted_cols(df_311, drop_cols):
    drop_list = [c for c in df_311.columns if c in drop_cols]
    df_311 = df_311.drop(*drop_list)
    return df_311


def drop_below_threshold(df_311):
    df_311.cache()
    df_size = df_311.count()
    missing_value_count_df = get_missing_value_count(df_311)
    missing_counts_dict = utilFor311.get_df_row_as_dict(missing_value_count_df.collect()[0])
    drop_list = [k for k, v in missing_counts_dict.items() if (v / df_size) >= Constants.DROP_THRESHOLD]
    if len(drop_list) > 0:
        return df_311.drop(*drop_list)


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
    timestamp_format_col = when(to_timestamp(df_311.Closed_Date, time_fmt).isNull(),
                                unix_timestamp('Closed_Date', format=time_fmt2)).otherwise(
        unix_timestamp('Closed_Date', format=time_fmt))
    df_311 = df_311.withColumn("Closing_timestamp", timestamp_format_col)
    timestamp_format_col = when(to_timestamp(df_311.Created_Date, time_fmt).isNull(),
                                unix_timestamp('Created_Date', format=time_fmt2)).otherwise(
        unix_timestamp('Created_Date', format=time_fmt))
    df_311 = df_311.withColumn("Creation_timestamp", timestamp_format_col)
    df_311 = df_311.withColumn("time_to_resolve_in_hrs", (col('Closing_timestamp') - col('Creation_timestamp')) / lit(
        3600))
    return df_311.filter(df_311["time_to_resolve_in_hrs"] > 0)


def create_separate_day_month_year_col(df_311):
    time_fmt = "dd/MM/yyyy HH:mm:ss"

    change_format_month = when(to_timestamp(df_311.Closed_Date, time_fmt).isNull(),
                               substring('Closed_Date', 1, 2).cast(IntegerType())).otherwise(
        substring('Closed_Date', 4, 2).cast(IntegerType()))
    df_with_month = df_311.withColumn('Closing_Month', change_format_month)

    change_format_month = when(to_timestamp(df_311.Created_Date, time_fmt).isNull(),
                               substring('Created_Date', 1, 2).cast(IntegerType())).otherwise(
        substring('Created_Date', 4, 2).cast(IntegerType()))
    df_with_month = df_with_month.withColumn('Creation_Month', change_format_month)

    change_format_day = when(to_timestamp(df_311.Closed_Date, time_fmt).isNull(),
                             substring('Closed_Date', 4, 2).cast(IntegerType())).otherwise(
        substring('Closed_Date', 1, 2).cast(IntegerType()))
    df_with_day = df_with_month.withColumn('Closing_Day', change_format_day)
    change_format_day = when(to_timestamp(df_311.Created_Date, time_fmt).isNull(),
                             substring('Created_Date', 4, 2).cast(IntegerType())).otherwise(
        substring('Created_Date', 1, 2).cast(IntegerType()))
    df_with_day = df_with_day.withColumn('Creation_Day', change_format_day)

    df_with_time = df_with_day.withColumn('Creation_Time', substring('Created_Date', 12, 11)).withColumn(
        'Closing_Time', substring('Closed_Date', 12, 11))

    df_with_time = df_with_time.withColumn('Creation_Hour', when(
        (substring('Creation_Time', 10, 2) == 'AM') | (substring('Creation_Time', 1, 2).cast(IntegerType()) == 12),
        substring('Creation_Time', 1, 2).cast(IntegerType())).otherwise(
        substring('Creation_Time', 1, 2).cast(IntegerType()) + 12))

    df_with_time = df_with_time.withColumn('Closing_Hour', when(
        (substring('Closing_Time', 10, 2) == 'AM') | (substring('Closing_Time', 1, 2).cast(IntegerType()) == 12),
        substring('Closing_Time', 1, 2).cast(IntegerType())).otherwise(
        substring('Closing_Time', 1, 2).cast(IntegerType()) + 12))

    df_with_time = df_with_time.withColumn('Creation_Hour', when(
        (col('Creation_Hour') == 12) & (substring('Creation_Time', 10, 2) == 'AM'), 0).otherwise(col('Creation_Hour')))
    df_with_year_month_day = df_with_time.withColumn('Closing_Hour', when(
        (col('Closing_Hour') == 12) & (substring('Closing_Time', 10, 2) == 'AM'), 0).otherwise(col('Closing_Hour')))

    return df_with_year_month_day


def filter_frequent_request_types(df_311):
    df_house_hold_cleaning_issues = df_311.filter(df_311.Complaint_Type.isin(
        Constants.HOUSE_HOLD_CLEANING_ISSUES)).withColumn('Issue_Category', lit('HOUSE_HOLD_CLEANING_ISSUES'))
    df_noise_issues = df_311.filter(df_311.Complaint_Type.isin(Constants.NOISE_ISSUES)).withColumn('Issue_Category',
                                                                                                   lit('NOISE_ISSUES'))
    df_vehicles_and_parking_issues = df_311.filter(
        df_311.Complaint_Type.isin(Constants.VEHICLES_AND_PARKING_ISSUE)).withColumn('Issue_Category',
                                                                                     lit('VEHICLES_AND_PARKING_ISSUE'))
    return df_house_hold_cleaning_issues.union(df_noise_issues).union(df_vehicles_and_parking_issues)
