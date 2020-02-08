from pyspark.sql import functions as f
import Constants
import Utilities as utilFor311


def get_missing_value_count(df_311):
    return df_311.select([f.count(f.when((f.isnull(c) | (f.col(c) == '')), c)).alias(c) for c in df_311.columns])


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
    df_311 = df_311.filter((f.col('City').isNotNull()) & (f.col('City') != "") & (f.col('Closed_Date').isNotNull()) & (
            f.col('Closed_Date') != ""))
    return df_311


def update_burrow_city_from_zip_code(df_311):
    areas = ['City', 'Borough']
    for area in areas:
        df_311 = df_311.withColumn(area, f.when(
            (f.col("Incident_Zip") > 10450.0) & (f.col("Incident_Zip") < 10475.0) & (f.col(area).isNull()),
            "BRONX").otherwise(f.col(area)))
        df_311 = df_311.withColumn(area, f.when(
            (f.col("Incident_Zip") > 11200.0) & (f.col("Incident_Zip") < 11240.0) & (f.col(area).isNull()),
            "BROOKLYN").otherwise(f.col(area)))
        df_311 = df_311.withColumn(area, f.when(
            (f.col("Incident_Zip") > 10000.0) & (f.col("Incident_Zip") < 10280.0) & (f.col(area).isNull()),
            "MANHATTAN").otherwise(f.col(area)))
        df_311 = df_311.withColumn(area, f.when(
            (f.col("Incident_Zip") > 10300.0) & (f.col("Incident_Zip") < 10315.0) & (f.col(area).isNull()),
            "STATEN ISLAND").otherwise(f.col(area)))
        df_311 = df_311.withColumn(area, f.when(
            (f.col("Incident_Zip") > 11350.0) & (f.col("Incident_Zip") < 11700.0) & (f.col(area).isNull()),
            "QUEENS").otherwise(f.col(area)))
    return df_311


def capitalize_cols(df_311, cols):
    for c in cols:
        df_311 = df_311.withColumn(c, f.upper(f.col(c)))
    return df_311


def format_zip_code(df_311):
    df_311 = df_311.withColumn('Incident_Zip', f.substring('Incident_Zip', 0, 5).cast('float'))
    return df_311


def calculate_time_to_resolve_in_seconds(df_311):
    time_fmt = "dd/MM/yyyy HH:mm:ss"
    time_fmt2 = "MM/dd/yyyy HH:mm:ss"
    time_diff = f.when(f.to_timestamp(df_311.Closed_Date, time_fmt).isNull(), f.unix_timestamp('Closed_Date',
                                                                                               format=time_fmt2) - f.unix_timestamp(
        'Created_Date', format=time_fmt)).otherwise(
        f.unix_timestamp('Closed_Date', format=time_fmt) - f.unix_timestamp('Created_Date', format=time_fmt))
    return df_311.withColumn("time_to_resolve", time_diff)