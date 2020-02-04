from pyspark.sql import functions as f


def get_missing_value_count(df_311):
    return df_311.select([f.count(f.when(f.isnull(c), c)).alias(c) for c in df_311.columns])


def drop_unwanted_cols(df_311):
    drop_list = ['Agency Name', 'Incident Address', 'Street Name', 'Cross Street 1', 'Cross Street 2',
                 'Intersection Street 1', 'Intersection Street 2', 'Landmark', 'Facility Type',
                 'Resolution Description', 'Resolution Action Updated Date', 'Community Board', 'BBL',
                 'X Coordinate (State Plane)', 'Y Coordinate (State Plane)', 'Park Facility Name', 'Park Borough',
                 'Vehicle Type', 'Taxi Company Borough', 'Taxi Pick Up Location', 'Bridge Highway Name',
                 'Bridge Highway Direction', 'Road Ramp', 'Bridge Highway Segment', 'Latitude', 'Longitude', 'Location']
    return df_311.drop(*drop_list)


def remove_space_from_col_names(df_311):
    return (df_311.withColumnRenamed('Unique Key', 'Unique_Key').withColumnRenamed('Created Date', 'Created_Date')
            .withColumnRenamed('Closed Date', 'Closed_Date')
            .withColumnRenamed('Due Date', 'Due_Date').withColumnRenamed('Address Type', 'Address_Type')
            .withColumnRenamed('Location Type', 'Location_Type').withColumnRenamed('Incident Zip', 'Incident_Zip')
            .withColumnRenamed('Complaint Type', 'Complaint_Type')
            .withColumnRenamed('Open Data Channel Type', 'Open_Data_Channel_Type'))


def calculate_time_to_resolve(df_311):
    time_fmt = "dd/MM/yyyy HH:mm:ss"
    time_diff = (f.unix_timestamp('Closed_Date', format=time_fmt)
                 - f.unix_timestamp('Created_Date', format=time_fmt))
    return df_311.withColumn("time_to_resolve", time_diff)
