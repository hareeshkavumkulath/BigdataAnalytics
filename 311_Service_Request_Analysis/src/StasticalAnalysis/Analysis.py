# Python source script for Analysis Part
from pyspark.sql.types import IntegerType

import Utilities as utilFor311


def monthly_hourly_analysis(df_with_month_hour):
    # Insight 2 : Daily, Hourly, Monthly Analysis
    df_with_month_hour.cache()
    df_with_month_hour = df_with_month_hour.withColumn("Creation_Hour",
                                                       df_with_month_hour["Creation_Hour"].cast(IntegerType()))
    df_with_month_hour = df_with_month_hour.withColumn("Creation_Month",
                                                       df_with_month_hour["Creation_Month"].cast(IntegerType()))
    df_with_month_hour = df_with_month_hour.withColumn("Creation_Day",
                                                       df_with_month_hour["Creation_Day"].cast(IntegerType()))

    df_house_hold_cleaning_issues = df_with_month_hour.filter(
        df_with_month_hour.Issue_Category == 'HOUSE_HOLD_CLEANING_ISSUES')
    df_noise_issues = df_with_month_hour.filter(df_with_month_hour.Issue_Category == 'NOISE_ISSUES')
    df_vehicles_and_parking_issues = df_with_month_hour.filter(
        df_with_month_hour.Issue_Category == 'VEHICLES_AND_PARKING_ISSUE')

    # Hourly
    utilFor311.prepare_plot(df_house_hold_cleaning_issues, 'Creation_Hour',
                            "Cleaning & Household Complaints count hourly basis",
                            "Hour", "Count", 1, range(0, 24, 1))
    utilFor311.prepare_plot(df_noise_issues, 'Creation_Hour', "Noise Complaints count hourly basis", "Hour", "Count", 2,
                            range(0, 24, 1))
    utilFor311.prepare_plot(df_vehicles_and_parking_issues, 'Creation_Hour',
                            "Vehicle and Parking Complaints count hourly basis",
                            "Hour", "Count", 3, range(0, 24, 1))
    # Daily
    utilFor311.prepare_plot(df_house_hold_cleaning_issues, 'Creation_Day',
                            "Cleaning & Household Complaints count daily basis", "Day", "Count", 4, range(1, 32, 1))
    utilFor311.prepare_plot(df_noise_issues, 'Creation_Day', "Noise Complaints count daily basis", "Day", "Count", 5,
                            range(1, 32, 1))
    utilFor311.prepare_plot(df_vehicles_and_parking_issues, 'Creation_Day',
                            "Vehicle and Parking Complaints count daily basis", "Day", "Count", 6, range(1, 32, 1))

    # Monthly
    months = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
    utilFor311.prepare_plot(df_house_hold_cleaning_issues, 'Creation_Month',
                            "Cleaning & Household Complaints count monthly basis",
                            "Month", "Count", 7, range(1, 13, 1), months)
    utilFor311.prepare_plot(df_noise_issues, 'Creation_Month', "Noise Complaints count monthly basis", "Month", "Count",
                            8,
                            range(1, 13, 1), months)
    utilFor311.prepare_plot(df_vehicles_and_parking_issues, 'Creation_Month',
                            "Vehicle and Parking Complaints count monthly basis",
                            "Month", "Count", 9, range(1, 13, 1), months)