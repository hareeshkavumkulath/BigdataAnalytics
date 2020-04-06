# Python source script for Analysis Part
from pyspark.sql.types import IntegerType

import Constants
import Utilities as utilFor311


def get_top_boroughs(complaint_type_df):
    boroughs = complaint_type_df.groupBy('Borough').count().filter('count > 5000').select(
        'Borough').distinct().collect()
    top_boroughs = []
    for item in boroughs:
        top_boroughs.append(item[0])
    return top_boroughs


def plot_top_boroughs_complaint_wise(complaint_type_df, year):
    top_boroughs = get_top_boroughs(complaint_type_df)
    results_folder = Constants.RESULTS_FOLDER_ANALYSIS_Q1
    fig_num = 2
    for i in range(len(top_boroughs)):
        nyc_311_df_borough = complaint_type_df.filter(complaint_type_df['Borough'] == top_boroughs[i])
        utilFor311.prepare_plot(nyc_311_df_borough, 'Complaint_Type', 'count',
                                "Complaint Wise Distribution - " + top_boroughs[i] + " - " + str(year),
                                "Complaint Types", "Count", fig_num, results_folder, x_tick_rotation='vertical')
        fig_num += 1


def plot_complaint_wise_distribution(complaint_type_df, creation_year):
    results_folder = Constants.RESULTS_FOLDER_ANALYSIS_Q1
    utilFor311.prepare_plot(complaint_type_df, 'Complaint_Type', 'count',
                            "Complaint Wise Distribution - " + str(creation_year), "Complaint Types", "Count", 1,
                            results_folder, x_tick_rotation='vertical')


def get_creation_year(df):
    return df.select("Created_Date").take(1)[0].__getitem__("Created_Date")[6:10]


def complaint_type_analysis(complaint_type_df):
    creation_year = get_creation_year(complaint_type_df)
    plot_complaint_wise_distribution(complaint_type_df, creation_year)
    plot_top_boroughs_complaint_wise(complaint_type_df, creation_year)


def monthly_hourly_analysis(df_with_month_hour):
    # Insight 2 : Daily, Hourly, Monthly Analysis
    df_with_month_hour.cache()
    results_folder = Constants.RESULTS_FOLDER_ANALYSIS_Q2
    creation_year = get_creation_year(df_with_month_hour)
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
    utilFor311.prepare_plot(df_house_hold_cleaning_issues, 'Creation_Hour', 'count',
                            "Cleaning & Household Complaints count hourly basis. Year-" + str(creation_year),
                            "Hour", 'Total Count (across the year)', 1, results_folder, range(0, 24, 1))
    utilFor311.prepare_plot(df_noise_issues, 'Creation_Hour', 'count',
                            "Noise Complaints count hourly basis. Year-" + str(creation_year), "Hour",
                            'Total Count (across the year)', 2, results_folder, range(0, 24, 1))
    utilFor311.prepare_plot(df_vehicles_and_parking_issues, 'Creation_Hour', 'count',
                            "Vehicle and Parking Complaints count hourly basis. Year-" + str(creation_year),
                            "Hour", 'Total Count (across the year)', 3, results_folder, range(0, 24, 1))
    # Daily
    utilFor311.prepare_plot(df_house_hold_cleaning_issues, 'Creation_Day', 'count',
                            "Cleaning & Household Complaints count daily basis. Year-" + str(creation_year), "Day",
                            'Total Count (across the year)', 4, results_folder, range(1, 32, 1))
    utilFor311.prepare_plot(df_noise_issues, 'Creation_Day', 'count',
                            "Noise Complaints count daily basis", "Day",
                            'Total Count (across the year)', 5, results_folder,
                            range(1, 32, 1))
    utilFor311.prepare_plot(df_vehicles_and_parking_issues, 'Creation_Day', 'count',
                            "Vehicle and Parking Complaints count daily basis. Year-" + str(creation_year), "Day",
                            'Total Count (across the year)', 6, results_folder, range(1, 32, 1))

    # Monthly
    months = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
    utilFor311.prepare_plot(df_house_hold_cleaning_issues, 'Creation_Month', 'count',
                            "Cleaning & Household Complaints count monthly basis. Year-" + str(creation_year),
                            "Month", 'Total Count (across the year)', 7, results_folder, range(1, 13, 1), months)
    utilFor311.prepare_plot(df_noise_issues, 'Creation_Month', 'count',
                            "Noise Complaints count monthly basis. Year-" + str(creation_year), "Month",
                            'Total Count (across the year)', 8, results_folder, range(1, 13, 1), months)
    utilFor311.prepare_plot(df_vehicles_and_parking_issues, 'Creation_Month', 'count',
                            "Vehicle and Parking Complaints count monthly basis. Year-" + str(creation_year),
                            "Month", 'Total Count (across the year)', 9, results_folder, range(1, 13, 1), months)


def get_top_agencies(df):
    agencies = df.select("Agency").distinct().collect()
    top_agencies = []
    for item in agencies:
        top_agencies.append(item[0])
    return top_agencies


def complaint_type_to_time_resolve(df_with_time_to_resolve, creation_year):
    results_folder = Constants.RESULTS_FOLDER_ANALYSIS_Q3
    top_agencies = get_top_agencies(df_with_time_to_resolve)
    all_agency_time_to_resolve = df_with_time_to_resolve.select('Complaint_Type', 'Agency',
                                                                df_with_time_to_resolve.time_to_resolve_in_hrs.cast(
                                                                    'float').alias('time_to_resolve_in_hrs'))
    fig_num = 1
    for i in range(len(top_agencies)):
        utilFor311.prepare_plot(
            all_agency_time_to_resolve.filter(all_agency_time_to_resolve["Agency"] == top_agencies[i]),
            'Complaint_Type', 'time_to_resolve_in_hrs',
            "Agency: " + top_agencies[i] + " average completion time for year " + str(creation_year),
            "Complaint_Type", "Average time_to_resolve_in_hrs", fig_num, results_folder, x_tick_rotation='vertical',
            is_count=False)
        fig_num += 1


def resolution_time_analysis(df_with_time_to_resolve):
    creation_year = get_creation_year(df_with_time_to_resolve)
    complaint_type_to_time_resolve(df_with_time_to_resolve, creation_year)
