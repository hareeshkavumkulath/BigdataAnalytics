# Python source script for Analysis Part
import Constants
import Utilities as utilFor311


def monthly_hourly_analysis(df_with_year_month_day):
    # Insight 2 : Monthly and Hourly Analysis
    complaint_frequency_hour = df_with_year_month_day.groupby('Creation_Hour').count().orderBy(['Creation_Hour'],
                                                                                               ascending=[1]).collect()
    x, y = utilFor311.create_x_y_coordinates_for_group_by_results(complaint_frequency_hour, 'Creation_Hour')
    utilFor311.plot_chart_x_y(x, y, "Frequency of complaints per hour", "Hour", "Frequency", 1, range(0, 24, 1))

    complaint_frequency_month = df_with_year_month_day.groupby('Creation_Month').count().orderBy(['Creation_Month'],
                                                                                                 ascending=[
                                                                                                     1]).collect()
    x, y = utilFor311.create_x_y_coordinates_for_group_by_results(complaint_frequency_month, 'Creation_Month')

    utilFor311.plot_chart_x_y(x, y, "Frequency of complaints per month", "Month", "Frequency", 2, range(1, 13, 1),
                              Constants.MONTHS)

    utilFor311.show_plot()
