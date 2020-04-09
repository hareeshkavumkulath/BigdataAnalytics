# Main Launcher script
import datetime
import sys

import Constants
import Utilities as utilFor311
from StasticalAnalysis import Analysis
from StasticalAnalysis import Clustering
from data_cleaning import DataCleaner
from model_training import TrainEvaluateAndSaveModelToDisk


def get_cleaned_data(read_cleaned_csv, file_name):
    if file_name == "":
        file_name = Constants.FILENAME
    if not read_cleaned_csv:
        df_311 = utilFor311.read_data_from_csv(file_name)
        df_311 = DataCleaner.remove_space_from_col_names(df_311)
        df_311 = DataCleaner.drop_unwanted_cols(df_311, Constants.DROP_COLS1)
        df_311 = DataCleaner.drop_below_threshold(df_311)
        df_311 = DataCleaner.filter_frequent_request_types(df_311)
        df_311 = DataCleaner.capitalize_cols(df_311, Constants.CAPITALIZE_COLS)
        df_311 = DataCleaner.format_zip_code(df_311)
        df_311 = DataCleaner.update_burrow_city_from_zip_code(df_311)
        df_311 = DataCleaner.drop_empty_null_values(df_311)
        df_311 = DataCleaner.calculate_time_to_resolve_in_seconds(df_311)
        cleaned_df = DataCleaner.create_separate_day_month_year_col(df_311)
        cleaned_df = utilFor311.rearrange_cols(cleaned_df)
    else:
        cleaned_df = utilFor311.read_data_from_csv(file_name)
    return cleaned_df


def run_analysis(cleaned_df):
    # Trend based questions
    Analysis.complaint_type_analysis(cleaned_df)
    Analysis.monthly_hourly_analysis(cleaned_df)
    Analysis.resolution_time_analysis(cleaned_df)
    #
    # # Clustering
    df_kmeans = Clustering.prepare_data_for_clustering(cleaned_df)
    final_df_kmeans = Clustering.prepare_feature_vector(df_kmeans)
    model_costs = Clustering.run_k_means(final_df_kmeans)
    Clustering.plot_elbow_curve(model_costs)
    model = Clustering.run_kmeans_with_optimal_number_of_cluster(final_df_kmeans)
    zip_code_clusters = Clustering.get_zip_code_assignment_to_clusters(model, final_df_kmeans)
    Clustering.save_clustering_results(zip_code_clusters)
    #
    # # Supervised Learning
    # final_nyc_311_df_supervised = TrainEvaluateAndSaveModelToDisk.directly_read_prepared_data(
    #    './311dataset/final_nyc_311_df_supervised.csv')
    final_nyc_311_df_supervised = TrainEvaluateAndSaveModelToDisk.prepare_data_for_supervised_learning(cleaned_df)
    feature_vector_with_labels = TrainEvaluateAndSaveModelToDisk.prepare_feature_vector(final_nyc_311_df_supervised)
    TrainEvaluateAndSaveModelToDisk.train_linear_regressor(feature_vector_with_labels)
    TrainEvaluateAndSaveModelToDisk.train_gradient_boost_regressor(feature_vector_with_labels)
    TrainEvaluateAndSaveModelToDisk.train_random_forest(feature_vector_with_labels)


if __name__ == "__main__":
    directly_read_cleaned_csv = False
    filename = ""
    if len(sys.argv) == 3:
        if sys.argv[2] == 'True':
            directly_read_cleaned_csv = True
        filename = sys.argv[1]

    print(datetime.datetime.now())
    cleaned_311_df = get_cleaned_data(directly_read_cleaned_csv, filename)
    run_analysis(cleaned_311_df)
    print(datetime.datetime.now())
