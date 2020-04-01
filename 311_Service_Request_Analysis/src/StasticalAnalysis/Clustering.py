import os

import matplotlib.pyplot as plt
from numpy import zeros
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.sql import Row
from pyspark.sql.functions import collect_list

import Constants
import Utilities as utilFor311


def get_actual_zip_complaint_map(cleaned_df, sc):
    zip_wise_complaint_count_rdd = sc.parallelize(
        cleaned_df.select('Incident_Zip', 'Complaint_Type').groupBy('Incident_Zip', 'Complaint_Type').count().collect())
    max_complaint_count = zip_wise_complaint_count_rdd.takeOrdered(1, key=lambda x: -x[2])[0].__getitem__('count')
    zip_wise_complaint_count_rdd = zip_wise_complaint_count_rdd.map(lambda row: (row[0], {row[1]: row[2]}))
    zip_wise_complaint_count_rdd = zip_wise_complaint_count_rdd.groupByKey().mapValues(list)
    zip_wise_complaint_count_rdd = zip_wise_complaint_count_rdd.mapValues(utilFor311.list_to_dict)
    return zip_wise_complaint_count_rdd, max_complaint_count


def get_full_zip_complaint_map(cleaned_df, sc):
    unq_zipcodes_rdd = sc.parallelize(cleaned_df.select('Incident_Zip').distinct().collect())
    unq_complaint_types_rdd = sc.parallelize(sorted(Constants.HOUSE_HOLD_CLEANING_ISSUES +
                                                    Constants.VEHICLES_AND_PARKING_ISSUE
                                                    + Constants.NOISE_ISSUES))
    unq_complaint_types_rdd = unq_complaint_types_rdd.map(lambda complaint: {complaint: 0})
    zipcode_complaint_full_pair_rdd = unq_zipcodes_rdd.cartesian(unq_complaint_types_rdd)
    zipcode_complaint_full_group_rdd = zipcode_complaint_full_pair_rdd.groupByKey().mapValues(list)
    zipcode_complaint_full_group_rdd = zipcode_complaint_full_group_rdd.mapValues(utilFor311.list_to_dict).map(
        lambda row: (row[0].__getitem__("Incident_Zip"), row[1]))
    return zipcode_complaint_full_group_rdd


def prepare_data_for_clustering(cleaned_df):
    spark = utilFor311.init_spark()
    sc = spark.sparkContext
    zip_wise_complaint_count_rdd, max_complaint_count = get_actual_zip_complaint_map(cleaned_df, sc)
    zipcode_complaint_full_group_rdd = get_full_zip_complaint_map(cleaned_df, sc)
    zipcode_complaint_mapping_rdd = zipcode_complaint_full_group_rdd.join(zip_wise_complaint_count_rdd)
    zipcode_complaint_mapping_rdd = zipcode_complaint_mapping_rdd.map(
        lambda row: (row[0], {**row[1][0], **row[1][1]}))
    complaints_vector_rdd = zipcode_complaint_mapping_rdd.map(lambda x: (x[0], list(x[1].values())))
    df_kmeans = complaints_vector_rdd.map(
        lambda row: Row(zip_code=row[0], a=row[1][0], b=row[1][1], c=row[1][2], d=row[1][3],
                        e=row[1][4], f=row[1][5], g=row[1][6], h=row[1][7], i=row[1][8],
                        j=row[1][9], k=row[1][10], l=row[1][11], m=row[1][12])
    ).toDF().select('zip_code', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm')
    return df_kmeans


def prepare_feature_vector(df_kmeans):
    feature_cols = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm']
    vector_assembler_complaints = VectorAssembler(inputCols=feature_cols, outputCol="features")
    final_df_kmeans = vector_assembler_complaints.transform(df_kmeans).select('zip_code', 'features')
    scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
    scaler_model = scaler.fit(final_df_kmeans.select("features"))
    final_df_kmeans = scaler_model.transform(final_df_kmeans).select('zip_code', 'scaled_features').withColumnRenamed(
        'scaled_features', 'features')
    return final_df_kmeans


def run_k_means(final_df_kmeans):
    cost = zeros(20)
    for k in range(2, 20):
        kmeans = KMeans().setK(k).setSeed(123).setFeaturesCol("features")
        model = kmeans.fit(final_df_kmeans.sample(False, 0.1, seed=123))
        # noinspection PyDeprecation
        # sum of squared distances of points to their nearest center
        cost[k] = model.computeCost(final_df_kmeans)
    return cost


def plot_elbow_curve(cost):
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    ax.plot(range(2, 20), cost[2:20])
    ax.set_xlabel('k')
    ax.set_ylabel('cost')
    plt.savefig(Constants.RESULTS_FOLDER_ANALYSIS_CLUSTERING + 'ElbowCurve' + '.png')


def run_kmeans_with_optimal_number_of_cluster(final_df_kmeans):
    opt_clusters = 8
    kmeans = KMeans().setK(opt_clusters).setSeed(123).setFeaturesCol("features")
    model = kmeans.fit(final_df_kmeans)
    return model


def get_zip_code_assignment_to_clusters(model, final_df_kmeans):
    zip_code_with_clusters = model.transform(final_df_kmeans).select('zip_code', 'prediction')
    zip_code_with_clusters = zip_code_with_clusters.groupBy('prediction').agg(collect_list("zip_code")).orderBy(
        'prediction')
    result = zip_code_with_clusters.withColumnRenamed('prediction', 'Cluster').withColumnRenamed(
        'collect_list(zip_code)',
        'Zip_Codes').collect()
    return result


def save_clustering_results(zip_code_clusters):
    op_string = ""
    for entry in zip_code_clusters:
        cluster = entry.__getitem__('Cluster')
        zip_codes = entry.__getitem__('Zip_Codes')
        op_string = op_string + 'Zip Codes in Cluster: ' + str(cluster) + os.linesep + str(zip_codes)[1:-1] + os.linesep
    filename = Constants.RESULTS_FOLDER_ANALYSIS_CLUSTERING + "ClusteringResults.txt"
    text_file = open(filename, "w")
    text_file.write(op_string)
    text_file.close()
