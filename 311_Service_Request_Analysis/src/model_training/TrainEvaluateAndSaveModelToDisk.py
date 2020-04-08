# Python source script for Training, Evaluating and Saving the Model to disk.
import datetime
import os

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import when, col

import Constants
import Utilities as utilFor311


def directly_read_prepared_data(path):
    return utilFor311.init_spark().read.csv(path, inferSchema=True, header=True)


def prepare_data_for_supervised_learning(nyc_311_df_supervised):
    print(datetime.datetime.now())
    nyc_311_df_supervised = nyc_311_df_supervised.drop('created_date')
    nyc_311_df_supervised.cache()

    # Code to make categorical data into columns (Agenct, Borough, Complaint Type, Channel)
    agencies = nyc_311_df_supervised.select("Agency").distinct().rdd.flatMap(lambda x: x).collect()
    boroughs = nyc_311_df_supervised.select("Borough").distinct().rdd.flatMap(lambda x: x).collect()
    complain_types = nyc_311_df_supervised.select("complaint_type").distinct().rdd.flatMap(lambda x: x).collect()
    open_data_channel_types = nyc_311_df_supervised.select("open_data_channel_type").distinct().rdd.flatMap(
        lambda x: x).collect()

    # filling new column with value 1 if belong to particular category
    agencies_expr = [when(col("Agency") == ty, 1).otherwise(0).alias("e_AGENCY_" + ty) for ty in agencies]
    boroughs_expr = [when(col("Borough") == code, 1).otherwise(0).alias("e_BOROUGH_" + code) for code in boroughs]
    complain_types_expr = [when(col("complaint_type") == ty, 1).otherwise(0).alias("e_COMPLAIN_TYPE_" + ty) for ty
                           in complain_types]
    open_data_channel_types_expr = [
        when(col("open_data_channel_type") == code, 1).otherwise(0).alias("e_CHANNEL_TYPE_" + code) for code in
        open_data_channel_types]

    final_nyc_311_df_supervised = nyc_311_df_supervised.select("Creation_Month", "Creation_Day", "Creation_Hour",
                                                               'time_to_resolve_in_hrs',
                                                               *agencies_expr + boroughs_expr + complain_types_expr + open_data_channel_types_expr)

    # Save new csv for prepared data to be used in model Learning
    final_nyc_311_df_supervised.coalesce(1).write.format('com.databricks.spark.csv').save(
        './311dataset/final_nyc_311_df_supervised.csv', header='true')
    print(datetime.datetime.now())
    return final_nyc_311_df_supervised


# Feature vector
def prepare_feature_vector(final_nyc_311_df_supervised):
    assembler = VectorAssembler(
        inputCols=['Creation_Month', 'Creation_Day', 'Creation_Hour', 'e_AGENCY_HPD', 'e_AGENCY_NYPD', 'e_AGENCY_DEP',
                   'e_AGENCY_DSNY', 'e_AGENCY_DOITT', 'e_BOROUGH_UNSPECIFIED', 'e_BOROUGH_BROOKLYN', 'e_BOROUGH_BRONX',
                   'e_BOROUGH_MANHATTAN', 'e_BOROUGH_STATEN ISLAND', 'e_COMPLAIN_TYPE_UNSANITARY CONDITION',
                   'e_COMPLAIN_TYPE_Illegal Parking', 'e_COMPLAIN_TYPE_Noise - Residential',
                   'e_COMPLAIN_TYPE_Noise - Commercial', 'e_COMPLAIN_TYPE_Water System',
                   'e_COMPLAIN_TYPE_Blocked Driveway', 'e_COMPLAIN_TYPE_HEAT/HOT WATER',
                   'e_COMPLAIN_TYPE_PAINT/PLASTER', 'e_COMPLAIN_TYPE_PAINT/PLASTER', 'e_COMPLAIN_TYPE_Noise',
                   'e_COMPLAIN_TYPE_Request Large Bulky Item Collection', 'e_COMPLAIN_TYPE_PLUMBING',
                   'e_COMPLAIN_TYPE_WATER LEAK', 'e_COMPLAIN_TYPE_Noise - Street/Sidewalk', 'e_CHANNEL_TYPE_MOBILE',
                   'e_CHANNEL_TYPE_UNKNOWN', 'e_CHANNEL_TYPE_OTHER', 'e_CHANNEL_TYPE_PHONE', 'e_CHANNEL_TYPE_ONLINE'],
        outputCol="features")
    output = assembler.transform(final_nyc_311_df_supervised)
    x = output.select("features", "time_to_resolve_in_hrs").withColumnRenamed("time_to_resolve_in_hrs", "label")
    return x


def split_train_test_set(feature_vector_with_labels):
    return feature_vector_with_labels.randomSplit([0.8, 0.2], seed=12345)


def train_and_evaluate_model(model, model_name, hyper_param_grid, evaluator, train, test):
    tvs = CrossValidator(estimator=model,
                         estimatorParamMaps=hyper_param_grid,
                         evaluator=evaluator, )
    model = tvs.fit(train)
    predictions = model.transform(test)
    evaluate_and_save_model(model, model_name, predictions)


def evaluate_and_save_model(model, model_name, predictions):
    op_string = ""

    if model_name == 'Linear Regressor':
        op_string = op_string + model_name + " Results on Training Data" + os.linesep
        op_string = op_string + "=======================================" + os.linesep
        op_string = op_string + "RootMeanSquared Error: " + str(
            model.bestModel.summary.rootMeanSquaredError) + os.linesep
        op_string = op_string + "R2: " + str(model.bestModel.summary.r2) + os.linesep
        op_string = op_string + "Best Regularization Parameter - " + str(
            model.bestModel._java_obj.getRegParam()) + os.linesep
        op_string = op_string + "Best MaxIteration Parameter - " + str(
            model.bestModel._java_obj.getMaxIter()) + os.linesep

    if model_name != 'Linear Regressor':
        op_string = op_string + "Feature Importance - " + str(model.bestModel.featureImportances) + os.linesep
        if model_name == 'Random Forest Regressor':
            op_string = op_string + "Best Number of Trees Parameter - " + str(
                model.bestModel.getNumTrees) + os.linesep
            op_string = op_string + "Decision Trees Created - " + str(model.bestModel.trees) + os.linesep
            op_string = op_string + "Decision Trees Weights - " + str(model.bestModel.treeWeights) + os.linesep
        if model_name == 'Gradient Boost Regressor':
            op_string = op_string + "Best MaxIteration Parameter - " + str(
                model.bestModel._java_obj.parent().getMaxIter()) + os.linesep
            op_string = op_string + "Number of Decision Trees - " + str(model.bestModel.getNumTrees) + os.linesep
            op_string = op_string + "Decision Trees Created - " + str(model.bestModel.trees) + os.linesep
            op_string = op_string + "Decision Trees Weights - " + str(model.bestModel.treeWeights) + os.linesep

    op_string = op_string + os.linesep

    op_string = op_string + model_name + " Results on Test Data" + os.linesep
    op_string = op_string + "=======================================" + os.linesep
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse_val = evaluator.evaluate(predictions)
    op_string = op_string + "RootMeanSquared Error: " + str(rmse_val) + os.linesep
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
    r2 = evaluator.evaluate(predictions)
    op_string = op_string + "R2: " + str(r2) + os.linesep
    op_string = op_string + "20 Sample Records for Predictions" + os.linesep
    op_string = op_string + str(predictions.select("features", "label", "prediction").take(20)) + os.linesep

    filename = Constants.RESULTS_FOLDER_ANALYSIS_SUPERVISED_LEARNING + model_name + "Results.txt"

    if os.path.exists(filename):
        append_write = 'a'
    else:
        append_write = 'w'
    text_file = open(filename, append_write)
    text_file.write(op_string)
    text_file.close()
    save_model_to_disk(model, model_name)


def save_model_to_disk(model, model_name):
    model.save(Constants.RESULTS_FOLDER_ANALYSIS_SUPERVISED_LEARNING + str(model_name))


def train_linear_regressor(feature_vector_with_labels):
    train, test = split_train_test_set(feature_vector_with_labels)
    lr = LinearRegression()

    # Prepare Parameter Grid for Hyperparameter Search
    hyperparam_grid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.1, 0.01]) \
        .addGrid(lr.maxIter, [100, 200, 300]) \
        .build()

    train_and_evaluate_model(lr, 'Linear Regressor', hyperparam_grid, RegressionEvaluator(), train, test)


def train_gradient_boost_regressor(feature_vector_with_labels):
    train, test = split_train_test_set(feature_vector_with_labels)
    gb = GBTRegressor()

    # Prepare Parameter Grid for Hyperparameter Search
    hyperparam_grid = ParamGridBuilder() \
        .addGrid(gb.maxIter, [10, 20]) \
        .build()

    train_and_evaluate_model(gb, 'Gradient Boost Regressor', hyperparam_grid, RegressionEvaluator(), train, test)


def train_random_forest(feature_vector_with_labels):
    train, test = split_train_test_set(feature_vector_with_labels)
    rf = RandomForestRegressor()

    # Prepare Parameter Grid for Hyperparameter Search
    hyperparam_grid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [70, 120]) \
        .build()

    train_and_evaluate_model(rf, 'Random Forest Regressor', hyperparam_grid, RegressionEvaluator(), train, test)
