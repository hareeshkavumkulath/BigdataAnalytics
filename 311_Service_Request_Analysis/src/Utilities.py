from pyspark.sql import SparkSession


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("NYC 311 Data Analysis") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def read_data_from_csv(filename):
    spark = init_spark()
    nyc_311_df = spark.read.format("csv").option("header", "true").load(filename)
    return nyc_311_df


def get_df_row_as_dict(df_row):
    return df_row.asDict()


def print_df_row_as_dict(df_row):
    for col in get_df_row_as_dict(df_row):
        print(col + ":" + str(df_row[col]))
