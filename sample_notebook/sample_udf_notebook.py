from pyspark import SparkContext
from pyspark.sql import SparkSession


def init_spark():
    sql = SparkSession.builder \
        .appName("trip-app") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar") \
        .getOrCreate()
    sc = sql.sparkContext
    return sql, sc


def create_sample_df():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([('1', 'Visarut'), ('2', 'Tanin'), ('3', 'Nithiphan')], ['id', 'first_name'])
    df.show(truncate=False)

    return df


create_sample_df()
