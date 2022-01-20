# Databricks notebook source
import pyspark.sql.functions as func
from pyspark.sql import DataFrame


def simple_udf_transform(input_df):
    inter_df = input_df.where(input_df['that_column'] ==
                             func.lit('hobbit')).groupBy('another_column').agg(
       func.sum('yet_another').alias('new_column'))

    output_df = inter_df.select('another_column', 'new_column',
                               func.when(func.col('new_column') > 10, 'yes').otherwise('no').alias('indicator')).where(
       func.col('indicator') == func.lit('yes'))
    return output_df


def make_test_on_databricks():
    df = spark.createDataFrame(
        [
            ('hobbit', 'Samwise', 5),
            ('hobbit', 'Billbo', 50),
            ('hobbit', 'Billbo', 20),
            ('wizard', 'Gandalf', 1000)
        ],
        ['that_column', 'another_column', 'yet_another']
    )
    return df

test_df = make_test_on_databricks()
output_df = simple_udf_transform(test_df)
output_df.display()

# COMMAND ----------


