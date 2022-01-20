# Databricks notebook source
# import pyspark.sql.functions as func
# from pyspark.sql import DataFrame


# def simple_udf_transform(input_df: DataFrame) -> DataFrame:
#    inter_df = input_df.where(input_df['that_column'] ==
#                              func.lit('hobbit')).groupBy('another_column').agg(
#        func.sum('yet_another').alias('new_column'))
#    output_df = inter_df.select('another_column', 'new_column',
#                                func.when(func.col('new_column') > 10, 'yes').otherwise('no').alias('indicator')).where(
#        func.col('indicator') == func.lit('yes'))
#    return output_df
%run ../sample/simple_udf_transform

# COMMAND ----------

import pytest
import coverage
import xmlrunner
import unittest
from pyspark.sql import SparkSession


def make_udf_test_on_databricks():
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

@pytest.mark.usefixtures("spark_session")
def make_udf_test_on_remotely(spark_session):
    df = spark_session.createDataFrame(
        [
            ('hobbit', 'Samwise', 5),
            ('hobbit', 'Billbo', 50),
            ('hobbit', 'Billbo', 20),
            ('wizard', 'Gandalf', 1000)
        ],
        ['that_column', 'another_column', 'yet_another']
    )
    return df


class SimpleUDFTransformTest(unittest.TestCase):
    
    def test_sample_transform(self):
        test_df = make_udf_test_on_databricks()
        new_df = simple_udf_transform(test_df)
        assert new_df.count() == 1
        assert new_df.toPandas().to_dict('list')['new_column'][0] == 70
        
cov = coverage.Coverage("/tmp/.coverage")
cov.start()

suite = unittest.TestLoader().loadTestsFromTestCase(SimpleUDFTransformTest)
runner = xmlrunner.XMLTestRunner(output='/tmp/testreport.xml')
runner.run(suite)

cov.stop()
cov.save()
cov.html_report(directory='/tmp/covhtml')

# COMMAND ----------


