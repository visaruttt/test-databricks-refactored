# Databricks notebook source
import pytest
from pyspark.shell import spark
from sample.simple_udf_transform import simple_udf_transform


# %run ../sample/simple_udf_transform
# COMMAND ----------
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


@pytest.mark.usefixtures("spark_session")
def test_sample_transform():
    test_df = make_udf_test_on_remotely(spark_session=spark)
    new_df = simple_udf_transform(test_df)
    assert new_df.count() == 1
    assert new_df.toPandas().to_dict('list')['new_column'][0] == 70


# def make_udf_test_on_databricks():
#     df = spark.createDataFrame(
#         [
#             ('hobbit', 'Samwise', 5),
#             ('hobbit', 'Billbo', 50),
#             ('hobbit', 'Billbo', 20),
#             ('wizard', 'Gandalf', 1000)
#         ],
#         ['that_column', 'another_column', 'yet_another']
#     )
#     return df

# test coverage for Databricks notebook
# cov = coverage.Coverage("/tmp/.coverage")
# cov.start()
#
# suite = unittest.TestLoader().loadTestsFromTestCase(SimpleUDFTransformTest)
# runner = xmlrunner.XMLTestRunner(output='/tmp/testreport.xml')
# runner.run(suite)
#
# cov.stop()
# cov.save()
# cov.html_report(directory='/tmp/covhtml')
