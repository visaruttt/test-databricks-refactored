# Databricks notebook source
# MAGIC %run ../sample/simple_udf_notebook

# COMMAND ----------

import unittest
import coverage
import xmlrunner
import pytest
from pyspark.sql import SparkSession
# from sample.simple_udf_notebook import simple_udf_notebook #for running remotely
# this example for running on Databricks
# this is to test a function that calling spark inside.

class SimpleUDFTest(unittest.TestCase):

    def test_simple_udf_notebook(self):
        self.assertEqual(len(simple_udf_notebook().columns), 2)
        self.assertNotEqual(len(simple_udf_notebook().columns), 10)


cov = coverage.Coverage("/tmp/.coverage")
cov.start()

suite = unittest.TestLoader().loadTestsFromTestCase(SimpleUDFTest)
runner = xmlrunner.XMLTestRunner(output='/tmp/testreport.xml')
runner.run(suite)

cov.stop()
cov.save()
cov.html_report(directory='/tmp/covhtml')

# COMMAND ----------


