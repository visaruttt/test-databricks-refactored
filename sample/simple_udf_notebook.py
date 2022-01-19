# # Databricks notebook source
# from pyspark.sql import SparkSession
#
#
# def simple_udf_notebook():
#     spark = SparkSession.builder.getOrCreate()
#     df = spark.createDataFrame([('1', 'Visarut'), ('2', 'Tanin'), ('3', 'Nithiphan')], ['id', 'first_name'])
#     df.show(truncate=False)
#     return df
#
#
# simple_udf_notebook()
