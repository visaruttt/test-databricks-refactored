from sample.simple_udf_transform import simple_udf_transform
from pyspark.sql import SparkSession


def print_hi(name):
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


if __name__ == '__main__':
    print_hi('PyCharm')
    # spark = SparkSession.builder.getOrCreate()
    # df = spark.createDataFrame(
    #     [
    #         ('hobbit', 'Samwise', 5),
    #         ('hobbit', 'Billbo', 50),
    #         ('hobbit', 'Billbo', 20),
    #         ('wizard', 'Gandalf', 1000)
    #     ],
    #     ['that_column', 'another_column', 'yet_another']
    # )
    # df.show(truncate=False)
    # simple_udf_transform(df)
