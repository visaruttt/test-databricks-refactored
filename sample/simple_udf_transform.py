import pyspark.sql.functions as func
from pyspark.sql import DataFrame


def simple_udf_transform(input_df: DataFrame) -> DataFrame:
    inter_df = input_df.where(input_df['that_column'] ==
                              func.lit('hobbit')).groupBy('another_column').agg(
        func.sum('yet_another').alias('new_column'))
    output_df = inter_df.select('another_column', 'new_column',
                                func.when(func.col('new_column') > 10, 'yes').otherwise('no').alias('indicator')).where(
        func.col('indicator') == func.lit('yes'))
    return output_df
