import pytest
from sample.simple_udf_transform import simple_udf_transform

# pytestmark = pytest.mark.usefixtures("spark_session")


@pytest.mark.usefixtures("spark_session")
def test_sample_transform(spark_session):
    test_df = spark_session.createDataFrame(
        [
            ('hobbit', 'Samwise', 5),
            ('hobbit', 'Billbo', 50),
            ('hobbit', 'Billbo', 20),
            ('wizard', 'Gandalf', 1000)
        ],
        ['that_column', 'another_column', 'yet_another']
    )
    new_df = simple_udf_transform(test_df)
    assert new_df.count() == 1
    assert new_df.toPandas().to_dict('list')['new_column'][0] == 70
