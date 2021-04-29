import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local") \
        .appName("sales-test") \
        .config('spark.sql.sources.partitionOverwriteMode', 'dynamic') \
        .getOrCreate()
    yield spark
    spark.stop()
