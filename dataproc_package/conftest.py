import pytest
from pyspark.sql import SparkSession


@pytest.fixture()
def spark_session():
    spark = SparkSession.builder.appName("Spark Test").getOrCreate()
    yield spark
