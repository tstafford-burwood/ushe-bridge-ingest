import pytest
from pyspark.sql.types import FloatType, StringType, StructField, StructType


@pytest.fixture()
def expected_schema():
    expected_schema = StructType(
        [
            StructField("B_INST", StringType()),
            StructField("B_LOCATION", StringType()),
            StructField("B_OWNERSHIP", StringType()),
            StructField("B_YEAR", StringType()),
            StructField("B_NAME", StringType()),
            StructField("B_NUMBER", StringType()),
            StructField("B_SNAME", StringType()),
            StructField("B_YEAR_CONS", StringType()),
            StructField("B_YEAR_REM", StringType()),
            StructField("B_REPLACE_COST", FloatType()),
            StructField("B_CONDITION", StringType()),
            StructField("B_GROSS", FloatType()),
            StructField("B_COST_MYR", FloatType()),
            StructField("B_RSKNBR", StringType()),
            StructField("B_AUX", StringType()),
            StructField("B_KEY", StringType()),
        ]
    )

    return expected_schema
