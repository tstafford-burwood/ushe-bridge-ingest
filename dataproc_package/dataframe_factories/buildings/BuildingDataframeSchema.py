from pyspark.sql.types import FloatType, StringType, StructField, StructType

building_schema = StructType(
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


if __name__ == "__main__":
    # Output the current struct type as a json schema.
    # Use this to generate a new schema JSON file.
    building_schema_json = building_schema.json()
    with open("building_schema.json", "w") as f:
        f.write(building_schema_json)
