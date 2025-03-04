from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

course_schema = StructType(
    [
        StructField("C_INST", IntegerType()),
        StructField("C_YEAR", IntegerType()),
        StructField("C_TERM", StringType()),
        StructField("C_EXTRACT", StringType()),
        StructField("C_CRS_SBJ", StringType()),
        StructField("C_CRS_NUM", StringType()),
        StructField("C_CRS_SEC", StringType()),
        StructField("C_MIN_CREDIT", FloatType()),
        StructField("C_MAX_CREDIT", FloatType()),
        StructField("C_CONTACT_HRS", FloatType()),
        StructField("C_LINE_ITEM", StringType()),
        StructField("C_SITE_TYPE", StringType()),
        StructField("C_BUDGET_CODE", StringType()),
        StructField("C_DELIVERY_METHOD", StringType()),
        StructField("C_PROGRAM_TYPE", StringType()),
        StructField("C_CREDIT_IND", StringType()),
        StructField("C_START_TIME", StringType()),
        StructField("C_STOP_TIME", StringType()),
        StructField("C_DAYS", StringType()),
        StructField("C_BLDG_SNAME", StringType()),
        StructField("C_BLDG_NUM", StringType()),
        StructField("C_ROOM_NUM", StringType()),
        StructField("C_ROOM_MAX", FloatType()),
        StructField("C_ROOM_TYPE", StringType()),
        StructField("C_START_TIME2", StringType()),
        StructField("C_STOP_TIME2", StringType()),
        StructField("C_DAYS2", StringType()),
        StructField("C_BLDG_SNAME2", StringType()),
        StructField("C_BLDG_NUM2", StringType()),
        StructField("C_ROOM_NUM2", StringType()),
        StructField("C_ROOM_MAX2", FloatType()),
        StructField("C_ROOM_TYPE2", StringType()),
        StructField("C_START_TIME3", StringType()),
        StructField("C_STOP_TIME3", StringType()),
        StructField("C_DAYS3", StringType()),
        StructField("C_BLDG_SNAME3", StringType()),
        StructField("C_BLDG_NUM3", StringType()),
        StructField("C_ROOM_NUM3", StringType()),
        StructField("C_ROOM_MAX3", FloatType()),
        StructField("C_ROOM_TYPE3", StringType()),
        StructField("C_START_DATE", DateType()),
        StructField("C_END_DATE", DateType()),
        StructField("C_TITLE", StringType()),
        StructField("C_INSTRUCT_ID", StringType()),
        StructField("C_INSTRUCT_NAME", StringType()),
        StructField("C_INSTRUCT_TYPE", StringType()),
        StructField("C_COLLEGE", StringType()),
        StructField("C_DEPT", StringType()),
        StructField("C_GEN_ED", StringType()),
        StructField("C_DEST_SITE", StringType()),
        StructField("C_CLASS_SIZE", IntegerType()),
        StructField("C_DELIVERY_MODEL", StringType()),
        StructField("C_LEVEL", StringType()),
        StructField("C_CRN", IntegerType()),
        StructField("C_SITE_TYPE2", StringType()),
        StructField("C_SITE_TYPE3", StringType()),
    ]
)


if __name__ == "__main__":
    # Output the current struct type as a json schema.
    # Use this to generate a new schema JSON file.
    course_schema_json = course_schema.json()
    with open("course_schema.json", "w") as f:
        f.write(course_schema_json)
