import pytest
from faker import Factory
from pyspark.sql.functions import lpad
from pyspark.sql.types import StringType


def make_valid_course_data():
    fake = Factory.create()
    return {
        "C_INST": fake.random_int(min=1000, max=9999),
        "C_YEAR": fake.random_int(min=2020, max=2025),
        "C_TERM": fake.random_int(min=1, max=2),
        "C_EXTRACT": fake.random_letter(),
        "C_CRS_SBJ": fake.random_letter(),
        "C_CRS_NUM": fake.random_int(min=100, max=999),
        "C_CRS_SEC": fake.random_int(min=100, max=999),
        "C_MIN_CREDIT": fake.random_int(min=1, max=4),
        "C_MAX_CREDIT": fake.random_int(min=1, max=4),
        "C_CONTACT_HRS": fake.random_int(min=1, max=4),
        "C_LINE_ITEM": fake.random_int(min=100, max=999),
        "C_SITE_TYPE": fake.random_letter(),
        "C_BUDGET_CODE": fake.random_letter(),
        "C_DELIVERY_METHOD": fake.random_letter(),
        "C_PROGRAM_TYPE": fake.random_letter(),
        "C_CREDIT_IND": fake.random_letter(),
        "C_START_TIME": fake.random_int(min=800, max=1800),
        "C_STOP_TIME": fake.random_int(min=800, max=1800),
        "C_DAYS": fake.random_letter(),
        "C_BLDG_SNAME": fake.random_letter(),
        "C_BLDG_NUM": fake.random_int(min=100, max=999),
        "C_ROOM_NUM": fake.random_int(min=100, max=999),
        "C_ROOM_MAX": fake.random_int(min=10, max=100),
        "C_ROOM_TYPE": fake.random_letter(),
        "C_START_TIME2": fake.random_int(min=800, max=1800),
        "C_STOP_TIME2": fake.random_int(min=800, max=1800),
        "C_DAYS2": fake.random_letter(),
        "C_BLDG_SNAME2": fake.random_letter(),
        "C_BLDG_NUM2": fake.random_int(min=100, max=999),
        "C_ROOM_NUM2": fake.random_int(min=100, max=999),
        "C_ROOM_MAX2": fake.random_int(min=10, max=100),
        "C_ROOM_TYPE2": fake.random_letter(),
        "C_START_TIME3": fake.random_int(min=800, max=1800),
        "C_STOP_TIME3": fake.random_int(min=800, max=1800),
        "C_DAYS3": fake.random_letter(),
        "C_BLDG_SNAME3": fake.random_letter(),
        "C_BLDG_NUM3": fake.random_int(min=100, max=999),
        "C_ROOM_NUM3": fake.random_int(min=100, max=999),
        "C_ROOM_MAX3": fake.random_int(min=10, max=100),
        "C_ROOM_TYPE3": fake.random_letter(),
        "C_START_DATE": fake.date_this_year(),
        "C_END_DATE": fake.date_this_year(),
        "C_TITLE": fake.catch_phrase(),
        "C_INSTRUCT_ID": fake.random_int(min=100, max=999),
        "C_INSTRUCT_NAME": fake.name(),
    }


@pytest.fixture()
def valid_course_data_batch_dataframe_with_whitespaces(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    df = spark_session.createDataFrame(valid_course_data_batch)
    for col in df.columns:
        # add whitespaces to string fields intentionally
        if df.schema[col].dataType == StringType():
            df = df.withColumn(col, lpad(col, 20, " "))

    return df
