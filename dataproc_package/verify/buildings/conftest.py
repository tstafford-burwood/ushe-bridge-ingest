import pytest
from faker import Factory
from pyspark.sql.functions import lit, lpad
from pyspark.sql.types import StringType


def make_valid_building_data():
    fake = Factory.create()
    return {
        "B_INST": fake.random_letter(),
        "B_LOCATION": fake.random_letter(),
        "B_OWNERSHIP": fake.random_element(elements=("L", "Test")),
        "B_YEAR": fake.random_element(elements=("2024", "2023")),
        "B_NAME": fake.random_letter(),
        "B_NUMBER": fake.random_letter(),
        "B_SNAME": fake.random_letter(),
        "B_YEAR_CONS": fake.random_letter(),
        "B_YEAR_REM": fake.random_letter(),
        "B_REPLACE_COST": fake.random_int(min=1, max=9999999999999),
        "B_CONDITION": fake.random_element(elements=("1", "2")),
        "B_GROSS": fake.random_int(min=1, max=99999999999),
        "B_COST_MYR": fake.random_int(min=1, max=9999999999),
        "B_RSKNBR": fake.random_letter(),
        "B_AUX": fake.random_letter(),
    }


def make_valid_room_data():
    fake = Factory.create()
    return {
        "R_INST": fake.random_element(elements=("1", "2")),
        "R_YEAR": "2024",
        "R_BUILD_NUMBER": fake.random_letter(),
        "R_NUMBER": fake.random_letter(),
        "R_SUFFIX": fake.random_letter(),
        "R_GROUP1": fake.random_letter(),
        "R_USE_CATEGORY": fake.random_letter(),
        "R_USE_CODE": fake.random_letter(),
        "R_NAME": fake.random_letter(),
        "R_STATIONS": fake.random_int(min=1, max=9999),
        "R_AREA": fake.random_int(min=1, max=9999),
        "R_DISAB_ACC": fake.random_letter(),
        "R_PRORATION": fake.random_letter(),
        "R_PRORATED_AREA": fake.random_int(min=1, max=9999),
        "R_UPDATE_DATE": "08291993",
    }


def make_invalid_b10a_building_data():
    fake = Factory.create()
    return {
        "B_INST": fake.random_letter(),
        "B_LOCATION": fake.random_element(elements=("MC", "SPC")),
        "B_OWNERSHIP": fake.random_element(elements=("O")),
        "B_YEAR": fake.random_element(elements=("2022", "2021", "2023")),
        "B_NAME": fake.random_element(elements=("Jon", "Jess", "Jim")),
        "B_NUMBER": fake.random_letter(),
        "B_SNAME": fake.random_element(elements=("Jon", "Jess", "Jim")),
        "B_YEAR_CONS": fake.random_letter(),
        "B_YEAR_REM": fake.random_letter(),
        "B_REPLACE_COST": fake.random_int(min=1, max=9999999999999),
        "B_CONDITION": fake.random_letter(),
        "B_GROSS": fake.random_int(min=1, max=99999999999),
        "B_COST_MYR": fake.random_int(min=1, max=9999999999),
        "B_RSKNBR": fake.random_letter(),
        "B_AUX": fake.random_element(elements=("N")),
    }


def make_valid_building_join_data():
    fake = Factory.create()
    return {
        "B_INST": fake.random_element(elements=("5220", "5220")),
        "B_LOCATION": fake.random_letter(),
        "B_OWNERSHIP": fake.random_element(elements=("O", "O")),
        "B_YEAR": fake.random_element(elements=("2024", "2024")),
        "B_NAME": fake.random_letter(),
        "B_NUMBER": fake.random_element(elements=("12", "12")),
        "B_SNAME": fake.random_letter(),
        "B_YEAR_CONS": fake.random_letter(),
        "B_YEAR_REM": fake.random_letter(),
        "B_REPLACE_COST": fake.random_int(min=1, max=9999999999999),
        "B_CONDITION": fake.random_element(elements=("1", "2")),
        "B_GROSS": fake.random_element(elements=("1", "0")),
        "B_COST_MYR": fake.random_int(min=1, max=9999999999),
        "B_RSKNBR": fake.random_letter(),
        "B_AUX": fake.random_letter(),
    }


@pytest.fixture()
def valid_room_data_batch_dataframe(spark_session):
    valid_room_data_batch = [make_valid_room_data() for _ in range(10)]
    return spark_session.createDataFrame(valid_room_data_batch)


@pytest.fixture()
def valid_building_data_batch_dataframe(spark_session):
    valid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    return spark_session.createDataFrame(valid_building_data_batch)


@pytest.fixture()
def valid_building_join_data_batch_dataframe(spark_session):
    valid_building_data_batch = [make_valid_building_join_data() for _ in range(10)]
    return spark_session.createDataFrame(valid_building_data_batch)


@pytest.fixture()
def valid_building_data_batch_dataframe_with_whitespaces(spark_session):
    valid_building_data_batch = [make_valid_building_data() for _ in range(10)]

    df = spark_session.createDataFrame(valid_building_data_batch)
    for col in df.columns:
        # add whitespaces to string fields intentionally
        if df.schema[col].dataType == StringType():
            df = df.withColumn(col, lpad(col, 20, " "))

    return df


@pytest.fixture()
def building_data_batch_with_duplicates_dataframe(spark_session):
    valid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    dupe = valid_building_data_batch[0]
    valid_building_data_batch.append(dupe)
    return spark_session.createDataFrame(valid_building_data_batch)


@pytest.fixture()
def building_data_batch_with_blanks_dataframe(spark_session):
    invalid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    building_data_row_with_blanks = make_valid_building_data()
    building_data_row_with_blanks["B_OWNERSHIP"] = ""
    building_data_row_with_blanks["B_YEAR"] = ""
    building_data_row_with_blanks["B_LOCATION"] = ""
    building_data_row_with_blanks["B_NAME"] = ""
    building_data_row_with_blanks["B_SNAME"] = ""
    building_data_row_with_blanks["B_NUMBER"] = ""
    building_data_row_with_blanks["B_AUX"] = ""
    invalid_building_data_batch.append(building_data_row_with_blanks)
    return spark_session.createDataFrame(invalid_building_data_batch)


@pytest.fixture()
def building_data_batch_with_replace_cost_dataframe(spark_session):
    invalid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    building_data_row_with_values = make_valid_building_data()
    building_data_row_with_values["B_OWNERSHIP"] = "O"
    building_data_row_with_values["B_AUX"] = "N"
    building_data_row_with_values["B_REPLACE_COST"] = "4500000"
    building_data_row_with_values["B_YEAR_CONS"] = None
    building_data_row_with_values["B_CONDITION"] = None
    building_data_row_with_values["B_GROSS"] = None
    invalid_building_data_batch.append(building_data_row_with_values)
    return spark_session.createDataFrame(invalid_building_data_batch)


@pytest.fixture()
def building_data_batch_with_replace_cost2_dataframe(spark_session):
    invalid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    building_data_row_with_values = make_valid_building_data()
    building_data_row_with_values["B_OWNERSHIP"] = "O"
    building_data_row_with_values["B_AUX"] = "N"
    building_data_row_with_values["B_REPLACE_COST"] = "2500000"
    building_data_row_with_values["B_YEAR_CONS"] = None
    building_data_row_with_values["B_RSKNBR"] = None
    invalid_building_data_batch.append(building_data_row_with_values)
    return spark_session.createDataFrame(invalid_building_data_batch)


@pytest.fixture()
def building_data_batch_with_replace_cost3_dataframe(spark_session):
    invalid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    building_data_row_with_values = make_valid_building_data()
    building_data_row_with_values["B_OWNERSHIP"] = "O"
    building_data_row_with_values["B_AUX"] = "N"
    building_data_row_with_values["B_REPLACE_COST"] = "2500000"
    building_data_row_with_values["B_YEAR_CONS"] = None
    building_data_row_with_values["B_CONDITION"] = None
    invalid_building_data_batch.append(building_data_row_with_values)
    return spark_session.createDataFrame(invalid_building_data_batch)


@pytest.fixture()
def building_data_batch_with_gross_dataframe(spark_session):
    invalid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    building_data_row_with_values = make_valid_building_data()
    building_data_row_with_values["B_OWNERSHIP"] = "O"
    building_data_row_with_values["B_AUX"] = "N"
    building_data_row_with_values["B_REPLACE_COST"] = "4500000"
    building_data_row_with_values["B_YEAR_CONS"] = None
    building_data_row_with_values["B_CONDITION"] = None
    building_data_row_with_values["B_GROSS"] = "0"
    building_data_row_with_values["B_RSKNBR"] = None
    invalid_building_data_batch.append(building_data_row_with_values)
    return spark_session.createDataFrame(invalid_building_data_batch)


@pytest.fixture()
def building_data_batch_b10a_dataframe(spark_session):
    invalid_b10a_building_data_batch = [
        make_invalid_b10a_building_data() for _ in range(10)
    ]
    row_with_none = make_invalid_b10a_building_data()
    row_with_none["B_REPLACE_COST"] = None
    invalid_b10a_building_data_batch.append(row_with_none)
    return spark_session.createDataFrame(invalid_b10a_building_data_batch)


@pytest.fixture()
def building_data_batch_with_aux_dataframe(spark_session):
    invalid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    building_data_row_with_blanks = make_valid_building_data()
    building_data_row_with_blanks["B_AUX"] = "test"
    invalid_building_data_batch.append(building_data_row_with_blanks)
    return spark_session.createDataFrame(invalid_building_data_batch)


@pytest.fixture()
def building_data_batch_with_auxa_dataframe(spark_session):
    invalid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    building_data_row_with_blanks = make_valid_building_data()
    building_data_row_with_blanks["B_AUX"] = "A"
    invalid_building_data_batch.append(building_data_row_with_blanks)
    return spark_session.createDataFrame(invalid_building_data_batch)


@pytest.fixture()
def building_data_batch_with_null_institution(spark_session):
    valid_building_data_batch = [make_valid_building_data() for _ in range(10)]

    valid_building_data_batch_df = spark_session.createDataFrame(
        valid_building_data_batch
    )

    # Set all B_INST to None
    valid_building_data_batch_df = valid_building_data_batch_df.withColumn(
        "B_INST", lit(None)
    )

    return valid_building_data_batch_df


@pytest.fixture()
def building_room_join_tuple_with_null_rooms(spark_session):
    building_data = [make_valid_building_data() for _ in range(10)]
    for row in building_data:
        row["B_INST"] = "INST1"
        row["B_YEAR"] = "2024"
        row["B_NUMBER"] = "1"

    building_data_df = spark_session.createDataFrame(building_data)

    room_data = [make_valid_room_data() for _ in range(10)]
    for row in room_data:
        row["R_INST"] = "INST1"
        row["R_YEAR"] = "2024"
        row["R_BUILD_NUMBER"] = "1"
        row["R_NUMBER"] = ""

    room_data_df = spark_session.createDataFrame(room_data)

    return (building_data_df, room_data_df)


@pytest.fixture()
def building_room_join_tuple_prorated_no_prorated(spark_session):
    building_data = [make_valid_building_data() for _ in range(10)]

    building_data[0]["B_INST"] = "INST1"
    building_data[0]["B_YEAR"] = "2024"
    building_data[0]["B_NUMBER"] = "1"
    building_data[0]["B_GROSS"] = 1
    building_data[0]["B_OWNERSHIP"] = "O"

    room_data_non_prorated = [make_valid_room_data() for _ in range(10)]
    for row in room_data_non_prorated:
        row["R_INST"] = "INST1"
        row["R_YEAR"] = "2024"
        row["R_BUILD_NUMBER"] = "1"
        row["R_NUMBER"] = "1"
        row["R_PRORATION"] = "N"
        row["R_AREA"] = 2

    room_data_prorated = [make_valid_room_data() for _ in range(10)]
    for row in room_data_prorated:
        row["R_INST"] = "INST1"
        row["R_YEAR"] = "2024"
        row["R_BUILD_NUMBER"] = "1"
        row["R_NUMBER"] = "1"
        row["R_PRORATION"] = "Y"
        row["R_PRORATED_AREA"] = 1

    room_data = room_data_non_prorated + room_data_prorated

    return (
        spark_session.createDataFrame(building_data),
        spark_session.createDataFrame(room_data),
    )


@pytest.fixture()
def building_data_batch_with_null_location(spark_session):
    valid_building_data_batch = [make_valid_building_data() for _ in range(10)]

    valid_building_data_batch_df = spark_session.createDataFrame(
        valid_building_data_batch
    )

    # Set all B_INST to None
    valid_building_data_batch_df = valid_building_data_batch_df.withColumn(
        "B_LOCATION", lit(None)
    )

    return valid_building_data_batch_df


@pytest.fixture()
def building_data_batch_with_condition_dataframe(spark_session):
    invalid_building_data_batch = [make_valid_building_data() for _ in range(10)]
    building_data_row_with_blanks = make_valid_building_data()
    building_data_row_with_blanks["B_CONDITION"] = "test"
    invalid_building_data_batch.append(building_data_row_with_blanks)
    return spark_session.createDataFrame(invalid_building_data_batch)


@pytest.fixture()
def building_data_batch_with_null_ownership(spark_session):
    valid_building_data_batch = [make_valid_building_data() for _ in range(10)]

    valid_building_data_batch_df = spark_session.createDataFrame(
        valid_building_data_batch
    )

    # Set all B_INST to None
    valid_building_data_batch_df = valid_building_data_batch_df.withColumn(
        "B_OWNERSHIP", lit("test")
    )

    return valid_building_data_batch_df
