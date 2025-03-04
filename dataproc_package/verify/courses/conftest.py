import pytest
from faker import Factory
from pyspark.sql.functions import lit


def make_valid_course_data():
    fake = Factory.create()
    return {
        "C_INST": fake.random_int(min=1, max=9999),
        "C_YEAR": fake.random_int(min=2020, max=2025),
        "C_TERM": fake.random_letter(),
        "C_EXTRACT": fake.random_letter(),
        "C_CRS_SBJ": fake.random_letter(),
        "C_CRS_NUM": fake.random_letter(),
        "C_CRS_SEC": fake.random_letter(),
        "C_MIN_CREDIT": fake.random_int(min=1, max=100),
        "C_MAX_CREDIT": fake.random_int(min=1, max=100),
        "C_CONTACT_HRS": fake.random_int(min=1, max=100),
        "C_LINE_ITEM": fake.random_letter(),
        "C_SITE_TYPE": fake.random_letter(),
        "C_BUDGET_CODE": fake.random_letter(),
        "C_DELIVERY_METHOD": fake.random_letter(),
        "C_PROGRAM_TYPE": fake.random_letter(),
        "C_CREDIT_IND": fake.random_letter(),
        "C_START_TIME": fake.random_letter(),
        "C_STOP_TIME": fake.random_letter(),
        "C_DAYS": fake.random_letter(),
        "C_BLDG_SNAME": fake.random_letter(),
        "C_BLDG_NUM": fake.random_letter(),
        "C_ROOM_NUM": fake.random_letter(),
        "C_ROOM_MAX": fake.random_int(min=1, max=9999),
        "C_ROOM_TYPE": fake.random_letter(),
        "C_START_TIME2": fake.random_letter(),
        "C_STOP_TIME2": fake.random_letter(),
        "C_DAYS2": fake.random_letter(),
        "C_BLDG_SNAME2": fake.random_letter(),
        "C_BLDG_NUM2": fake.random_letter(),
        "C_ROOM_NUM2": fake.random_letter(),
        "C_ROOM_MAX2": fake.random_int(min=1, max=9999),
        "C_ROOM_TYPE2": fake.random_letter(),
        "C_START_TIME3": fake.random_letter(),
        "C_STOP_TIME3": fake.random_letter(),
        "C_DAYS3": fake.random_letter(),
        "C_BLDG_SNAME3": fake.random_letter(),
        "C_BLDG_NUM3": fake.random_letter(),
        "C_ROOM_NUM3": fake.random_letter(),
        "C_ROOM_MAX3": fake.random_int(min=1, max=9999),
        "C_ROOM_TYPE3": fake.random_letter(),
        "C_START_DATE": fake.date_this_year(),
        "C_END_DATE": fake.date_this_year(),
        "C_TITLE": fake.catch_phrase(),
        "C_INSTRUCT_ID": fake.random_letter(),
        "C_INSTRUCT_NAME": fake.name(),
        "C_INSTRUCT_TYPE": fake.random_letter(),
        "C_COLLEGE": fake.random_letter(),
        "C_DEPT": fake.random_letter(),
        "C_GEN_ED": fake.random_letter(),
        "C_DEST_SITE": fake.random_letter(),
        "C_CLASS_SIZE": fake.random_int(min=1, max=9999),
        "C_DELIVERY_MODEL": fake.random_letter(),
        "C_LEVEL": fake.random_letter(),
        "C_CRN": fake.random_int(min=1, max=9999),
        "C_SITE_TYPE2": fake.random_letter(),
        "C_SITE_TYPE3": fake.random_letter(),
        "c_instance": fake.random_letter(),
        "c_key": fake.random_letter(),
        "c_crs_unique": fake.random_letter(),
    }


def make_valid_student_data():
    fake = Factory.create()
    return {
        "S_INST": fake.random_int(min=1, max=9999),
        "S_YEAR": fake.random_element(elements=("2024", "2024")),
        "S_TERM": fake.random_int(min=1, max=2),
        "S_EXTRACT": fake.random_letter(),
        "S_SSN": fake.random_letter(),
        "S_RPT_RES": fake.random_letter(),
        "S_ID": fake.random_element(elements=("12345678", "12345678")),
        "S_ID_FLAG": fake.random_element(elements=("S")),
        "S_PREVIOUS_ID": fake.random_element(elements=("0", "00", "000")),
        "S_LAST": fake.random_letter(),
        "S_FIRST": fake.random_letter(),
        "S_MIDDLE": fake.random_letter(),
        "S_SUFFIX": fake.random_letter(),
        "S_PREVIOUS_LAST": fake.random_letter(),
        "S_PREVIOUS_FIRST": fake.random_letter(),
        "S_PREVIOUS_MIDDLE": fake.random_letter(),
        "S_PREVIOUS_SUFFIX": fake.random_letter(),
        "S_CURR_ZIP": fake.random_element(elements=("92103", "92104")),
        "S_CITZ_CODE": fake.random_element(elements=("1")),
        "S_COUNTY_ORIGIN": fake.random_letter(),
        "S_STATE_ORIGIN": fake.random_element(elements=("UT")),
        "S_BIRTH_DT": fake.random_element(elements=("20201101", "20200808")),
        "S_GENDER": fake.random_element(elements=("m", "n", "f")),
        "S_ETHNIC_H": fake.random_letter(),
        "S_ETHNIC_A": fake.random_letter(),
        "S_ETHNIC_B": fake.random_letter(),
        "S_ETHNIC_I": fake.random_letter(),
        "S_ETHNIC_P": fake.random_letter(),
        "S_ETHNIC_W": fake.random_letter(),
        "S_ETHNIC_N": fake.random_letter(),
        "S_ETHNIC_U": fake.random_element(elements=("U", "U")),
        "S_ETHNIC_IPEDS": fake.random_letter(),
        "S_REGENT_RES": fake.random_letter(),
        "S_CURR_CIP": fake.random_letter(),
        "S_REG_STATUS": fake.random_element(elements=("CS", "HS", "FF", "FH", "TU")),
        "S_LEVEL": fake.random_element(elements=("GN", "GG")),
        "S_DEG_INTENT": fake.random_letter(),
        "S_CUM_HRS_UGRAD": fake.random_int(min=1, max=9999),
        "S_CUM_GPA_UGRAD": fake.random_int(min=1, max=9999),
        "S_CUM_HRS_GRAD": fake.random_int(min=1, max=9999),
        "S_CUM_GPA_GRAD": fake.random_int(min=1, max=9999),
        "S_TRANS_TOTAL": fake.random_int(min=1, max=9999),
        "S_PT_FT": fake.random_letter(),
        "S_AGE": fake.random_int(min=10, max=100),
        "S_COUNTRY_ORIGIN": fake.random_letter(),
        "S_HIGH_SCHOOL": fake.random_letter(),
        "S_HB75_WAIVER": fake.random_int(min=1, max=9999),
        "S_CURR_CIP2": fake.random_letter(),
        "S_CUM_MEMBERSHIP": fake.random_int(min=1, max=9999),
        "S_TOTAL_CLEP": fake.random_int(min=1, max=9999),
        "S_TOTAL_AP": fake.random_int(min=1, max=9999),
        "S_SSID": fake.random_letter(),
        "S_BANNER_ID": fake.random_element(elements=("test", "test2")),
        "S_ACT": fake.random_int(min=1, max=9999),
        "S_INTENT_CIP": fake.random_letter(),
        "S_ACT_ENG": fake.random_int(min=1, max=9999),
        "S_ACT_MATH": fake.random_int(min=1, max=9999),
        "S_ACT_READ": fake.random_int(min=1, max=9999),
        "S_ACT_SCI": fake.random_int(min=1, max=9999),
        "S_HS_GRAD_DATE": fake.random_letter(),
        "S_TERM_GPA": fake.random_int(min=1, max=9999),
        "S_PELL": fake.random_letter(),
        "S_BIA": fake.random_letter(),
        "S_COLLEGE": fake.random_letter(),
        "S_MAJOR": fake.random_letter(),
        "S_COLLEGE2": fake.random_letter(),
        "S_MAJOR2": fake.random_letter(),
        "S_DEG_INTENT2": fake.random_letter(),
    }


def make_valid_studentcourses_data():
    fake = Factory.create()
    return {
        "SC_INST": fake.random_int(min=1000, max=9999),
        "SC_YEAR": fake.random_int(min=2020, max=2025),
        "SC_TERM": fake.random_int(min=1, max=2),
        "SC_EXTRACT": fake.random_letter(),
        "SC_ID": fake.random_letter(),
        "SC_CRS_SBJ": fake.random_letter(),
        "SC_CRS_NUM": fake.random_letter(),
        "SC_CRS_SEC": fake.random_letter(),
        "SC_ATT_CR": fake.random_int(min=1, max=9999),
        "SC_EARNED_CR": fake.random_int(min=1, max=9999),
        "SC_CONTACT_HRS": fake.random_int(min=1, max=9999),
        "SC_GRADE": fake.random_letter(),
        "SC_MEMBERSHIP_HRS": fake.random_int(min=1, max=9999),
        "SC_STUDENT_TYPE": fake.random_letter(),
        "SC_BANNER_ID": fake.random_letter(),
        "SC_CRN": fake.random_int(min=1000, max=9999),
        "SC_CR_TYPE": fake.random_letter(),
    }


def make_valid_building_prod_data():
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


def make_valid_room_prod_data():
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


def make_valid_perkins2015_prod_data():
    fake = Factory.create()
    return {
        "p_crs_sbj": fake.random_letter(),
        "p_crs_num": fake.random_letter(),
        "p_inst": fake.random_letter(),
        "p_year": fake.random_letter(),
        "p_term": fake.random_letter(),
        "Inactive": fake.random_letter(),
        "Modify_Date": fake.random_letter(),
    }


def make_valid_logan_perkins_prod_data():
    fake = Factory.create()
    return {
        "p_crs_sbj": fake.random_letter(),
        "p_crs_num": fake.random_letter(),
        "p_inst": fake.random_letter(),
        "p_year": fake.random_letter(),
        "p_term": fake.random_letter(),
        "Inactive": fake.random_letter(),
        "Modify_Date": fake.random_letter(),
    }


def make_valid_cc_master_list_prod_data():
    fake = Factory.create()
    return {
        "sbj": fake.random_letter(),
        "num": fake.random_letter(),
        "inst": fake.random_letter(),
        "year": fake.random_letter(),
    }


@pytest.fixture()
def valid_cc_master_list_prod_data_batch_dataframe(spark_session):
    valid_cc_master_list_prod_data_batch = [
        make_valid_cc_master_list_prod_data() for _ in range(10)
    ]
    return spark_session.createDataFrame(valid_cc_master_list_prod_data_batch)


@pytest.fixture()
def valid_room_prod_data_batch_dataframe(spark_session):
    valid_room_prod_data_batch = [make_valid_room_prod_data() for _ in range(10)]
    return spark_session.createDataFrame(valid_room_prod_data_batch)


@pytest.fixture()
def valid_perkins2015_prod_data_batch_dataframe(spark_session):
    valid_perkins2015_prod_data_batch = [
        make_valid_perkins2015_prod_data() for _ in range(10)
    ]
    return spark_session.createDataFrame(valid_perkins2015_prod_data_batch)


@pytest.fixture()
def valid_logan_perkins_prod_data_batch_dataframe(spark_session):
    valid_logan_perkins_prod_data_batch = [
        make_valid_logan_perkins_prod_data() for _ in range(10)
    ]
    return spark_session.createDataFrame(valid_logan_perkins_prod_data_batch)


@pytest.fixture()
def valid_building_prod_data_batch_dataframe(spark_session):
    valid_building_prod_data_batch = [
        make_valid_building_prod_data() for _ in range(10)
    ]
    return spark_session.createDataFrame(valid_building_prod_data_batch)


@pytest.fixture()
def valid_studentcourses_data_batch_dataframe(spark_session):
    valid_studentcourses_data_batch = [
        make_valid_studentcourses_data() for _ in range(10)
    ]
    return spark_session.createDataFrame(valid_studentcourses_data_batch)


@pytest.fixture()
def valid_student_data_batch_dataframe(spark_session):
    valid_student_data_batch = [make_valid_student_data() for _ in range(10)]
    return spark_session.createDataFrame(valid_student_data_batch)


@pytest.fixture()
def valid_course_data_batch_dataframe(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    return spark_session.createDataFrame(valid_course_data_batch)


@pytest.fixture()
def valid_course_loads_data_batch_dataframe(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    return spark_session.createDataFrame(valid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_duplicates_dataframe(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    dupe = valid_course_data_batch[0]
    valid_course_data_batch.append(dupe)
    return spark_session.createDataFrame(valid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_null_institution(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    # Set all C_INST to None
    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INST", lit(None)
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_blanks_dataframe(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_blanks = make_valid_course_data()
    course_data_with_blanks["C_INST"] = None
    course_data_with_blanks["C_YEAR"] = None
    course_data_with_blanks["C_EXTRACT"] = None
    invalid_course_data_batch.append(course_data_with_blanks)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_invalid_course_number(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_CRS_NUM", lit("19")
    )

    return valid_course_data_batch_df

    # course_data_with_blanks = make_valid_course_data()
    # course_data_with_blanks["C_CRS_NUM"] = "1"
    # invalid_course_data_batch.append(course_data_with_blanks)
    # return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_invalid_course_number_grad(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INST", lit("5220")
    ).withColumn("C_CRS_NUM", lit("71234"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_course_section(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INST", lit("5220")
    ).withColumn("C_CRS_SEC", lit("71"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_min_credits(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_MAX_CREDITS", lit("71")
    ).withColumn("C_MIN_CREDITS", lit("71"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_min_max_credits(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_MAX_CREDITS", lit(None)
    ).withColumn("C_MIN_CREDITS", lit("72"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_contact_hrs(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_CONTACT_HRS", lit("5000")
    ).withColumn("C_MIN_CREDITS", lit("72"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_line_item(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_LINE_ITEM", lit("z")
    ).withColumn("C_MIN_CREDITS", lit("72"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_line_item_for_inst(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_LINE_ITEM", lit("z")
    ).withColumn("C_INST", lit("3675"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_site_type(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_SITE_TYPE", lit(None)
    ).withColumn("C_EXTRACT", lit("3"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_site_type10a(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_SITE_TYPE", lit("test"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_DELIVERY_METHOD", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_BUDGET_CODE", lit(""))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_budget_code(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_SITE_TYPE", lit("test"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_DELIVERY_METHOD", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_BUDGET_CODE", lit("test"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_delivery_method(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_DELIVERY_METHOD", lit("test")
    ).withColumn("C_EXTRACT", lit("3"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_delivery_method_12a(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_DELIVERY_METHOD", lit("test")
    ).withColumn("C_EXTRACT", lit("E"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_program_type(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_PROGRAM_TYPE", lit(None)
    ).withColumn("C_EXTRACT", lit("E"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_program_typec13a(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("5220"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_PROGRAM_TYPE", lit("V"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("456"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def valid_perkins2015_prod_data_batch_dataframe_c13a(spark_session):
    valid_perkins2015_prod_data_batch = [
        make_valid_perkins2015_prod_data() for _ in range(10)
    ]
    valid_perkins2015_prefix_prod_data_batch_dataframe = spark_session.createDataFrame(
        valid_perkins2015_prod_data_batch
    )
    valid_perkins2015_prefix_prod_data_batch_dataframe = (
        valid_perkins2015_prefix_prod_data_batch_dataframe.withColumn(
            "p_crs_sbj", lit("123")
        )
        .withColumn("p_crs_num", lit("457"))
        .withColumn("Inactive", lit("N"))
        .withColumn("p_inst", lit("5220"))
        .withColumn("p_term", lit("1"))
        .withColumn("p_year", lit("2024"))
    )
    return valid_perkins2015_prefix_prod_data_batch_dataframe


@pytest.fixture()
def course_data_batch_with_program_typec13b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("5220"))
        .withColumn("C_EXTRACT", lit("e"))
        .withColumn("C_PROGRAM_TYPE", lit("V"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("456"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def valid_perkins2015_prod_data_batch_dataframe_c13c(spark_session):
    valid_perkins2015_prod_data_batch = [
        make_valid_perkins2015_prod_data() for _ in range(10)
    ]
    valid_perkins2015_prefix_prod_data_batch_dataframe = spark_session.createDataFrame(
        valid_perkins2015_prod_data_batch
    )
    valid_perkins2015_prefix_prod_data_batch_dataframe = (
        valid_perkins2015_prefix_prod_data_batch_dataframe.withColumn(
            "p_crs_sbj", lit("123")
        )
        .withColumn("p_crs_num", lit("456"))
        .withColumn("Inactive", lit("N"))
        .withColumn("p_inst", lit("5220"))
        .withColumn("p_term", lit("1"))
        .withColumn("p_year", lit("2024"))
    )
    return valid_perkins2015_prefix_prod_data_batch_dataframe


@pytest.fixture()
def course_data_batch_with_program_typec13c(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("5220"))
        .withColumn("C_EXTRACT", lit("e"))
        .withColumn("C_PROGRAM_TYPE", lit("S"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("456"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_program_typec13e(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("3676"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_PROGRAM_TYPE", lit("P"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("457"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
        .withColumn("C_LINE_ITEM", lit("S"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def valid_perkinslogan_prod_data_batch_dataframe_c13e(spark_session):
    valid_perkinslogan_prod_data_batch = [
        make_valid_logan_perkins_prod_data() for _ in range(10)
    ]
    valid_perkinslogan_prod_data_batch_dataframe = spark_session.createDataFrame(
        valid_perkinslogan_prod_data_batch
    )
    valid_perkinslogan_prod_data_batch_dataframe = (
        valid_perkinslogan_prod_data_batch_dataframe.withColumn("p_crs_sbj", lit("123"))
        .withColumn("p_crs_num", lit("456"))
        .withColumn("Inactive", lit("N"))
        .withColumn("p_inst", lit("3676"))
        .withColumn("p_term", lit("1"))
        .withColumn("p_year", lit("2024"))
    )
    return valid_perkinslogan_prod_data_batch_dataframe


@pytest.fixture()
def course_data_batch_with_invalid_vocational(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("3676"))
        .withColumn("C_EXTRACT", lit("e"))
        .withColumn("C_PROGRAM_TYPE", lit("P"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("457"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
        .withColumn("C_LINE_ITEM", lit("S"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_vocationalc13g(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("3676"))
        .withColumn("C_EXTRACT", lit("e"))
        .withColumn("C_PROGRAM_TYPE", lit("A"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("456"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
        .withColumn("C_LINE_ITEM", lit("S"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_vocationalc13i(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("3676"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_PROGRAM_TYPE", lit("V"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("457"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
        .withColumn("C_LINE_ITEM", lit("T"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_vocationalc13j(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("3676"))
        .withColumn("C_EXTRACT", lit("e"))
        .withColumn("C_PROGRAM_TYPE", lit("V"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("457"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
        .withColumn("C_LINE_ITEM", lit("T"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_vocationalc13k(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("3676"))
        .withColumn("C_EXTRACT", lit("e"))
        .withColumn("C_PROGRAM_TYPE", lit("E"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("456"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
        .withColumn("C_LINE_ITEM", lit("T"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def valid_perkins2015_prod_data_batch_dataframe_c13k(spark_session):
    valid_perkins2015_prod_data_batch = [
        make_valid_perkins2015_prod_data() for _ in range(10)
    ]
    valid_perkins2015_prefix_prod_data_batch_dataframe = spark_session.createDataFrame(
        valid_perkins2015_prod_data_batch
    )
    valid_perkins2015_prefix_prod_data_batch_dataframe = (
        valid_perkins2015_prefix_prod_data_batch_dataframe.withColumn(
            "p_crs_sbj", lit("123")
        )
        .withColumn("p_crs_num", lit("456"))
        .withColumn("Inactive", lit("N"))
        .withColumn("p_inst", lit("3676"))
        .withColumn("p_term", lit("1"))
        .withColumn("p_year", lit("2024"))
    )
    return valid_perkins2015_prefix_prod_data_batch_dataframe


@pytest.fixture()
def course_data_batch_with_invalid_credit_ind(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_CREDIT_IND", lit("test")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_credit_ind14b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_CREDIT_IND", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("notLAB"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_vocational14c(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_CREDIT_IND", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("notLAB"))
        .withColumn("C_PROGRAM_TYPE", lit("S"))
        .withColumn("C_BUDGET_CODE", lit("SF"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_CRS_SBJ", lit("NURP"))
        .withColumn("C_CRS_NUM", lit("test"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_start_time(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_START_TIME", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_start_timec15b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_START_TIME", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_stop_time(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_STOP_TIME", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_stop_timec16b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_STOP_TIME", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_days(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_DAYS", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_daysc17b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_DAYS", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_bldg_name_num(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_BLDG_SNAME", lit("N")
    ).withColumn("C_BLDG_NUM", lit("N"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_sname(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_BLDG_SNAME", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_snamec18b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_BLDG_SNAME", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_num(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_BLDG_NUM", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_numc19b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_BLDG_NUM", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_room_num(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_NUM", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_room_numc20b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_NUM", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_room_type(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_room_typec22c(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_start_time2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE2", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
        .withColumn("C_START_TIME2", lit(None))
        .withColumn("C_DAYS2", lit("3"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_start_time2c23c(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE2", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
        .withColumn("C_START_TIME2", lit(None))
        .withColumn("C_DAYS2", lit("3"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_stop_time2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE2", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
        .withColumn("C_STOP_TIME2", lit(None))
        .withColumn("C_DAYS2", lit("4"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_stop_time2c24c(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE2", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
        .withColumn("C_STOP_TIME2", lit(None))
        .withColumn("C_DAYS2", lit("4"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_days2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE2", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
        .withColumn("C_DAYS2", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_days2c25c(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE2", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
        .withColumn("C_DAYS2", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_sname2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE2", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
        .withColumn("C_DAYS2", lit("3"))
        .withColumn("C_BLDG_SNAME2", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_sname2c26b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE2", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
        .withColumn("C_DAYS2", lit("3"))
        .withColumn("C_BLDG_SNAME2", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_ceml(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("5220"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_BUDGET_CODE", lit("SF"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("456"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def valid_ceml_prod_data_batch_dataframe(spark_session):
    valid_ceml_prod_data_batch = [
        make_valid_cc_master_list_prod_data() for _ in range(10)
    ]
    valid_ceml_prod_data_batch_dataframe = spark_session.createDataFrame(
        valid_ceml_prod_data_batch
    )
    valid_ceml_prod_data_batch_dataframe = (
        valid_ceml_prod_data_batch_dataframe.withColumn("sbj", lit("123"))
        .withColumn("num", lit("457"))
        .withColumn("inst", lit("5220"))
        .withColumn("year", lit("2024"))
    )
    return valid_ceml_prod_data_batch_dataframe


@pytest.fixture()
def course_data_batch_with_cemlc11c(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_INST", lit("5220"))
        .withColumn("C_EXTRACT", lit("e"))
        .withColumn("C_BUDGET_CODE", lit("SF"))
        .withColumn("C_CRS_SBJ", lit("123"))
        .withColumn("C_CRS_NUM", lit("456"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_YEAR", lit("2024"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_bldg_and_b_num(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INST", lit("5220")
    ).withColumn("C_BLDG_NUM", lit("18"))

    return valid_course_data_batch_df


@pytest.fixture()
def building_prod_data_batch_dataframe_bnumber_null(spark_session):
    valid_building_prod_data_batch = [
        make_valid_building_prod_data() for _ in range(10)
    ]
    valid_building_prod_data_batch_df = spark_session.createDataFrame(
        valid_building_prod_data_batch
    )
    valid_building_prod_data_batch_df = valid_building_prod_data_batch_df.withColumn(
        "B_INST", lit("5220")
    ).withColumn("B_NUMBER", lit(None))
    return valid_building_prod_data_batch_df


@pytest.fixture()
def rooms_prod_data_batch_dataframe_bnumber_null(spark_session):
    valid_room_prod_data_batch = [make_valid_room_prod_data() for _ in range(10)]
    valid_room_prod_data_batch_df = spark_session.createDataFrame(
        valid_room_prod_data_batch
    )
    valid_room_prod_data_batch_df = valid_room_prod_data_batch_df.withColumn(
        "R_INST", lit("5220")
    ).withColumn("R_BUILD_NUMBER", lit(None))
    return valid_room_prod_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_room_max(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_ROOM_MAX", lit("10000")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_use_code(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_ROOM_TYPE", lit("test")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_room_typec22d(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("N"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_ROOM_TYPE", lit("210"))
        .withColumn("C_BUDGET_CODE", lit("SJ"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit("test"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_typec22e(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("B"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_DAYS", lit("18"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(None))
        .withColumn("C_ROOM_TYPE2", lit(None))
        .withColumn("C_ROOM_TYPE3", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_room_type_missing_delivery_method_b(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("B"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_DAYS3", lit("18"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(""))
        .withColumn("C_ROOM_TYPE2", lit(""))
        .withColumn("C_ROOM_TYPE3", lit(""))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_room_type_missing_delivery_method_p(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_DAYS3", lit("18"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(""))
        .withColumn("C_ROOM_TYPE2", lit(""))
        .withColumn("C_ROOM_TYPE3", lit(""))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_room_type_missing_delivery_method_p_1(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_DAYS3", lit("18"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(""))
        .withColumn("C_ROOM_TYPE2", lit(""))
        .withColumn("C_ROOM_TYPE3", lit(""))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_start_date_summer(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_START_DATE", lit("20230916"))
        .withColumn("c_instance", lit("2024hello"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_start_date_fall(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_TERM", lit("2"))
        .withColumn("C_START_DATE", lit("20230516"))
        .withColumn("c_instance", lit("2024hello"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_start_date_spring(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_TERM", lit("3"))
        .withColumn("C_START_DATE", lit("20230616"))
        .withColumn("c_instance", lit("2024hello"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_end_date_summer(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_TERM", lit("1"))
        .withColumn("C_START_DATE", lit("20230916"))
        .withColumn("c_instance", lit("2024hello"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_end_date_fall(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_TERM", lit("2"))
        .withColumn("C_START_DATE", lit("20230516"))
        .withColumn("c_instance", lit("2024hello"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_end_date_spring(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_TERM", lit("3"))
        .withColumn("C_START_DATE", lit("20230616"))
        .withColumn("c_instance", lit("2024hello"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_title_missing(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_TITLE", lit("")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_crn_missing(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn("C_CRN", lit(""))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_crn_invalid(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_CRN", lit("0")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_site_type2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_EXTRACT", lit("3")
    ).withColumn("C_SITE_TYPE2", lit(""))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_values_site_type2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_SITE_TYPE2", lit("A999"))
        .withColumn("C_DELIVERY_METHOD", lit("Z"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEC"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_site_type3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_EXTRACT", lit("3")
    ).withColumn("C_SITE_TYPE3", lit(""))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_values_site_type3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_SITE_TYPE3", lit("A999"))
        .withColumn("C_DELIVERY_METHOD", lit("Z"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEC"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_typec22f(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("B"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_DAYS", lit("18"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(None))
        .withColumn("C_ROOM_TYPE2", lit(None))
        .withColumn("C_ROOM_TYPE3", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_typec22g(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_DAYS", lit("18"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(None))
        .withColumn("C_ROOM_TYPE2", lit(None))
        .withColumn("C_ROOM_TYPE3", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_typec22h(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DELIVERY_METHOD", lit("P"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_INSTRUCT_TYPE", lit("LAB"))
        .withColumn("C_DAYS", lit("18"))
        .withColumn("C_INST", lit("3679"))
        .withColumn("C_SITE_TYPE", lit("A01"))
        .withColumn("C_ROOM_TYPE", lit(None))
        .withColumn("C_ROOM_TYPE2", lit(None))
        .withColumn("C_ROOM_TYPE3", lit(None))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_course_start_time(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_START_TIME2", lit("ABCD"))
        .withColumn("C_START_TIME", lit(None))
        .withColumn("C_SITE_TYPE", lit("A"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_course_stop_time(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_STOP_TIME2", lit("ABCD"))
        .withColumn("C_STOP_TIME", lit(None))
        .withColumn("C_SITE_TYPE", lit("A"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_course_days2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DAYS2", lit("ABCD"))
        .withColumn("C_DAYS", lit(None))
        .withColumn("C_SITE_TYPE", lit("A"))
    )

    return valid_course_data_batch_df


# @pytest.fixture()
# def course_data_batch_with_invalid_building_name(spark_session):
#     valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

#     valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

#     valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
#         "C_DAYS2", lit("ABCD")
#     ).withColumn("C_BLDG_NUM2", lit("ABCD"))


#     return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_buildingnum2_cond_required(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_BLDG_NUM2", lit(None)
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_buildingnum2_cond_required_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_BLDG_NUM2", lit(None))
        .withColumn("C_BUDGET_CODE", lit("AB"))
        .withColumn("C_ROOM_TYPE2", lit("110"))
        .withColumn("C_DAYS2", lit("12"))
        .withColumn("C_DELIVERY_METHOD", lit("A"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEC"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_buildingnum2_cond_required_3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_BLDG_NUM2", lit(None))
        .withColumn("C_BUDGET_CODE", lit("AB"))
        .withColumn("C_DAYS2", lit("12"))
        .withColumn("C_DELIVERY_METHOD", lit("A"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEC"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_roomnum2_cond_required(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_NUM2", lit(None))
        .withColumn("C_BUDGET_CODE", lit("AB"))
        .withColumn("C_ROOM_TYPE2", lit("110"))
        .withColumn("C_DAYS2", lit("12"))
        .withColumn("C_DELIVERY_METHOD", lit("A"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEC"))
        .withColumn("C_EXTRACT", lit("3"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_roomnum2_cond_required_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_NUM2", lit(None))
        .withColumn("C_BUDGET_CODE", lit("AB"))
        .withColumn("C_ROOM_TYPE2", lit("110"))
        .withColumn("C_DAYS2", lit("12"))
        .withColumn("C_DELIVERY_METHOD", lit("A"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEC"))
        .withColumn("C_EXTRACT", lit("E"))
        .withColumn("C_SITE_TYPE2", lit("A01"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_bldg_and_b_inv(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INST", lit("5220")
    ).withColumn("C_BLDG_NUM2", lit("18"))

    return valid_course_data_batch_df


@pytest.fixture()
def building_prod_data_batch_dataframe_bnumber_null_2(spark_session):
    valid_building_prod_data_batch = [
        make_valid_building_prod_data() for _ in range(10)
    ]
    valid_building_prod_data_batch_df = spark_session.createDataFrame(
        valid_building_prod_data_batch
    )
    valid_building_prod_data_batch_df = valid_building_prod_data_batch_df.withColumn(
        "B_INST", lit("5220")
    ).withColumn("B_NUMBER", lit(None))
    return valid_building_prod_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_building_num2_not_in_room_inventory(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INST", lit("5220")
    ).withColumn("C_BLDG_NUM2", lit("18"))

    return valid_course_data_batch_df


@pytest.fixture()
def rooms_prod_data_batch_dataframe_bnumber_null_2(spark_session):
    valid_room_prod_data_batch = [make_valid_room_prod_data() for _ in range(10)]
    valid_room_prod_data_batch_df = spark_session.createDataFrame(
        valid_room_prod_data_batch
    )
    valid_room_prod_data_batch_df = valid_room_prod_data_batch_df.withColumn(
        "R_INST", lit("5220")
    ).withColumn("R_BUILD_NUMBER", lit(None))
    return valid_room_prod_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_course_title(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_TITLE", lit("CA123")
    ).withColumn("C_EXTRACT", lit("3"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_course_title_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_TITLE", lit("CA123")
    ).withColumn("C_EXTRACT", lit("e"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_course_title_3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_TITLE", lit("Programming101")
    )
    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_instructor_id(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INSTRUCT_ID", lit("ABCD")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_instructor_id_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INSTRUCT_ID", lit("ABCDEFGHI")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_instructor_name(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INSTRUCT_NAME", lit(None)
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_instructor_name_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INSTRUCT_NAME", lit("AB'CDEFGHI")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_instruction_type_null(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INSTRUCT_TYPE", lit(None)
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_college_exists(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_COLLEGE", lit(None)
    ).withColumn("C_INST", lit("64"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_instruct_id(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INSTRUCT_ID", lit("test")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_instruct_type(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INSTRUCT_ID", lit("SUPC")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_college_name(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_COLLEGE", lit("UNIVERSITY OF UTAH")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_department(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_DEPT", lit(None)
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_dest_site(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_DEST_SITE", lit("999154")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_students_enrolled(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_CLASS_SIZE", lit("0")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_class_size(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_CLASS_SIZE", lit("10000")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_c_level(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_LEVEL", lit("A")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_invalid_c_ge_ed(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_GEN_ED", lit("AB")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_compare_c_gen_ed(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_GEN_ED", lit("AB")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_compare_c_level_remedial(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_LEVEL", lit("R"))
        .withColumn("C_CRS_SBJ", lit("wABC"))
        .withColumn("C_CRS_NUM", lit("0123"))
        .withColumn("C_INST", lit("4028"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def valid_studentcourses_data_batch_dataframe_delivery_model(spark_session):
    valid_studentcourses_data_batch = [
        make_valid_studentcourses_data() for _ in range(10)
    ]

    valid_studentcourses_batch_df = spark_session.createDataFrame(
        valid_studentcourses_data_batch
    )

    valid_studentcourses_batch_df = valid_studentcourses_batch_df.withColumn(
        "SC_C_KEY", lit("ABC")
    )
    return valid_studentcourses_batch_df


@pytest.fixture()
def course_data_batch_with_compare_delivery_model(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_KEY", lit("ABC")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_data_batch_with_missing_room_num2(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_NUM2"] = ""
    course_data_with_error["C_DAYS2"] = "WED"
    course_data_with_error["C_DELIVERY_METHOD"] = "A"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    course_data_with_error["C_SITE_TYPE2"] = "A01"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_invalid_room_max2(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_MAX2"] = "10000"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_invalid_room_type2(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_TYPE2"] = "XYZ"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_room_type2(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_TYPE2"] = ""
    course_data_with_error["C_DAYS2"] = "W"
    course_data_with_error["C_DELIVERY_METHOD"] = "A"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_SITE_TYPE2"] = "A01"
    course_data_with_error["C_EXTRACT"] = "3"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_room_type2_1(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_TYPE2"] = ""
    course_data_with_error["C_DAYS2"] = "W"
    course_data_with_error["C_DELIVERY_METHOD"] = "A"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_SITE_TYPE2"] = "A01"
    course_data_with_error["C_EXTRACT"] = "E"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_room_type2_in_space_utilization(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_TYPE2"] = "000"
    course_data_with_error["C_DAYS2"] = "W"
    course_data_with_error["C_DELIVERY_METHOD"] = "A"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_SITE_TYPE2"] = "A01"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_room_type2_2(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_TYPE"] = ""
    course_data_with_error["C_ROOM_TYPE2"] = ""
    course_data_with_error["C_DAYS2"] = "W"
    course_data_with_error["C_DELIVERY_METHOD"] = "B"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_SITE_TYPE2"] = "A01"
    course_data_with_error["C_EXTRACT"] = "3"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_room_type2_3(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_TYPE"] = ""
    course_data_with_error["C_ROOM_TYPE2"] = ""
    course_data_with_error["C_DAYS2"] = "W"
    course_data_with_error["C_DELIVERY_METHOD"] = "B"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_SITE_TYPE2"] = "A01"
    course_data_with_error["C_EXTRACT"] = "E"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_room_type2_4(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_TYPE"] = ""
    course_data_with_error["C_ROOM_TYPE2"] = ""
    course_data_with_error["C_DAYS2"] = "W"
    course_data_with_error["C_DELIVERY_METHOD"] = "P"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_SITE_TYPE2"] = "A01"
    course_data_with_error["C_EXTRACT"] = "3"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_room_type2_5(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_ROOM_TYPE"] = ""
    course_data_with_error["C_ROOM_TYPE2"] = ""
    course_data_with_error["C_DAYS2"] = "W"
    course_data_with_error["C_DELIVERY_METHOD"] = "P"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_SITE_TYPE2"] = "A01"
    course_data_with_error["C_EXTRACT"] = "E"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_invalid_start_time3(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_START_TIME3"] = "25:00:00"
    course_data_with_error["C_START_TIME2"] = ""
    course_data_with_error["C_SITE_TYPE2"] = "NV"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_start_time3(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_START_TIME3"] = ""
    course_data_with_error["C_DAYS3"] = "MWF"
    course_data_with_error["C_DELIVERY_METHOD"] = "NV"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    course_data_with_error["C_ROOM_TYPE3"] = "110"
    course_data_with_error["C_SITE_TYPE3"] = "A01"
    course_data_with_error["C_EXTRACT"] = "3"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_start_time3_1(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_START_TIME3"] = ""
    course_data_with_error["C_DAYS3"] = "MWF"
    course_data_with_error["C_DELIVERY_METHOD"] = "NV"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    course_data_with_error["C_ROOM_TYPE3"] = "110"
    course_data_with_error["C_SITE_TYPE3"] = "A01"
    course_data_with_error["C_EXTRACT"] = "E"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_start_time3_2(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_START_TIME3"] = ""
    course_data_with_error["C_DAYS3"] = "MWF"
    course_data_with_error["C_DELIVERY_METHOD"] = "NV"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    course_data_with_error["C_SITE_TYPE3"] = "A01"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_invalid_stop_time3(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_STOP_TIME3"] = "25:00:00"
    course_data_with_error["C_STOP_TIME2"] = ""
    course_data_with_error["C_SITE_TYPE2"] = "NV"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_stop_time3(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_STOP_TIME3"] = ""
    course_data_with_error["C_DAYS3"] = "MWF"
    course_data_with_error["C_DELIVERY_METHOD"] = "NV"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    course_data_with_error["C_ROOM_TYPE3"] = "110"
    course_data_with_error["C_SITE_TYPE3"] = "A01"
    course_data_with_error["C_EXTRACT"] = "3"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_stop_time3_1(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_STOP_TIME3"] = ""
    course_data_with_error["C_DAYS3"] = "MWF"
    course_data_with_error["C_DELIVERY_METHOD"] = "NV"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    course_data_with_error["C_ROOM_TYPE3"] = "110"
    course_data_with_error["C_SITE_TYPE3"] = "A01"
    course_data_with_error["C_EXTRACT"] = "E"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_stop_time3_2(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_STOP_TIME3"] = ""
    course_data_with_error["C_DAYS3"] = "MWF"
    course_data_with_error["C_DELIVERY_METHOD"] = "NV"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    course_data_with_error["C_SITE_TYPE3"] = "A01"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_invalid_days3(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_DAYS3"] = "MWF"
    course_data_with_error["C_DAYS2"] = ""
    course_data_with_error["C_SITE_TYPE2"] = "NV"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_data_batch_with_missing_days3(spark_session):
    invalid_course_data_batch = [make_valid_course_data() for _ in range(10)]
    course_data_with_error = make_valid_course_data()
    course_data_with_error["C_DAYS3"] = ""
    course_data_with_error["C_DELIVERY_METHOD"] = "NV"
    course_data_with_error["C_INSTRUCT_TYPE"] = "LEC"
    course_data_with_error["C_BUDGET_CODE"] = "NSF"
    course_data_with_error["C_ROOM_TYPE3"] = "110"
    course_data_with_error["C_SITE_TYPE3"] = "A01"
    course_data_with_error["C_EXTRACT"] = "3"
    invalid_course_data_batch.append(course_data_with_error)
    return spark_session.createDataFrame(invalid_course_data_batch)


@pytest.fixture()
def course_load_data_batch_with_invalid_days3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DAYS3", lit(None))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEC"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("e"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_days3_1(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DAYS3", lit(None))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEC"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_same_blgd_name_and_number(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_BLDG_SNAME3", lit("SAME")
    ).withColumn("C_BLDG_NUM3", lit("SAME"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_sname3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_BLDG_SNAME3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("3"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_sname3_1(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_BLDG_SNAME3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("E"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_sname3_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_BLDG_SNAME3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_snum3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_BLDG_NUM3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("3"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_snum3_1(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_BLDG_NUM3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("E"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_snum3_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INST", lit("5220")
    ).withColumn("C_BLDG_NUM3", lit("18"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_snum3_3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_INST", lit("5220")
    ).withColumn("C_BLDG_NUM3", lit("18"))

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_snum3_4(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_BLDG_NUM3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_room3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_NUM3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("3"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_room3_1(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_NUM3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("E"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_room3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_NUM3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_ROOM_TYPE3", lit("110"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("3"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_bldg_room3_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_NUM3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_max3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_ROOM_MAX3", lit("100000")
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_type3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_TYPE3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("3"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_type3_1(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_TYPE3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("E"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_type3_2(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("S"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_BUDGET_CODE", lit("NSF"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_ROOM_TYPE3", lit("060"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_type3_3(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = (
        valid_course_data_batch_df.withColumn("C_ROOM_TYPE", lit(None))
        .withColumn("C_ROOM_TYPE2", lit(None))
        .withColumn("C_ROOM_TYPE3", lit(None))
        .withColumn("C_DAYS3", lit("W"))
        .withColumn("C_DELIVERY_METHOD", lit("B"))
        .withColumn("C_INSTRUCT_TYPE", lit("LEL"))
        .withColumn("C_SITE_TYPE3", lit("A01"))
        .withColumn("C_EXTRACT", lit("3"))
    )

    return valid_course_data_batch_df


@pytest.fixture()
def course_load_data_batch_with_invalid_room_type3_4(spark_session):
    valid_course_data_batch = [make_valid_course_data() for _ in range(10)]

    valid_course_data_batch_df = spark_session.createDataFrame(valid_course_data_batch)

    valid_course_data_batch_df = valid_course_data_batch_df.withColumn(
        "C_ROOM_TYPE3", lit("060")
    )

    return valid_course_data_batch_df
