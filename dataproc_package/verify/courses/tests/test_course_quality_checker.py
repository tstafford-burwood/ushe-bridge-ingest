import os
from unittest.mock import patch

from pyspark.sql import functions as F

from dga_dataproc_package.verify.courses.CourseQualityChecker import (
    CourseQualityChecker,
)


class TestCourseQualityChecker:

    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = "/packages/dga_dataproc_package/tests/fixture_files/fake_service_account.json"

    def test_c00_duplicate_records_finds_duplicates(
        self,
        spark_session,
        course_data_batch_with_duplicates_dataframe,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_duplicates_dataframe
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        # Get institution id from df fixture
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        c00_quality_report = course_quality_checker.c00_duplicate_records()

        # There should be data in the dataframe
        report_schema = ["C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "count"]

        assert c00_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c00_quality_report.schema.names) == sorted(report_schema)

    def test_c00_duplicate_records_finds_no_duplicates(
        self,
        spark_session,
        valid_course_data_batch_dataframe,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = valid_course_data_batch_dataframe
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c00_quality_report = course_quality_checker.c00_duplicate_records()

        report_schema = ["C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "count"]

        # There should not be data in the dataframe
        assert c00_quality_report.count() == 0

        # The dataframe should have the correct schema
        assert sorted(c00_quality_report.schema.names) == sorted(report_schema)

    def test_c01_missing_institution_code_null_check(
        self,
        spark_session,
        course_data_batch_with_null_institution,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_null_institution
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            "courses/1/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        c01_quality_report = course_quality_checker.c01_missing_institution_code()

        report_schema = ["C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC"]

        # There should be data in the error dataframe
        assert c01_quality_report.count() > 0

        # Assert that the report dataframe contains the rows with null institution codes
        assert (
            c01_quality_report.filter(c01_quality_report.C_INST.isNull()).count()
            == course_data_batch_with_null_institution.count()
        )

        # The dataframe should have the correct schema
        assert sorted(c01_quality_report.schema.names) == sorted(report_schema)

    def test_c01_invalid_institution_code(
        self,
        spark_session,
        valid_course_data_batch_dataframe,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):

        invalid_inst_id = 1000000
        df = valid_course_data_batch_dataframe.withColumn(
            "C_INST", F.lit(invalid_inst_id)
        )
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            "courses/1/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c01_quality_report = course_quality_checker.c01_missing_institution_code()

        report_schema = ["C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC"]

        assert sorted(c01_quality_report.schema.names) == sorted(report_schema)
        assert (
            c01_quality_report.filter(
                c01_quality_report.C_INST == invalid_inst_id
            ).count()
            == valid_course_data_batch_dataframe.count()
        )

    def test_c02_blank_records_finds_blanks(
        self,
        spark_session,
        course_data_batch_with_blanks_dataframe,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_blanks_dataframe
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c02_quality_report = course_quality_checker.c02_blank_records()

        report_schema = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c02_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c02_quality_report.schema.names) == sorted(report_schema)

    def test_c04b_invalid_course_number_grad(
        self,
        spark_session,
        course_data_batch_with_invalid_course_number_grad,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_course_number_grad
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c04b_quality_report = course_quality_checker.c04b_invalid_course_number_grad()

        report_schema = ["C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN"]

        # There should not be data in the dataframe
        assert c04b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c04b_quality_report.schema.names) == sorted(report_schema)

    def test_c04a_invalid_course_number(
        self,
        spark_session,
        course_data_batch_invalid_course_number,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_invalid_course_number
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c04a_quality_report = course_quality_checker.c04a_invalid_course_number()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c04a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c04a_quality_report.schema.names) == sorted(report_schema)

    def test_c04c_invalid_course_number(
        self,
        spark_session,
        course_data_batch_invalid_course_number,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_invalid_course_number
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c04c_quality_report = course_quality_checker.c04c_invalid_course_number()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c04c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c04c_quality_report.schema.names) == sorted(report_schema)

    def test_c04d_invalid_course_number(
        self,
        spark_session,
        course_data_batch_invalid_course_number,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_invalid_course_number
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c04d_quality_report = course_quality_checker.c04d_invalid_course_number()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c04d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c04d_quality_report.schema.names) == sorted(report_schema)

    def test_c05_invalid_course_section(
        self,
        spark_session,
        course_data_batch_with_invalid_course_section,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_course_section
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c05_quality_report = course_quality_checker.c05_invalid_course_section()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c05_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c05_quality_report.schema.names) == sorted(report_schema)

    def test_c06a_invalid_min_credits(
        self,
        spark_session,
        course_data_batch_with_invalid_min_credits,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_min_credits
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c06a_quality_report = course_quality_checker.c06a_invalid_min_credits()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_MIN_CREDIT_ERROR",
        ]

        # There should not be data in the dataframe
        assert c06a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c06a_quality_report.schema.names) == sorted(report_schema)

    def test_c07a_invalid_max_credits(
        self,
        spark_session,
        course_data_batch_with_invalid_min_credits,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_min_credits
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c07a_quality_report = course_quality_checker.c07a_invalid_max_credits()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_MAX_CREDIT_ERROR",
        ]

        # There should not be data in the dataframe
        assert c07a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c07a_quality_report.schema.names) == sorted(report_schema)

    def test_c07b_invalid_credits(
        self,
        spark_session,
        course_data_batch_with_invalid_min_max_credits,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_min_max_credits
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c07b_quality_report = course_quality_checker.c07b_invalid_credits()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_MIN_CREDIT",
            "C_MAX_CREDIT",
        ]

        # There should not be data in the dataframe
        assert c07b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c07b_quality_report.schema.names) == sorted(report_schema)

    def test_c08a_invalid_contact_hrs(
        self,
        spark_session,
        course_data_batch_with_invalid_contact_hrs,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_contact_hrs
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c08a_quality_report = course_quality_checker.c08a_invalid_contact_hrs()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CONTACT_HRS_ERROR",
        ]

        # There should not be data in the dataframe
        assert c08a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c08a_quality_report.schema.names) == sorted(report_schema)

    def test_c09_invalid_line_item(
        self,
        spark_session,
        course_data_batch_with_line_item,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_line_item
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c09_quality_report = course_quality_checker.c09_invalid_line_item()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_LINE_ITEM",
        ]

        # There should not be data in the dataframe
        assert c09_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c09_quality_report.schema.names) == sorted(report_schema)

    def test_c09a_invalid_line_item(
        self,
        spark_session,
        course_data_batch_with_line_item_for_inst,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_line_item_for_inst
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c09a_quality_report = course_quality_checker.c09a_invalid_line_item()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_LINE_ITEM",
        ]

        # There should not be data in the dataframe
        assert c09a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c09a_quality_report.schema.names) == sorted(report_schema)

    def test_c10_invalid_site_type(
        self,
        spark_session,
        course_data_batch_with_site_type,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_site_type
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c10_quality_report = course_quality_checker.c10_invalid_site_type()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
        ]

        # There should not be data in the dataframe
        assert c10_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c10_quality_report.schema.names) == sorted(report_schema)

    def test_c10a_invalid_site_type(
        self,
        spark_session,
        course_data_batch_with_site_type10a,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_site_type10a
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c10a_quality_report = course_quality_checker.c10a_invalid_site_type()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
        ]

        # There should not be data in the dataframe
        assert c10a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c10a_quality_report.schema.names) == sorted(report_schema)

    def test_c11_invalid_budget_code(
        self,
        spark_session,
        course_data_batch_with_site_type10a,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_site_type10a
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c11_quality_report = course_quality_checker.c11_invalid_budget_code()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_BUDGET_CODE",
        ]

        # There should not be data in the dataframe
        assert c11_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c11_quality_report.schema.names) == sorted(report_schema)

    def test_c11a_invalid_budget_code(
        self,
        spark_session,
        course_data_batch_with_budget_code,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_budget_code
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c11a_quality_report = course_quality_checker.c11a_invalid_budget_code()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_BUDGET_CODE",
        ]

        # There should not be data in the dataframe
        assert c11a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c11a_quality_report.schema.names) == sorted(report_schema)

    def test_c12_invalid_delivery_method(
        self,
        spark_session,
        course_data_batch_with_delivery_method,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_delivery_method
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c12_quality_report = course_quality_checker.c12_invalid_delivery_method()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_DELIVERY_METHOD",
        ]

        # There should not be data in the dataframe
        assert c12_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c12_quality_report.schema.names) == sorted(report_schema)

    def test_c12a_invalid_delivery_method(
        self,
        spark_session,
        course_data_batch_with_delivery_method_12a,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_delivery_method_12a
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c12a_quality_report = course_quality_checker.c12a_invalid_delivery_method()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_DELIVERY_METHOD",
        ]

        # There should not be data in the dataframe
        assert c12a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c12a_quality_report.schema.names) == sorted(report_schema)

    def test_c13_invalid_program_type(
        self,
        spark_session,
        course_data_batch_with_program_type,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_program_type
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13_quality_report = course_quality_checker.c13_invalid_program_type()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_PROGRAM_TYPE",
        ]

        # There should not be data in the dataframe
        assert c13_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13_quality_report.schema.names) == sorted(report_schema)

    def test_c13a_invalid_program_type(
        self,
        spark_session,
        course_data_batch_with_program_typec13a,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13a,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_program_typec13a
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13a
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13a_quality_report = course_quality_checker.c13a_invalid_program_type()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13a_quality_report.schema.names) == sorted(report_schema)

    def test_c13b_invalid_program_type(
        self,
        spark_session,
        course_data_batch_with_program_typec13b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13a,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_program_typec13b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13a
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13b_quality_report = course_quality_checker.c13b_invalid_program_type()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13b_quality_report.schema.names) == sorted(report_schema)

    def test_c13c_invalid_program_type(
        self,
        spark_session,
        course_data_batch_with_program_typec13c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_program_typec13c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13c_quality_report = course_quality_checker.c13c_invalid_program_type()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13c_quality_report.schema.names) == sorted(report_schema)

    def test_c13e_invalid_vocational(
        self,
        spark_session,
        course_data_batch_with_program_typec13e,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_program_typec13e
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13e_quality_report = course_quality_checker.c13e_invalid_vocational()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13e_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13e_quality_report.schema.names) == sorted(report_schema)

    def test_c13f_invalid_vocational(
        self,
        spark_session,
        course_data_batch_with_invalid_vocational,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_vocational
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13f_quality_report = course_quality_checker.c13f_invalid_vocational()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13f_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13f_quality_report.schema.names) == sorted(report_schema)

    def test_c13g_invalid_vocational(
        self,
        spark_session,
        course_data_batch_with_invalid_vocationalc13g,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_vocationalc13g
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13g_quality_report = course_quality_checker.c13g_invalid_vocational()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13g_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13g_quality_report.schema.names) == sorted(report_schema)

    def test_c14a_invalid_credit_ind(
        self,
        spark_session,
        course_data_batch_with_invalid_credit_ind,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_credit_ind
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c14a_quality_report = course_quality_checker.c14a_invalid_credit_ind()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CREDIT_IND",
        ]

        # There should not be data in the dataframe
        assert c14a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c14a_quality_report.schema.names) == sorted(report_schema)

    def test_c13i_invalid_vocational(
        self,
        spark_session,
        course_data_batch_with_invalid_vocationalc13i,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13k,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_vocationalc13i
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13k
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13i_quality_report = course_quality_checker.c13i_invalid_vocational()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13i_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13i_quality_report.schema.names) == sorted(report_schema)

    def test_c13j_invalid_vocational(
        self,
        spark_session,
        course_data_batch_with_invalid_vocationalc13j,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13k,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_vocationalc13j
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13k
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13j_quality_report = course_quality_checker.c13j_invalid_vocational()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13j_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13j_quality_report.schema.names) == sorted(report_schema)

    def test_c13k_invalid_vocational(
        self,
        spark_session,
        course_data_batch_with_invalid_vocationalc13k,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13k,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_vocationalc13k
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13k
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c13k_quality_report = course_quality_checker.c13k_invalid_vocational()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c13k_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c13k_quality_report.schema.names) == sorted(report_schema)

    def test_c14b_invalid_credit_ind(
        self,
        spark_session,
        course_data_batch_with_invalid_credit_ind14b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_credit_ind14b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c14b_quality_report = course_quality_checker.c14b_invalid_credit_ind()

        report_schema = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CREDIT_IND",
        ]

        # There should not be data in the dataframe
        assert c14b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c14b_quality_report.schema.names) == sorted(report_schema)

    def test_c14c_invalid_vocational(
        self,
        spark_session,
        course_data_batch_with_invalid_vocational14c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_vocational14c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c14c_quality_report = course_quality_checker.c14c_invalid_vocational()

        report_schema = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_TITLE",
            "C_CREDIT_IND",
            "C_INSTRUCT_TYPE",
            "C_PROGRAM_TYPE",
            "C_BUDGET_CODE",
            "C_CONTACT_HRS",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c14c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c14c_quality_report.schema.names) == sorted(report_schema)

    def test_c15a_invalid_start_time(
        self,
        spark_session,
        course_data_batch_with_invalid_start_time,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_start_time
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c15a_quality_report = course_quality_checker.c15a_invalid_start_time()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_START_TIME",
        ]

        # There should not be data in the dataframe
        assert c15a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c15a_quality_report.schema.names) == sorted(report_schema)

    def test_c15b_invalid_start_time(
        self,
        spark_session,
        course_data_batch_with_invalid_start_timec15b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_start_timec15b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c15b_quality_report = course_quality_checker.c15b_invalid_start_time()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_START_TIME",
        ]

        # There should not be data in the dataframe
        assert c15b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c15b_quality_report.schema.names) == sorted(report_schema)

    def test_c15c_invalid_start_time(
        self,
        spark_session,
        course_data_batch_with_invalid_start_timec15b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_start_timec15b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c15c_quality_report = course_quality_checker.c15c_invalid_start_time()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_START_TIME",
        ]

        # There should not be data in the dataframe
        assert c15c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c15c_quality_report.schema.names) == sorted(report_schema)

    def test_c16a_invalid_stop_time(
        self,
        spark_session,
        course_data_batch_with_invalid_stop_time,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_stop_time
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c16a_quality_report = course_quality_checker.c16a_invalid_stop_time()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_STOP_TIME",
        ]

        # There should not be data in the dataframe
        assert c16a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c16a_quality_report.schema.names) == sorted(report_schema)

    def test_c16b_invalid_stop_time(
        self,
        spark_session,
        course_data_batch_with_invalid_stop_timec16b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_stop_timec16b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c16b_quality_report = course_quality_checker.c16b_invalid_stop_time()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_STOP_TIME",
        ]

        # There should not be data in the dataframe
        assert c16b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c16b_quality_report.schema.names) == sorted(report_schema)

    def test_c16c_invalid_stop_time(
        self,
        spark_session,
        course_data_batch_with_invalid_stop_timec16b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_stop_timec16b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c16c_quality_report = course_quality_checker.c16c_invalid_stop_time()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_STOP_TIME",
        ]

        # There should not be data in the dataframe
        assert c16c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c16c_quality_report.schema.names) == sorted(report_schema)

    def test_c17a_invalid_days(
        self,
        spark_session,
        course_data_batch_with_invalid_days,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_days
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c17a_quality_report = course_quality_checker.c17a_invalid_days()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c17a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c17a_quality_report.schema.names) == sorted(report_schema)

    def test_c17b_invalid_days(
        self,
        spark_session,
        course_data_batch_with_invalid_daysc17b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_daysc17b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c17b_quality_report = course_quality_checker.c17b_invalid_days()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c17b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c17b_quality_report.schema.names) == sorted(report_schema)

    def test_c17c_invalid_days(
        self,
        spark_session,
        course_data_batch_with_invalid_daysc17b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_daysc17b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c17c_quality_report = course_quality_checker.c17c_invalid_days()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c17c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c17c_quality_report.schema.names) == sorted(report_schema)

    def test_c18_invalid_bldg_name_and_num(
        self,
        spark_session,
        course_data_batch_with_invalid_bldg_name_num,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_bldg_name_num
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c18_quality_report = course_quality_checker.c18_invalid_bldg_name_and_num()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
        ]

        # There should not be data in the dataframe
        assert c18_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c18_quality_report.schema.names) == sorted(report_schema)

    def test_c18a_invalid_bldg_name(
        self,
        spark_session,
        course_data_batch_with_invalid_sname,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_sname
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c18a_quality_report = course_quality_checker.c18a_invalid_bldg_name()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_SNAME",
        ]

        # There should not be data in the dataframe
        assert c18a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c18a_quality_report.schema.names) == sorted(report_schema)

    def test_c18b_invalid_bldg_name(
        self,
        spark_session,
        course_data_batch_with_invalid_snamec18b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_snamec18b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c18b_quality_report = course_quality_checker.c18b_invalid_bldg_name()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_SNAME",
        ]

        # There should not be data in the dataframe
        assert c18b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c18b_quality_report.schema.names) == sorted(report_schema)

    def test_c18c_invalid_bldg_name(
        self,
        spark_session,
        course_data_batch_with_invalid_snamec18b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_snamec18b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c18c_quality_report = course_quality_checker.c18c_invalid_bldg_name()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_SNAME",
        ]

        # There should not be data in the dataframe
        assert c18c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c18c_quality_report.schema.names) == sorted(report_schema)

    def test_c19a_invalid_bldg_num(
        self,
        spark_session,
        course_data_batch_with_invalid_num,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_num
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c19a_quality_report = course_quality_checker.c19a_invalid_bldg_num()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_NUM",
        ]

        # There should not be data in the dataframe
        assert c19a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c19a_quality_report.schema.names) == sorted(report_schema)

    def test_c19b_invalid_bldg_num(
        self,
        spark_session,
        course_data_batch_with_invalid_numc19b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_numc19b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c19b_quality_report = course_quality_checker.c19b_invalid_bldg_num()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_NUM",
        ]

        # There should not be data in the dataframe
        assert c19b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c19b_quality_report.schema.names) == sorted(report_schema)

    def test_c19e_invalid_bldg_num(
        self,
        spark_session,
        course_data_batch_with_invalid_numc19b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_numc19b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c19e_quality_report = course_quality_checker.c19e_invalid_bldg_num()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_NUM",
        ]

        # There should not be data in the dataframe
        assert c19e_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c19e_quality_report.schema.names) == sorted(report_schema)

    def test_c20a_invalid_room_num(
        self,
        spark_session,
        course_data_batch_with_invalid_room_num,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_num
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c20a_quality_report = course_quality_checker.c20a_invalid_room_num()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_ROOM_NUM",
        ]

        # There should not be data in the dataframe
        assert c20a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c20a_quality_report.schema.names) == sorted(report_schema)

    def test_c20b_invalid_room_num(
        self,
        spark_session,
        course_data_batch_with_invalid_room_numc20b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_numc20b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c20b_quality_report = course_quality_checker.c20b_invalid_room_num()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_ROOM_NUM",
        ]

        # There should not be data in the dataframe
        assert c20b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c20b_quality_report.schema.names) == sorted(report_schema)

    def test_c20c_invalid_room_num(
        self,
        spark_session,
        course_data_batch_with_invalid_room_numc20b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_numc20b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c20c_quality_report = course_quality_checker.c20c_invalid_room_num()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_ROOM_NUM",
        ]

        # There should not be data in the dataframe
        assert c20c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c20c_quality_report.schema.names) == sorted(report_schema)

    def test_c22b_invalid_room_type(
        self,
        spark_session,
        course_data_batch_with_invalid_room_type,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_type
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22b_quality_report = course_quality_checker.c22b_invalid_room_type()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c22b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22b_quality_report.schema.names) == sorted(report_schema)

    def test_c22c_invalid_room_type(
        self,
        spark_session,
        course_data_batch_with_invalid_room_typec22c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_typec22c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22c_quality_report = course_quality_checker.c22c_invalid_room_type()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c22c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22c_quality_report.schema.names) == sorted(report_schema)

    def test_c23b_invalid_start_time2(
        self,
        spark_session,
        course_data_batch_with_invalid_start_time2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_start_time2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c23b_quality_report = course_quality_checker.c23b_invalid_start_time2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_TYPE",
            "C_START_TIME2",
        ]

        # There should not be data in the dataframe
        assert c23b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c23b_quality_report.schema.names) == sorted(report_schema)

    def test_c23c_invalid_start_time2(
        self,
        spark_session,
        course_data_batch_with_invalid_start_time2c23c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_start_time2c23c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c23c_quality_report = course_quality_checker.c23c_invalid_start_time2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_TYPE",
            "C_START_TIME2",
        ]

        # There should not be data in the dataframe
        assert c23c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c23c_quality_report.schema.names) == sorted(report_schema)

    def test_c23d_invalid_start_time2(
        self,
        spark_session,
        course_data_batch_with_invalid_start_time2c23c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_start_time2c23c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c23d_quality_report = course_quality_checker.c23d_invalid_start_time2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_TYPE",
            "C_START_TIME2",
        ]

        # There should not be data in the dataframe
        assert c23d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c23d_quality_report.schema.names) == sorted(report_schema)

    def test_c24b_invalid_stop_time2(
        self,
        spark_session,
        course_data_batch_with_invalid_stop_time2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_stop_time2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c24b_quality_report = course_quality_checker.c24b_invalid_stop_time2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_STOP_TIME2",
        ]

        # There should not be data in the dataframe
        assert c24b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c24b_quality_report.schema.names) == sorted(report_schema)

    def test_c24c_invalid_stop_time2(
        self,
        spark_session,
        course_data_batch_with_invalid_stop_time2c24c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_stop_time2c24c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c24c_quality_report = course_quality_checker.c24c_invalid_stop_time2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_STOP_TIME2",
        ]

        # There should not be data in the dataframe
        assert c24c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c24c_quality_report.schema.names) == sorted(report_schema)

    def test_c24d_invalid_stop_time2(
        self,
        spark_session,
        course_data_batch_with_invalid_stop_time2c24c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_stop_time2c24c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c24d_quality_report = course_quality_checker.c24d_invalid_stop_time2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_STOP_TIME2",
        ]

        # There should not be data in the dataframe
        assert c24d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c24d_quality_report.schema.names) == sorted(report_schema)

    def test_c25b_invalid_days2(
        self,
        spark_session,
        course_data_batch_with_invalid_days2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_days2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c25b_quality_report = course_quality_checker.c25b_invalid_days2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_DAYS2",
        ]

        # There should not be data in the dataframe
        assert c25b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c25b_quality_report.schema.names) == sorted(report_schema)

    def test_c25c_invalid_days2(
        self,
        spark_session,
        course_data_batch_with_invalid_days2c25c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_days2c25c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c25c_quality_report = course_quality_checker.c25c_invalid_days2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_DAYS2",
        ]

        # There should not be data in the dataframe
        assert c25c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c25c_quality_report.schema.names) == sorted(report_schema)

    def test_c25d_invalid_days2(
        self,
        spark_session,
        course_data_batch_with_invalid_days2c25c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_days2c25c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c25d_quality_report = course_quality_checker.c25d_invalid_days2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_DAYS2",
        ]

        # There should not be data in the dataframe
        assert c25d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c25d_quality_report.schema.names) == sorted(report_schema)

    def test_c26a_invalid_sname2(
        self,
        spark_session,
        course_data_batch_with_invalid_sname2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_sname2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c26a_quality_report = course_quality_checker.c26a_invalid_sname2()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_SNAME2",
        ]

        # There should not be data in the dataframe
        assert c26a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c26a_quality_report.schema.names) == sorted(report_schema)

    def test_c26b_invalid_sname2(
        self,
        spark_session,
        course_data_batch_with_invalid_sname2c26b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_sname2c26b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c26b_quality_report = course_quality_checker.c26b_invalid_sname2()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_SNAME2",
        ]

        # There should not be data in the dataframe
        assert c26b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c26b_quality_report.schema.names) == sorted(report_schema)

    def test_c26c_invalid_sname2(
        self,
        spark_session,
        course_data_batch_with_invalid_sname2c26b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_sname2c26b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c26c_quality_report = course_quality_checker.c26c_invalid_sname2()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_SNAME2",
        ]

        # There should not be data in the dataframe
        assert c26c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c26c_quality_report.schema.names) == sorted(report_schema)

    def test_c11b_invalid_ceml(
        self,
        spark_session,
        course_data_batch_with_ceml,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_ceml
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c11b_quality_report = course_quality_checker.c11b_invalid_ceml()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c11b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c11b_quality_report.schema.names) == sorted(report_schema)

    def test_c11c_invalid_ceml(
        self,
        spark_session,
        course_data_batch_with_cemlc11c,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_cemlc11c
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c11c_quality_report = course_quality_checker.c11c_invalid_ceml()

        report_schema = [
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_CRN",
        ]

        # There should not be data in the dataframe
        assert c11c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c11c_quality_report.schema.names) == sorted(report_schema)

    def test_c19c_invalid_bldg_num(
        self,
        spark_session,
        course_data_batch_with_invalid_bldg_and_b_num,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_bldg_and_b_num
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c19c_quality_report = course_quality_checker.c19c_invalid_bldg_num()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_NUM",
        ]

        # There should not be data in the dataframe
        assert c19c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c19c_quality_report.schema.names) == sorted(report_schema)

    def test_c19d_invalid_r_bldg_num(
        self,
        spark_session,
        course_data_batch_with_invalid_bldg_and_b_num,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_bldg_and_b_num
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c19d_quality_report = course_quality_checker.c19d_invalid_r_bldg_num()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_NUM",
        ]

        # There should not be data in the dataframe
        assert c19d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c19d_quality_report.schema.names) == sorted(report_schema)

    def test_c21a_invalid_room_max(
        self,
        spark_session,
        course_data_batch_with_invalid_room_max,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_max
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c21a_quality_report = course_quality_checker.c21a_invalid_room_max()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRN",
            "C_ROOM_MAX",
            "C_ROOM_MAX_ERROR",
        ]

        # There should not be data in the dataframe
        assert c21a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c21a_quality_report.schema.names) == sorted(report_schema)

    def test_c22_room_type_count(
        self,
        spark_session,
        course_data_batch_with_invalid_room_max,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_max
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22_quality_report = course_quality_checker.c22_room_type_count()

        report_schema = ["C_INST", "C_ROOM_TYPE", "DESCRIPTION", "NUMBER_OF_COURSES"]

        # There should not be data in the dataframe
        assert c22_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22_quality_report.schema.names) == sorted(report_schema)

    def test_c22a_invalid_room_type(
        self,
        spark_session,
        course_data_batch_with_invalid_use_code,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_use_code
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22a_quality_report = course_quality_checker.c22a_invalid_room_type()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_TERM",
            "C_EXTRACT",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_ROOM_TYPE",
        ]

        # There should not be data in the dataframe
        assert c22a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22a_quality_report.schema.names) == sorted(report_schema)

    def test_c22d_invalid_room_type(
        self,
        spark_session,
        course_data_batch_with_invalid_room_typec22d,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_typec22d
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22d_quality_report = course_quality_checker.c22d_invalid_room_type()

        report_schema = ["C_INST", "C_ROOM_TYPE", "DESCRIPTION", "NUMBER_OF_COURSES"]

        # There should not be data in the dataframe
        assert c22d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22d_quality_report.schema.names) == sorted(report_schema)

    def test_c22e_invalid_room_type(
        self,
        spark_session,
        course_data_batch_with_invalid_room_typec22d,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_typec22d
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22e_quality_report = course_quality_checker.c22e_invalid_room_type()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c22e_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22e_quality_report.schema.names) == sorted(report_schema)

    def test_c38f_room_type_missing_delivery_method_b(
        self,
        spark_session,
        course_data_batch_with_room_type_missing_delivery_method_b,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_room_type_missing_delivery_method_b
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c38f_quality_report = (
            course_quality_checker.c38f_room_type_missing_delivery_method_b()
        )

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c38f_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c38f_quality_report.schema.names) == sorted(report_schema)

    def test_c38g_room_type_missing_delivery_method_p(
        self,
        spark_session,
        course_data_batch_with_room_type_missing_delivery_method_p,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_room_type_missing_delivery_method_p
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c38g_quality_report = (
            course_quality_checker.c38g_room_type_missing_delivery_method_p()
        )

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c38g_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c38g_quality_report.schema.names) == sorted(report_schema)

    def test_c38h_room_type_missing_delivery_method_p(
        self,
        spark_session,
        course_data_batch_with_room_type_missing_delivery_method_p_1,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_room_type_missing_delivery_method_p_1
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c38h_quality_report = (
            course_quality_checker.c38h_room_type_missing_delivery_method_p()
        )

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c38h_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c38h_quality_report.schema.names) == sorted(report_schema)

    def test_c39a_start_date_summer(
        self,
        spark_session,
        course_data_batch_start_date_summer,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_start_date_summer
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c39a_quality_report = course_quality_checker.c39a_start_date_summer()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_COLLEGE",
            "C_CRN",
            "C_START_DATE",
        ]

        # There should not be data in the dataframe
        assert c39a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c39a_quality_report.schema.names) == sorted(report_schema)

    def test_c39b_start_date_fall(
        self,
        spark_session,
        course_data_batch_start_date_fall,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_start_date_fall
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c39b_quality_report = course_quality_checker.c39b_start_date_fall()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_COLLEGE",
            "C_CRN",
            "C_START_DATE",
        ]

        # There should not be data in the dataframe
        assert c39b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c39b_quality_report.schema.names) == sorted(report_schema)

    def test_c39c_start_date_spring(
        self,
        spark_session,
        course_data_batch_start_date_spring,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_start_date_spring
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c39c_quality_report = course_quality_checker.c39c_start_date_spring()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_COLLEGE",
            "C_CRN",
            "C_START_DATE",
        ]

        # There should not be data in the dataframe
        assert c39c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c39c_quality_report.schema.names) == sorted(report_schema)

    def test_c40a_end_date_summer(
        self,
        spark_session,
        course_data_batch_end_date_summer,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_end_date_summer
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c40a_quality_report = course_quality_checker.c40a_end_date_summer()
        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_COLLEGE",
            "C_CRN",
            "C_START_DATE",
        ]

        # There should not be data in the dataframe
        assert c40a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c40a_quality_report.schema.names) == sorted(report_schema)

    def test_c40b_end_date_fall(
        self,
        spark_session,
        course_data_batch_end_date_fall,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_end_date_fall
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c40b_quality_report = course_quality_checker.c40b_end_date_fall()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_COLLEGE",
            "C_CRN",
            "C_START_DATE",
        ]

        # There should not be data in the dataframe
        assert c40b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c40b_quality_report.schema.names) == sorted(report_schema)

    def test_c40c_start_date_spring(
        self,
        spark_session,
        course_data_batch_end_date_spring,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_end_date_spring
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c40c_quality_report = course_quality_checker.c40c_end_date_spring()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_COLLEGE",
            "C_CRN",
            "C_START_DATE",
        ]

        # There should not be data in the dataframe
        assert c40c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c40c_quality_report.schema.names) == sorted(report_schema)

    def test_c41a_title_missing(
        self,
        spark_session,
        course_data_batch_title_missing,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_title_missing
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c41a_quality_report = course_quality_checker.c41a_title_missing()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_TITLE",
        ]
        # There should not be data in the dataframe
        assert c41a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c41a_quality_report.schema.names) == sorted(report_schema)

    def test_c51c_course_level_counts(
        self,
        spark_session,
        valid_course_data_batch_dataframe,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = valid_course_data_batch_dataframe
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c51c_quality_report = course_quality_checker.c51c_course_level_counts()

        report_schema = ["C_INST", "C_YEAR", "C_TERM", "C_EXTRACT", "C_LEVEL", "Levels"]
        # There should not be data in the dataframe
        assert c51c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c51c_quality_report.schema.names) == sorted(report_schema)

    def test_c52a_missing_crn(
        self,
        spark_session,
        course_data_batch_crn_missing,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_crn_missing
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c52a_quality_report = course_quality_checker.c52a_missing_crn()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DEPT",
            "C_CRN",
        ]
        # There should not be data in the dataframe
        assert c52a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c52a_quality_report.schema.names) == sorted(report_schema)

    def test_c52b_invalid_crn(
        self,
        spark_session,
        course_data_batch_crn_invalid,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_crn_invalid
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c52b_quality_report = course_quality_checker.c52b_invalid_crn()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DEPT",
            "C_CRN",
            "C_CRN_Error",
        ]
        # There should not be data in the dataframe
        assert c52b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c52b_quality_report.schema.names) == sorted(report_schema)

    def test_c52c_non_unique_crn(
        self,
        spark_session,
        course_data_batch_with_duplicates_dataframe,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_duplicates_dataframe
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c52c_quality_report = course_quality_checker.c52c_non_unique_crn()

        report_schema = ["C_INST", "C_CRN", "Count"]
        # There should not be data in the dataframe
        assert c52c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c52c_quality_report.schema.names) == sorted(report_schema)

    def test_c53_validate_site_type2(
        self,
        spark_session,
        course_data_batch_with_invalid_site_type2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_site_type2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c53_quality_report = course_quality_checker.c53_validate_site_type2()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE2",
        ]
        # There should not be data in the dataframe
        assert c53_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c53_quality_report.schema.names) == sorted(report_schema)

    def test_c53a_validate_values_site_type2(
        self,
        spark_session,
        course_data_batch_with_invalid_values_site_type2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_values_site_type2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c53a_quality_report = course_quality_checker.c53a_validate_values_site_type2()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE2",
        ]
        # There should not be data in the dataframe
        assert c53a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c53a_quality_report.schema.names) == sorted(report_schema)

    def test_c54_validate_site_type3(
        self,
        spark_session,
        course_data_batch_with_invalid_site_type3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_site_type3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c54_quality_report = course_quality_checker.c54_validate_site_type3()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE2",
        ]
        # There should not be data in the dataframe
        assert c54_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c54_quality_report.schema.names) == sorted(report_schema)

    def test_c54a_validate_values_site_type3(
        self,
        spark_session,
        course_data_batch_with_invalid_values_site_type3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_values_site_type3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c54a_quality_report = course_quality_checker.c54a_validate_values_site_type3()
        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE2",
        ]
        # There should not be data in the dataframe
        assert c54a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c54a_quality_report.schema.names) == sorted(report_schema)

    def test_c22f_invalid_room_type(
        self,
        spark_session,
        course_data_batch_with_invalid_room_typec22d,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22f,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_typec22d
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22f
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22f_quality_report = course_quality_checker.c22f_invalid_room_type()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c22f_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22f_quality_report.schema.names) == sorted(report_schema)

    def test_c22g_invalid_room_type(
        self,
        spark_session,
        course_data_batch_with_invalid_room_typec22d,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22g,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_typec22d
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22g
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22g_quality_report = course_quality_checker.c22g_invalid_room_type()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c22g_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22g_quality_report.schema.names) == sorted(report_schema)

    def test_c22h_invalid_room_type(
        self,
        spark_session,
        course_data_batch_with_invalid_room_typec22d,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22h,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_typec22d
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22h
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c22h_quality_report = course_quality_checker.c22h_invalid_room_type()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c22h_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c22h_quality_report.schema.names) == sorted(report_schema)

    def test_c23a_invalid_course_start_time(
        self,
        spark_session,
        course_data_batch_with_invalid_course_start_time,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_course_start_time
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c23a_quality_report = course_quality_checker.c23a_invalid_course_start_time()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_START_TIME",
            "C_START_TIME2",
        ]

        # There should not be data in the dataframe
        assert c23a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c23a_quality_report.schema.names) == sorted(report_schema)

    def test_c24a_invalid_course_stop_time(
        self,
        spark_session,
        course_data_batch_with_invalid_course_stop_time,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_course_stop_time
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c24a_quality_report = course_quality_checker.c24a_invalid_course_stop_time()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_STOP_TIME",
            "C_STOP_TIME2",
        ]

        # There should not be data in the dataframe
        assert c24a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c24a_quality_report.schema.names) == sorted(report_schema)

    def test_c25a_invalid_course_days2(
        self,
        spark_session,
        course_data_batch_with_invalid_course_days2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_course_days2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c25a_quality_report = course_quality_checker.c25a_invalid_course_days2()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_DAYS",
            "C_DAYS2",
        ]

        # There should not be data in the dataframe
        assert c25a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c25a_quality_report.schema.names) == sorted(report_schema)

    # def test_c26a_invalid_building_name(
    #     self,
    #     spark_session,
    #     course_data_batch_with_invalid_building_name,
    #     valid_studentcourses_data_batch_dataframe,
    #     valid_student_data_batch_dataframe,
    #     valid_building_prod_data_batch_dataframe,
    #     valid_room_prod_data_batch_dataframe,
    #     valid_course_loads_data_batch_dataframe,
    #     valid_perkins2015_prod_data_batch_dataframe,
    #     valid_logan_perkins_prod_data_batch_dataframe,
    #     valid_cc_master_list_prod_data_batch_dataframe,
    # ):
    #     df = course_data_batch_with_invalid_building_name
    #     studentcourses_df = valid_studentcourses_data_batch_dataframe
    #     students_df = valid_student_data_batch_dataframe
    #     buildings_prod_df = valid_building_prod_data_batch_dataframe
    #     rooms_prod_df = valid_room_prod_data_batch_dataframe
    #     courses_load_prod_df = valid_course_loads_data_batch_dataframe
    #     perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
    #     logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
    #     cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
    #     inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
    #     course_quality_checker = CourseQualityChecker(
    #         df,
    #         students_df,
    #         studentcourses_df,
    #         buildings_prod_df,
    #         rooms_prod_df,
    #         courses_load_prod_df,
    #         perkins2015_prod_df,
    #         logan_perkins_prod_df,
    #         cc_master_list_prod_df,
    #         "test_pk-1",
    #         "test-project",
    #         "test-pubsub-topic",
    #         f"courses/{inst_id}/2024/02/02/1/courses.txt",
    #         "test-bucket",
    #         use_local_filepaths=True,
    #     )
    #     c26a_quality_report = course_quality_checker.c26a_invalid_building_name()

    #     report_schema = [
    #         "C_INST",
    #         "C_BLDG_SNAME2",
    #         "C_ROOM_TYPE2",
    #         "C_CRS_SBJ",
    #         "C_CRS_NUM",
    #         "C_CRS_SEC",
    #         "C_DAYS2"
    #         "C_DELIVERY_METHOD",
    #         "C_INSTRUCT_TYPE",
    #         "C_SITE_TYPE2",
    #         "C_CRN",
    #         "C_BLDG_NUM2",
    #     ]

    #     # There should not be data in the dataframe
    #     assert c26a_quality_report.count() > 0

    #     # The dataframe should have the correct schema
    #     assert sorted(c26a_quality_report.schema.names) == sorted(report_schema)

    def test_c27a_buildingnum2_cond_required(
        self,
        spark_session,
        course_data_batch_with_buildingnum2_cond_required,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_buildingnum2_cond_required
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c27a_quality_report = course_quality_checker.c27a_buildingnum2_cond_required()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_NUM2",
        ]

        # There should not be data in the dataframe
        assert c27a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c27a_quality_report.schema.names) == sorted(report_schema)

    def test_c27b_buildingnum2_cond_required(
        self,
        spark_session,
        course_data_batch_with_buildingnum2_cond_required_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_buildingnum2_cond_required_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c27b_quality_report = course_quality_checker.c27b_buildingnum2_cond_required()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_NUM2",
        ]

        # There should not be data in the dataframe
        assert c27b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c27b_quality_report.schema.names) == sorted(report_schema)

    def test_c27e_buildingnum2_cond_required(
        self,
        spark_session,
        course_data_batch_with_buildingnum2_cond_required_3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_buildingnum2_cond_required_3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c27e_quality_report = course_quality_checker.c27e_buildingnum2_cond_required()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_NUM2",
        ]

        # There should not be data in the dataframe
        assert c27e_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c27e_quality_report.schema.names) == sorted(report_schema)

    def test_c28a_roomnum2_cond_required(
        self,
        spark_session,
        course_data_batch_with_roomnum2_cond_required,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_roomnum2_cond_required
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c28a_quality_report = course_quality_checker.c28a_roomnum2_cond_required()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_NUM2",
        ]

        # There should not be data in the dataframe
        assert c28a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c28a_quality_report.schema.names) == sorted(report_schema)

    def test_c28b_roomnum2_cond_required(
        self,
        spark_session,
        course_data_batch_with_roomnum2_cond_required_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_roomnum2_cond_required_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c28b_quality_report = course_quality_checker.c28b_roomnum2_cond_required()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_NUM2",
        ]

        # There should not be data in the dataframe
        assert c28b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c28b_quality_report.schema.names) == sorted(report_schema)

    def test_c27c_building_num2_not_in_building_inventory(
        self,
        spark_session,
        course_data_batch_with_invalid_bldg_and_b_inv,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null_2,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_bldg_and_b_inv
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null_2
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c27c_quality_report = (
            course_quality_checker.c27c_building_num2_not_in_building_inventory()
        )

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_NUM2",
        ]

        # There should not be data in the dataframe
        assert c27c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c27c_quality_report.schema.names) == sorted(report_schema)

    def test_c27d_building_num2_not_in_room_inventory(
        self,
        spark_session,
        course_data_batch_with_invalid_building_num2_not_in_room_inventory,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        rooms_prod_data_batch_dataframe_bnumber_null_2,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_building_num2_not_in_room_inventory
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null_2
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c27d_quality_report = (
            course_quality_checker.c27d_building_num2_not_in_room_inventory()
        )

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_NUM2",
        ]

        # There should not be data in the dataframe
        assert c27d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c27d_quality_report.schema.names) == sorted(report_schema)

    def test_c41b_course_title_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_course_title,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_course_title
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c41b_quality_report = course_quality_checker.c41b_course_title_invalid()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_TITLE",
        ]

        # There should not be data in the dataframe
        assert c41b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c41b_quality_report.schema.names) == sorted(report_schema)

    def test_c41c_course_title_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_course_title_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_course_title_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c41c_quality_report = course_quality_checker.c41c_course_title_invalid()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_TITLE",
        ]

        # There should not be data in the dataframe
        assert c41c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c41c_quality_report.schema.names) == sorted(report_schema)

    def test_c41d_course_title_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_course_title_3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_course_title_3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c41d_quality_report = course_quality_checker.c41d_course_title_invalid()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_TITLE",
        ]

        # There should not be data in the dataframe
        assert c41d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c41d_quality_report.schema.names) == sorted(report_schema)

    def test_c42a_instructor_id_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_instructor_id,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_instructor_id
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c42a_quality_report = course_quality_checker.c42a_instructor_id_invalid()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ]

        # There should not be data in the dataframe
        assert c42a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c42a_quality_report.schema.names) == sorted(report_schema)

    def test_c42b_instructor_id_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_instructor_id_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_instructor_id_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c42b_quality_report = course_quality_checker.c42b_instructor_id_invalid()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ]

        # There should not be data in the dataframe
        assert c42b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c42b_quality_report.schema.names) == sorted(report_schema)

    def test_c43a_instructor_name_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_instructor_name,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_instructor_name
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c43a_quality_report = course_quality_checker.c43a_instructor_name_invalid()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ]

        # There should not be data in the dataframe
        assert c43a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c43a_quality_report.schema.names) == sorted(report_schema)

    def test_c43c_instructor_name_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_instructor_name_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_instructor_name_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c43c_quality_report = course_quality_checker.c43c_instructor_name_invalid()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ]

        # There should not be data in the dataframe
        assert c43c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c43c_quality_report.schema.names) == sorted(report_schema)

    def test_c44_instruction_type_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_instruction_type_null,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_instruction_type_null
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c44_quality_report = course_quality_checker.c44_instruction_type_invalid()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_TYPE",
        ]

        # There should not be data in the dataframe
        assert c44_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c44_quality_report.schema.names) == sorted(report_schema)

    def test_c45_college_exists(
        self,
        spark_session,
        course_data_batch_with_college_exists,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_college_exists
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c45_quality_report = course_quality_checker.c45_college_exists()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_COLLEGE",
        ]

        # There should not be data in the dataframe
        assert c45_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c45_quality_report.schema.names) == sorted(report_schema)

    def test_c42c_invalid_instruct_id(
        self,
        spark_session,
        course_data_batch_with_invalid_instruct_id,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_instruct_id
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c42c_quality_report = course_quality_checker.c42c_invalid_instruct_id()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ]

        # There should not be data in the dataframe
        assert c42c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c42c_quality_report.schema.names) == sorted(report_schema)

    def test_c44a_valid_instruct_type(
        self,
        spark_session,
        course_data_batch_with_invalid_instruct_type,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_instruct_type
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c44a_quality_report = course_quality_checker.c44a_valid_instruct_type()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_TYPE",
        ]

        # There should not be data in the dataframe
        assert c44a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c44a_quality_report.schema.names) == sorted(report_schema)

    def test_c45a_invalid_college_name(
        self,
        spark_session,
        course_data_batch_with_invalid_college_name,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_college_name
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c45a_quality_report = course_quality_checker.c45a_invalid_college_name()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_COLLEGE",
        ]

        # There should not be data in the dataframe
        assert c45a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c45a_quality_report.schema.names) == sorted(report_schema)

    def test_c46_department_exists(
        self,
        spark_session,
        course_data_batch_with_department,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_department
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c46_quality_report = course_quality_checker.c46_department_exists()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DEPT",
            "LEVELS",
        ]

        # There should not be data in the dataframe
        assert c46_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c46_quality_report.schema.names) == sorted(report_schema)

    def test_c48a_dest_site_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_dest_site,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_dest_site
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c48a_quality_report = course_quality_checker.c48a_dest_site_invalid()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_CRN",
            "C_DEST_SITE",
        ]

        # There should not be data in the dataframe
        assert c48a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c48a_quality_report.schema.names) == sorted(report_schema)

    def test_c49a_invalid_students_enrolled(
        self,
        spark_session,
        course_data_batch_with_invalid_students_enrolled,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_students_enrolled
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c49a_quality_report = course_quality_checker.c49a_invalid_students_enrolled()

        report_schema = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CLASS_SIZE",
        ]

        # There should not be data in the dataframe
        assert c49a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c49a_quality_report.schema.names) == sorted(report_schema)

    def test_c49b_invalid_class_size(
        self,
        spark_session,
        course_data_batch_with_invalid_class_size,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_class_size
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c49b_quality_report = course_quality_checker.c49b_invalid_class_size()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CLASS_SIZE",
            "C_CLASS_SIZE_Error",
        ]

        # There should not be data in the dataframe
        assert c49b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c49b_quality_report.schema.names) == sorted(report_schema)

    def test_c51a_invalid_c_level(
        self,
        spark_session,
        course_data_batch_with_invalid_c_level,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_c_level
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c51a_quality_report = course_quality_checker.c51a_invalid_c_level()

        report_schema = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_LEVEL",
        ]

        # There should not be data in the dataframe
        assert c51a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c51a_quality_report.schema.names) == sorted(report_schema)

    def test_c47b_gen_ed_invalid(
        self,
        spark_session,
        course_data_batch_with_invalid_c_ge_ed,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_c_ge_ed
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c47b_quality_report = course_quality_checker.c47b_gen_ed_invalid()

        report_schema = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_GEN_ED",
        ]

        # There should not be data in the dataframe
        assert c47b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c47b_quality_report.schema.names) == sorted(report_schema)

    def test_c47a_compare_c_gen_ed(
        self,
        spark_session,
        course_data_batch_with_compare_c_gen_ed,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_compare_c_gen_ed
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            "courses/1/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c47a_quality_report = course_quality_checker.c47a_compare_c_gen_ed()

        report_schema = ["C_INST", "Total Count", "Gen Ed Count"]

        # There should not be data in the dataframe
        assert c47a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c47a_quality_report.schema.names) == sorted(report_schema)

    def test_c50_count_c_delivery_model(
        self,
        spark_session,
        valid_course_data_batch_dataframe,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = valid_course_data_batch_dataframe
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c50_quality_report = course_quality_checker.c50_count_c_delivery_model()

        report_schema = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_DELIVERY_MODEL",
            "COURSES",
        ]

        # There should not be data in the dataframe
        assert c50_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c50_quality_report.schema.names) == sorted(report_schema)

    def test_c51b_invalid_c_level_remedial(
        self,
        spark_session,
        course_data_batch_with_compare_c_level_remedial,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_compare_c_level_remedial
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c51b_quality_report = course_quality_checker.c51b_invalid_c_level_remedial()

        report_schema = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_LEVEL",
        ]

        # There should not be data in the dataframe
        assert c51b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c51b_quality_report.schema.names) == sorted(report_schema)

    def test_c49c_count_c_delivery_model(
        self,
        spark_session,
        course_data_batch_with_compare_delivery_model,
        valid_studentcourses_data_batch_dataframe_delivery_model,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_compare_delivery_model
        studentcourses_df = valid_studentcourses_data_batch_dataframe_delivery_model
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c49c_quality_report = course_quality_checker.c49c_count_c_delivery_model()

        report_schema = [
            "COUNT_SC_ID",
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CLASS_SIZE",
        ]

        # There should not be data in the dataframe
        assert c49c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c49c_quality_report.schema.names) == sorted(report_schema)

    ###################################
    def test_c28c_missing_room_num2(
        self,
        spark_session,
        course_data_batch_with_missing_room_num2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_room_num2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c28c_quality_report = course_quality_checker.c28c_missing_room_num2()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_NUM2",
        ]

        # There should not be data in the dataframe
        assert c28c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c28c_quality_report.schema.names) == sorted(report_schema)

    def test_c29a_invalid_room_max2(
        self,
        spark_session,
        course_data_batch_with_invalid_room_max2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_max2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c29a_quality_report = course_quality_checker.c29a_invalid_room_max2()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRN",
            "C_ROOM_MAX2",
            "C_ROOM_MAX2_error",
        ]

        # There should not be data in the dataframe
        assert c29a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c29a_quality_report.schema.names) == sorted(report_schema)

    def test_c30a_invalid_room_type2(
        self,
        spark_session,
        course_data_batch_with_invalid_room_type2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_room_type2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c30a_quality_report = course_quality_checker.c30a_invalid_room_type2()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_ROOM_TYPE2",
        ]

        # There should not be data in the dataframe
        assert c30a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c30a_quality_report.schema.names) == sorted(report_schema)

    def test_c30b_missing_room_type2(
        self,
        spark_session,
        course_data_batch_with_missing_room_type2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_room_type2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c30b_quality_report = course_quality_checker.c30b_missing_room_type2()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c30b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c30b_quality_report.schema.names) == sorted(report_schema)

    def test_c30c_missing_room_type2(
        self,
        spark_session,
        course_data_batch_with_missing_room_type2_1,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_room_type2_1
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c30c_quality_report = course_quality_checker.c30c_missing_room_type2()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c30c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c30c_quality_report.schema.names) == sorted(report_schema)

    def test_c30d_room_type2_in_space_utilization(
        self,
        spark_session,
        course_data_batch_with_room_type2_in_space_utilization,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_room_type2_in_space_utilization
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c30d_quality_report = (
            course_quality_checker.c30d_room_type2_in_space_utilization()
        )

        report_schema = [
            "C_INST",
            "C_ROOM_TYPE2",
            "R_Use_Name",
        ]

        # There should not be data in the dataframe
        assert c30d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c30d_quality_report.schema.names) == sorted(report_schema)

    def test_c30e_missing_room_type2(
        self,
        spark_session,
        course_data_batch_with_missing_room_type2_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_room_type2_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c30e_quality_report = course_quality_checker.c30e_missing_room_type2()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c30e_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c30e_quality_report.schema.names) == sorted(report_schema)

    def test_c30f_missing_room_type2(
        self,
        spark_session,
        course_data_batch_with_missing_room_type2_3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_room_type2_3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c30f_quality_report = course_quality_checker.c30f_missing_room_type2()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c30f_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c30f_quality_report.schema.names) == sorted(report_schema)

    def test_c30g_missing_room_type2(
        self,
        spark_session,
        course_data_batch_with_missing_room_type2_4,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_room_type2_4
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c30g_quality_report = course_quality_checker.c30g_missing_room_type2()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c30g_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c30g_quality_report.schema.names) == sorted(report_schema)

    def test_c30h_missing_room_type2(
        self,
        spark_session,
        course_data_batch_with_missing_room_type2_5,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_room_type2_5
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c30h_quality_report = course_quality_checker.c30h_missing_room_type2()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c30h_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c30h_quality_report.schema.names) == sorted(report_schema)

    #################################

    def test_c31a_invalid_start_time3(
        self,
        spark_session,
        course_data_batch_with_invalid_start_time3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_start_time3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c31a_quality_report = course_quality_checker.c31a_invalid_start_time3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_SITE_TYPE3",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_START_TIME",
            "C_START_TIME2",
            "C_START_TIME3",
        ]

        # There should not be data in the dataframe
        assert c31a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c31a_quality_report.schema.names) == sorted(report_schema)

    def test_c31b_missing_start_time3(
        self,
        spark_session,
        course_data_batch_with_missing_start_time3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_start_time3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c31b_quality_report = course_quality_checker.c31b_missing_start_time3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_START_TIME3",
        ]

        # There should not be data in the dataframe
        assert c31b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c31b_quality_report.schema.names) == sorted(report_schema)

    def test_c31c_missing_start_time3(
        self,
        spark_session,
        course_data_batch_with_missing_start_time3_1,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_start_time3_1
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c31c_quality_report = course_quality_checker.c31c_missing_start_time3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_START_TIME3",
        ]

        # There should not be data in the dataframe
        assert c31c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c31c_quality_report.schema.names) == sorted(report_schema)

    def test_c31d_missing_start_time3(
        self,
        spark_session,
        course_data_batch_with_missing_start_time3_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_start_time3_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c31d_quality_report = course_quality_checker.c31d_missing_start_time3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_START_TIME3",
        ]

        # There should not be data in the dataframe
        assert c31d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c31d_quality_report.schema.names) == sorted(report_schema)

    def test_c32a_invalid_stop_time3(
        self,
        spark_session,
        course_data_batch_with_invalid_stop_time3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_stop_time3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c32a_quality_report = course_quality_checker.c32a_invalid_stop_time3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_SITE_TYPE3",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_STOP_TIME",
            "C_STOP_TIME2",
            "C_STOP_TIME3",
        ]

        # There should not be data in the dataframe
        assert c32a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c32a_quality_report.schema.names) == sorted(report_schema)

    def test_c32b_missing_stop_time3(
        self,
        spark_session,
        course_data_batch_with_missing_stop_time3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_stop_time3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c32b_quality_report = course_quality_checker.c32b_missing_stop_time3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_STOP_TIME3",
        ]

        # There should not be data in the dataframe
        assert c32b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c32b_quality_report.schema.names) == sorted(report_schema)

    def test_c32c_missing_stop_time3(
        self,
        spark_session,
        course_data_batch_with_missing_stop_time3_1,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_stop_time3_1
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c32c_quality_report = course_quality_checker.c32c_missing_stop_time3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_STOP_TIME3",
        ]

        # There should not be data in the dataframe
        assert c32c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c32c_quality_report.schema.names) == sorted(report_schema)

    def test_c32d_missing_stop_time3(
        self,
        spark_session,
        course_data_batch_with_missing_stop_time3_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_stop_time3_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c32d_quality_report = course_quality_checker.c32d_missing_stop_time3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_STOP_TIME3",
        ]

        # There should not be data in the dataframe
        assert c32d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c32d_quality_report.schema.names) == sorted(report_schema)

    def test_c33a_invalid_days3(
        self,
        spark_session,
        course_data_batch_with_invalid_days3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_invalid_days3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c33a_quality_report = course_quality_checker.c33a_invalid_days3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_SITE_TYPE3",
            "C_INSTRUCT_TYPE",
            "C_DAYS",
            "C_DAYS2",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c33a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c33a_quality_report.schema.names) == sorted(report_schema)

    def test_c33b_missing_days3(
        self,
        spark_session,
        course_data_batch_with_missing_days3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_data_batch_with_missing_days3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c33b_quality_report = course_quality_checker.c33b_missing_days3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c33b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c33b_quality_report.schema.names) == sorted(report_schema)

    def test_c33c_invalid_days3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_days3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_days3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c33c_quality_report = course_quality_checker.c33c_invalid_days3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_DAYS2",
        ]

        # There should not be data in the dataframe
        assert c33c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c33c_quality_report.schema.names) == sorted(report_schema)

    def test_c33d_invalid_days3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_days3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_days3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c33d_quality_report = course_quality_checker.c33d_invalid_days3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c33d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c33d_quality_report.schema.names) == sorted(report_schema)

    def test_c34_same_blgd_name_number(
        self,
        spark_session,
        course_load_data_batch_with_same_blgd_name_and_number,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_same_blgd_name_and_number
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c34_quality_report = course_quality_checker.c34_same_blgd_name_number()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
        ]

        # There should not be data in the dataframe
        assert c34_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c34_quality_report.schema.names) == sorted(report_schema)

    def test_c34a_invalid_bldg_sname3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_sname3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_sname3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c34a_quality_report = course_quality_checker.c34a_invalid_bldg_sname3()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_SNAME3",
        ]

        # There should not be data in the dataframe
        assert c34a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c34a_quality_report.schema.names) == sorted(report_schema)

    def test_c34b_invalid_bldg_sname3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_sname3_1,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_sname3_1
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c34b_quality_report = course_quality_checker.c34b_invalid_bldg_sname3()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_SNAME3",
        ]

        # There should not be data in the dataframe
        assert c34b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c34b_quality_report.schema.names) == sorted(report_schema)

    def test_c34c_invalid_bldg_sname3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_sname3_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_sname3_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c34c_quality_report = course_quality_checker.c34c_invalid_bldg_sname3()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_SNAME3",
        ]

        # There should not be data in the dataframe
        assert c34c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c34c_quality_report.schema.names) == sorted(report_schema)

    def test_c35a_invalid_bldg_snum3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_snum3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_snum3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c35a_quality_report = course_quality_checker.c35a_invalid_bldg_snum3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_NUM3",
        ]

        # There should not be data in the dataframe
        assert c35a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c35a_quality_report.schema.names) == sorted(report_schema)

    def test_c35b_invalid_bldg_snum3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_snum3_1,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_snum3_1
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c35b_quality_report = course_quality_checker.c35b_invalid_bldg_snum3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_NUM3",
        ]

        # There should not be data in the dataframe
        assert c35b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c35b_quality_report.schema.names) == sorted(report_schema)

    def test_c35c_invalid_bldg_num3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_snum3_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_snum3_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c35c_quality_report = course_quality_checker.c35c_invalid_bldg_num3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_NUM3",
        ]

        # There should not be data in the dataframe
        assert c35c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c35c_quality_report.schema.names) == sorted(report_schema)

    def test_c35d_invalid_r_bldg_num3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_snum3_3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_snum3_3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c35d_quality_report = course_quality_checker.c35d_invalid_r_bldg_num3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_NUM3",
        ]

        # There should not be data in the dataframe
        assert c35d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c35d_quality_report.schema.names) == sorted(report_schema)

    def test_c35e_invalid_bldg_snum3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_snum3_4,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_snum3_4
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c35e_quality_report = course_quality_checker.c35e_invalid_bldg_snum3()

        report_schema = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_NUM3",
        ]

        # There should not be data in the dataframe
        assert c35e_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c35e_quality_report.schema.names) == sorted(report_schema)

    def test_c36a_invalid_bldg_room3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_room3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_room3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c36a_quality_report = course_quality_checker.c36a_invalid_bldg_room3()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_ROOM_NUM3",
        ]

        # There should not be data in the dataframe
        assert c36a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c36a_quality_report.schema.names) == sorted(report_schema)

    def test_c36b_invalid_bldg_room3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_room3_1,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_room3_1
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c36b_quality_report = course_quality_checker.c36b_invalid_bldg_room3()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_ROOM_NUM3",
        ]

        # There should not be data in the dataframe
        assert c36b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c36b_quality_report.schema.names) == sorted(report_schema)

    def test_c36c_invalid_bldg_room3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_bldg_room3_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_bldg_room3_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c36c_quality_report = course_quality_checker.c36c_invalid_bldg_room3()

        report_schema = [
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_ROOM_NUM3",
        ]

        # There should not be data in the dataframe
        assert c36c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c36c_quality_report.schema.names) == sorted(report_schema)

    def test_c37a_invalid_room_max3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_room_max3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        building_prod_data_batch_dataframe_bnumber_null,
        rooms_prod_data_batch_dataframe_bnumber_null,
        course_load_data_batch_with_invalid_room_typec22e,
        valid_perkins2015_prod_data_batch_dataframe_c13c,
        valid_perkinslogan_prod_data_batch_dataframe_c13e,
        valid_ceml_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_room_max3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = building_prod_data_batch_dataframe_bnumber_null
        rooms_prod_df = rooms_prod_data_batch_dataframe_bnumber_null
        courses_load_prod_df = course_load_data_batch_with_invalid_room_typec22e
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe_c13c
        logan_perkins_prod_df = valid_perkinslogan_prod_data_batch_dataframe_c13e
        cc_master_list_prod_df = valid_ceml_prod_data_batch_dataframe
        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]
        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c37a_quality_report = course_quality_checker.c37a_invalid_room_max3()

        report_schema = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BLDG_NUM3",
            "C_CRN",
            "C_ROOM_NUM3",
            "C_ROOM_MAX3",
            "C_ROOM_MAX3_error",
        ]

        # There should not be data in the dataframe
        assert c37a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c37a_quality_report.schema.names) == sorted(report_schema)

    def test_c38_count_courses(
        self,
        spark_session,
        course_load_data_batch_with_invalid_room_type3_4,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_room_type3_4
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c38_quality_report = course_quality_checker.c38_count_courses()

        report_schema = ["C_INST", "C_ROOM_TYPE3", "Description", "Number_of_Courses"]

        # There should not be data in the dataframe
        assert c38_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c38_quality_report.schema.names) == sorted(report_schema)

    def test_c38b_invalid_room_type3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_room_type3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_room_type3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c38b_quality_report = course_quality_checker.c38b_invalid_room_type3()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c38b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c38b_quality_report.schema.names) == sorted(report_schema)

    def test_c38c_invalid_room_type3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_room_type3_1,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_room_type3_1
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c38c_quality_report = course_quality_checker.c38c_invalid_room_type3()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c38c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c38c_quality_report.schema.names) == sorted(report_schema)

    def test_c38d_count_in_space_utilization(
        self,
        spark_session,
        course_load_data_batch_with_invalid_room_type3_2,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_room_type3_2
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c38d_quality_report = course_quality_checker.c38d_count_in_space_utilization()

        report_schema = [
            "C_INST",
            "C_ROOM_TYPE3",
            "Description",
            "Number_of_Courses",
        ]

        # There should not be data in the dataframe
        assert c38d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c38d_quality_report.schema.names) == sorted(report_schema)

    def test_c38e_invalid_room_type3(
        self,
        spark_session,
        course_load_data_batch_with_invalid_room_type3_3,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):
        df = course_load_data_batch_with_invalid_room_type3_3
        studentcourses_df = valid_studentcourses_data_batch_dataframe
        students_df = valid_student_data_batch_dataframe
        buildings_prod_df = valid_building_prod_data_batch_dataframe
        rooms_prod_df = valid_room_prod_data_batch_dataframe
        courses_load_prod_df = valid_course_loads_data_batch_dataframe
        perkins2015_prod_df = valid_perkins2015_prod_data_batch_dataframe
        logan_perkins_prod_df = valid_logan_perkins_prod_data_batch_dataframe
        cc_master_list_prod_df = valid_cc_master_list_prod_data_batch_dataframe

        inst_id = df.select("C_INST").distinct().collect()[0]["C_INST"]

        course_quality_checker = CourseQualityChecker(
            df,
            students_df,
            studentcourses_df,
            buildings_prod_df,
            rooms_prod_df,
            courses_load_prod_df,
            perkins2015_prod_df,
            logan_perkins_prod_df,
            cc_master_list_prod_df,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            f"courses/{inst_id}/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        c38e_quality_report = course_quality_checker.c38e_invalid_room_type3()

        report_schema = [
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ]

        # There should not be data in the dataframe
        assert c38e_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(c38e_quality_report.schema.names) == sorted(report_schema)

    @patch("google.cloud.pubsub_v1.PublisherClient")
    @patch("google.cloud.pubsub_v1.PublisherClient.publish")
    def test_full_quality_check(
        self,
        mock_publisher_client,
        mock_publish,
        spark_session,
        course_data_batch_with_duplicates_dataframe,
        valid_studentcourses_data_batch_dataframe,
        valid_student_data_batch_dataframe,
        valid_building_prod_data_batch_dataframe,
        valid_room_prod_data_batch_dataframe,
        valid_course_loads_data_batch_dataframe,
        valid_perkins2015_prod_data_batch_dataframe,
        valid_logan_perkins_prod_data_batch_dataframe,
        valid_cc_master_list_prod_data_batch_dataframe,
    ):

        df = course_data_batch_with_duplicates_dataframe

        course_quality_checker = CourseQualityChecker(
            df,
            valid_student_data_batch_dataframe,
            valid_studentcourses_data_batch_dataframe,
            valid_building_prod_data_batch_dataframe,
            valid_room_prod_data_batch_dataframe,
            valid_course_loads_data_batch_dataframe,
            valid_perkins2015_prod_data_batch_dataframe,
            valid_logan_perkins_prod_data_batch_dataframe,
            valid_cc_master_list_prod_data_batch_dataframe,
            "test_pk-1",
            "test-project",
            "test-pubsub-topic",
            "courses/1/2024/02/02/1/courses.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        try:

            course_quality_checker.quality_check()
        except Exception:
            # If we are in here, the test is functioning as expected
            # We expect to throw an exception when we fail a quality check
            # Call pass to prevent the test from failing
            pass

        assert len(course_quality_checker.error_dataframes) > 0

        # Assert we call pub/sub publish
        assert mock_publish.call_count > 0
