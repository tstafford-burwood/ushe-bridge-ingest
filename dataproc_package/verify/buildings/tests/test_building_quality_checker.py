import os
from unittest.mock import patch

from dga_dataproc_package.verify.buildings.BuildingQualityChecker import (
    BuildingQualityChecker,
)


class TestBuildingQualityChecker:

    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = "/packages/dga_dataproc_package/tests/fixture_files/fake_service_account.json"

    def test_b06b_duplicate_records_finds_duplicates(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_duplicates_dataframe,
    ):
        df = building_data_batch_with_duplicates_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b06b_quality_report = building_quality_checker.b06b_duplicate_records()

        # There should be data in the dataframe
        report_schema = ["B_INST", "B_NUMBER", "count"]

        assert b06b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b06b_quality_report.schema.names) == sorted(report_schema)

    def test_b06a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_blanks_dataframe,
    ):
        df = building_data_batch_with_blanks_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b06a_quality_report = building_quality_checker.b06a_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NAME",
            "B_NUMBER",
        ]

        assert b06a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b06a_quality_report.schema.names) == sorted(report_schema)

    def test_b02a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_blanks_dataframe,
    ):
        df = building_data_batch_with_blanks_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b02a_quality_report = building_quality_checker.b02a_blank_records()

        # There should be data in the dataframe
        report_schema = ["B_INST", "B_LOCATION", "B_NUMBER", "B_YEAR"]

        assert b02a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b02a_quality_report.schema.names) == sorted(report_schema)

    def test_b03a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_blanks_dataframe,
    ):
        df = building_data_batch_with_blanks_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b03a_quality_report = building_quality_checker.b03a_blank_records()

        # There should be data in the dataframe
        report_schema = ["B_INST", "B_OWNERSHIP", "B_NUMBER", "B_YEAR"]

        assert b03a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b03a_quality_report.schema.names) == sorted(report_schema)

    def test_b04a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_blanks_dataframe,
    ):
        df = building_data_batch_with_blanks_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b04a_quality_report = building_quality_checker.b04a_blank_records()

        # There should be data in the dataframe
        report_schema = ["B_INST", "B_LOCATION", "B_NUMBER", "B_YEAR"]

        assert b04a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b04a_quality_report.schema.names) == sorted(report_schema)

    def test_b05a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_blanks_dataframe,
    ):
        df = building_data_batch_with_blanks_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b05a_quality_report = building_quality_checker.b05a_blank_records()

        # There should be data in the dataframe
        report_schema = ["B_INST", "B_LOCATION", "B_NUMBER", "B_NAME"]

        assert b05a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b05a_quality_report.schema.names) == sorted(report_schema)

    def test_b07a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_blanks_dataframe,
    ):
        df = building_data_batch_with_blanks_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b07a_quality_report = building_quality_checker.b07a_blank_records()

        # There should be data in the dataframe
        report_schema = ["B_INST", "B_LOCATION", "B_NUMBER", "B_NAME", "B_SNAME"]

        assert b07a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b07a_quality_report.schema.names) == sorted(report_schema)

    def test_b10a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_b10a_dataframe,
    ):
        df2 = building_data_batch_b10a_dataframe

        building_quality_checker = BuildingQualityChecker(
            df2,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b10a_quality_report = building_quality_checker.b10a_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_YEAR_CONS",
            "B_AUX",
            "B_REPLACE_COST",
        ]

        assert b10a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b10a_quality_report.schema.names) == sorted(report_schema)

    def test_b01a_missing_institution_code(
        self,
        spark_session,
        valid_room_data_batch_dataframe,
        valid_building_data_batch_dataframe,
    ):
        df = valid_building_data_batch_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        b01a_quality_report = building_quality_checker.b01a_missing_institution_code()

        report_schema = ["B_INST", "B_LOCATION", "B_NUMBER", "B_YEAR"]

        # There should be data in the dataframe
        assert b01a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b01a_quality_report.schema.names) == sorted(report_schema)

    def test_b02b_missing_b_location(
        self,
        spark_session,
        valid_room_data_batch_dataframe,
        valid_building_data_batch_dataframe,
    ):
        df = valid_building_data_batch_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        b02b_quality_report = building_quality_checker.b02b_missing_b_location()

        report_schema = ["B_INST", "B_LOCATION", "B_NUMBER", "B_YEAR"]

        # There should be data in the dataframe
        assert b02b_quality_report.count() > 0

        # Assert that the report dataframe contains the rows with null institution codes
        # assert (
        #     b02b_quality_report.filter(b02b_quality_report.B_LOCATION.isNull()).count()
        #     == building_data_batch_with_null_location.count()
        # )

        # The dataframe should have the correct schema
        assert sorted(b02b_quality_report.schema.names) == sorted(report_schema)

    def test_b03b_missing_b_ownership(
        self,
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_null_ownership,
    ):
        df = building_data_batch_with_null_ownership

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        b03b_quality_report = building_quality_checker.b03b_missing_b_ownership()

        report_schema = ["B_INST", "B_OWNERSHIP", "B_NUMBER", "B_YEAR"]

        # There should be data in the dataframe
        assert b03b_quality_report.count() > 0

        # Assert that the report dataframe contains the rows with null institution codes
        # assert (
        #     b03b_quality_report.filter(b03b_quality_report.B_OWNERSHIP.isNull()).count()
        #     == valid_building_data_batch_dataframe.count()
        # )

        # The dataframe should have the correct schema
        assert sorted(b03b_quality_report.schema.names) == sorted(report_schema)

    def test_b03d_lease_by_location(
        spark_session,
        valid_room_data_batch_dataframe,
        valid_building_data_batch_dataframe,
    ):
        df = valid_building_data_batch_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b03d_quality_report = building_quality_checker.b03d_lease_by_location()

        # There should be data in the dataframe
        report_schema = ["B_INST", "B_OWNERSHIP", "B_LOCATION", "B_NUMBER"]

        assert b03d_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b03d_quality_report.schema.names) == sorted(report_schema)

    def test_b08a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_replace_cost_dataframe,
    ):
        df = building_data_batch_with_replace_cost_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b08a_quality_report = building_quality_checker.b08a_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_YEAR_CONS",
        ]

        assert b08a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b08a_quality_report.schema.names) == sorted(report_schema)

    def test_b08b_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_replace_cost2_dataframe,
    ):
        df = building_data_batch_with_replace_cost2_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b08b_quality_report = building_quality_checker.b08b_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_YEAR_CONS",
        ]

        assert b08b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b08b_quality_report.schema.names) == sorted(report_schema)

    def test_b11a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_replace_cost_dataframe,
    ):
        df = building_data_batch_with_replace_cost_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b11a_quality_report = building_quality_checker.b11a_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_CONDITION",
        ]

        assert b11a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b11a_quality_report.schema.names) == sorted(report_schema)

    def test_b12a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_replace_cost_dataframe,
    ):
        df = building_data_batch_with_replace_cost_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b12a_quality_report = building_quality_checker.b12a_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
            "B_GROSS",
        ]

        assert b12a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b12a_quality_report.schema.names) == sorted(report_schema)

    def test_b12b_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_gross_dataframe,
    ):
        df = building_data_batch_with_gross_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b12b_quality_report = building_quality_checker.b12b_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
            "B_GROSS",
        ]

        assert b12b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b12b_quality_report.schema.names) == sorted(report_schema)

    def test_b14a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_gross_dataframe,
    ):
        df = building_data_batch_with_gross_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b14a_quality_report = building_quality_checker.b14a_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_RSKNBR",
        ]

        assert b14a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b14a_quality_report.schema.names) == sorted(report_schema)

    def test_b14b_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_replace_cost2_dataframe,
    ):
        df = building_data_batch_with_replace_cost2_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b14b_quality_report = building_quality_checker.b14b_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_RSKNBR",
        ]

        assert b14b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b14b_quality_report.schema.names) == sorted(report_schema)

    def test_b15a_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_blanks_dataframe,
    ):
        df = building_data_batch_with_blanks_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b15a_quality_report = building_quality_checker.b15a_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
        ]

        assert b15a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b15a_quality_report.schema.names) == sorted(report_schema)

    def test_b15b_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_aux_dataframe,
    ):
        df = building_data_batch_with_aux_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b15b_quality_report = building_quality_checker.b15b_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
        ]

        assert b15b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b15b_quality_report.schema.names) == sorted(report_schema)

    def test_b15c_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_auxa_dataframe,
    ):
        df = building_data_batch_with_auxa_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b15c_quality_report = building_quality_checker.b15c_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
        ]

        assert b15c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b15c_quality_report.schema.names) == sorted(report_schema)

    def test_b99a_duplicate_records_finds_duplicates(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_duplicates_dataframe,
    ):
        df = building_data_batch_with_duplicates_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b99a_quality_report = building_quality_checker.b99a_duplicate_records()

        # There should be data in the dataframe
        report_schema = ["B_INST", "B_YEAR", "B_NUMBER", "B_KEY", "count"]

        assert b99a_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b99a_quality_report.schema.names) == sorted(report_schema)

    def test_b11b_missing_b_condition(
        self,
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_condition_dataframe,
    ):
        df = building_data_batch_with_condition_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        b11b_quality_report = building_quality_checker.b11b_missing_b_condition()

        report_schema = [
            "B_INST",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_CONDITION",
        ]

        # There should be data in the dataframe
        assert b11b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b11b_quality_report.schema.names) == sorted(report_schema)

    def test_b11c_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_replace_cost3_dataframe,
    ):
        df = building_data_batch_with_replace_cost3_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b11c_quality_report = building_quality_checker.b11c_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_CONDITION",
        ]

        assert b11c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b11c_quality_report.schema.names) == sorted(report_schema)

    def test_b04b_blank_records_finds_blanks(
        spark_session,
        valid_room_data_batch_dataframe,
        valid_building_data_batch_dataframe,
    ):
        df = valid_building_data_batch_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b04b_quality_report = building_quality_checker.b04b_blank_records()

        # There should be data in the dataframe
        report_schema = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_YEAR",
        ]

        assert b04b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b04b_quality_report.schema.names) == sorted(report_schema)

    def test_b03c_lease_space(
        spark_session,
        valid_room_data_batch_dataframe,
        valid_building_data_batch_dataframe,
    ):
        df = valid_building_data_batch_dataframe

        building_quality_checker = BuildingQualityChecker(
            df,
            valid_room_data_batch_dataframe,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )
        b03c_quality_report = building_quality_checker.b03c_lease_space()

        # There should be data in the dataframe
        report_schema = ["B_INST", "LEASE_SPACE"]

        assert b03c_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b03c_quality_report.schema.names) == sorted(report_schema)

    def test_b99b_missing_rooms(
        self, spark_session, building_room_join_tuple_with_null_rooms
    ):
        building_df, room_df = building_room_join_tuple_with_null_rooms
        building_quality_checker = BuildingQualityChecker(
            building_df,
            room_df,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        b99b_quality_report = building_quality_checker.b99b_missing_rooms()

        report_schema = ["B_INST", "B_LOCATION", "B_OWNERSHIP", "B_NUMBER"]

        # There should be data in the dataframe
        assert b99b_quality_report.count() > 0

        # The dataframe should have the correct schema
        assert sorted(b99b_quality_report.schema.names) == sorted(report_schema)

    def test_b12c_b_gross(
        self, spark_session, building_room_join_tuple_prorated_no_prorated
    ):
        building_df, room_df = building_room_join_tuple_prorated_no_prorated

        building_quality_checker = BuildingQualityChecker(
            building_df,
            room_df,
            "test-pk-1",
            "test-project",
            "test-pubsub-topic",
            "buildings/1/2024/02/02/1/buildings.txt",
            "test-bucket",
            use_local_filepaths=True,
        )

        b12c_quality_report = building_quality_checker.b12c_b_gross()

        report_schema = [
            "B_INST",
            "B_NUMBER",
            "B_NAME",
            "B_Gross_Area",
            "Sum_Rooms_Area",
            "Sum_Prorated_Area",
            "Sum_NonPro_Area",
        ]

        # There should be data in the dataframe
        assert b12c_quality_report.count() == 1
        assert b12c_quality_report.collect()[0]["B_Gross_Area"] == 1.0
        assert b12c_quality_report.collect()[0]["Sum_Rooms_Area"] == 30.0
        assert b12c_quality_report.collect()[0]["Sum_Prorated_Area"] == 10.0
        assert b12c_quality_report.collect()[0]["Sum_NonPro_Area"] == 20.0

        # The dataframe should have the correct schema
        assert sorted(b12c_quality_report.schema.names) == sorted(report_schema)

    @patch("google.cloud.pubsub_v1.PublisherClient")
    @patch("google.cloud.pubsub_v1.PublisherClient.publish")
    def test_full_quality_check(
        self,
        mock_publisher_client,
        mock_publish,
        spark_session,
        valid_room_data_batch_dataframe,
        building_data_batch_with_duplicates_dataframe,
    ):

        df = building_data_batch_with_duplicates_dataframe

        try:
            building_quality_checker = BuildingQualityChecker(
                df,
                valid_room_data_batch_dataframe,
                "test-pk-1",
                "test-project",
                "test-pubsub-topic",
                "buildings/1/2024/02/02/1/buildings.txt",
                "test-bucket",
                use_local_filepaths=True,
            )
            building_quality_checker.quality_check()
        except Exception:
            # If we are in here, the test is functioning as expected
            # We expect to throw an exception when we fail a quality check
            # Call pass to prevent the test from failing
            pass

        assert len(building_quality_checker.error_dataframes) > 0

        # Assert we call pub/sub publish
        assert mock_publish.call_count > 0
