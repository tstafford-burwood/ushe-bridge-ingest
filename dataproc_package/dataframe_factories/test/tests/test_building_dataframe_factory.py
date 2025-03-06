import pathlib

from dataproc_package.dataframe_factories.test.BuildingDataframeFactory import (
    BuildingDataframeFactory,
)


class TestBuildingDataframeFactory:
    def test_read_file(spark_session, expected_schema):
        test_input_file_path = pathlib.Path(
            "/packages/dataproc_package/dataframe_factories/buildings/tests/fixture_files/buildings.txt"
        )

        dataframe_factory = BuildingDataframeFactory()
        dataframe_factory.set_dataframe(test_input_file_path)
        df = dataframe_factory.get_dataframe()

        # There should be data in the dataframe
        assert df.count() > 0

        # The dataframe should have the correct schema
        assert df.schema == expected_schema
