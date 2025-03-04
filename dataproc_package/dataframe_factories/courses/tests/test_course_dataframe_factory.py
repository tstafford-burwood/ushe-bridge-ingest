import pathlib

from dga_dataproc_package.dataframe_factories.courses.CourseDataframeFactory import (
    CourseDataframeFactory,
)


class TestCourseDataframeFactory:
    def test_read_file(spark_session, expected_schema):
        test_input_file_path = pathlib.Path(
            "/packages/dga_dataproc_package/dataframe_factories/courses/tests/fixture_files/courses.txt"
        )

        dataframe_factory = CourseDataframeFactory()
        dataframe_factory.set_dataframe(test_input_file_path)
        df = dataframe_factory.get_dataframe()

        # There should be data in the dataframe
        assert df.count() > 0

        # The dataframe should have the correct schema
        assert df.schema == expected_schema

        # Assert we do not have erroneous nulls
        assert df.filter(df.C_INST.isNull()).count() == 0

