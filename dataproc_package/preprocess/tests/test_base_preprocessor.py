from pyspark.sql.types import StringType

from dataproc_package.preprocess import BasePreprocessor


class TestBasePreprocessor:
    def test_trim_all_string_columns(
        self, valid_course_data_batch_dataframe_with_whitespaces
    ):
        base_preprocessor = BasePreprocessor(
            valid_course_data_batch_dataframe_with_whitespaces
        )
        df = base_preprocessor.trim_all_string_columns(
            valid_course_data_batch_dataframe_with_whitespaces
        )

        # Assert no leading or trailing whitespaces in any column

        for column in df.columns:
            if df.schema[column].dataType == StringType():
                assert (
                    df.select(column).filter(f"trim({column}) != {column}").count() == 0
                )
