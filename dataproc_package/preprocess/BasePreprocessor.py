from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim


class BasePreprocessor:
    def __init__(self, df_raw):

        """
        Base class for preprocessing dataframes. Performs preprocessing logic common to all input data.
        """
        self.df_raw = df_raw

    def trim_all_string_columns(self, df: DataFrame) -> DataFrame:
        """

        Trim all whitespaces from string type columns.

        Returns:
            DataFrame: Dataframe with trimed whitespaces.
        """
        return df.select(
            *[
                trim(col(c[0])).alias(c[0]) if c[1] == "string" else col(c[0])
                for c in df.dtypes
            ]
        )

    def preprocess(self):
        """
        Base preprocessor function. Override and call super().preprocess(),
        or implement your own preprocessing logic in a child class.
        """
        try:
            df_preprocessed = self.trim_all_string_columns(self.df_raw)

            return df_preprocessed
        except Exception as e:
            raise Exception(
                f"An error occurred while preprocessing the dataframe. {e}"
            ) from e
