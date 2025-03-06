import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dga_dataproc_package.verify.BaseQualityChecker import BaseQualityChecker


class TestQualityChecker(BaseQualityChecker):
    def __init__(
        self,
        test_df: DataFrame,
        #rooms_df: DataFrame,
        building_pk: str,
        *args,
        **kwargs,
    ):
        super().__init__(test_df, *args, **kwargs)
        self.test_df = self.df
        #self.rooms_df = rooms_df
        self.test_pk = test_pk

    def b06b_duplicate_records(self) -> DataFrame:
        """
        B-06B Duplicate Records

        Returns error dataframe if duplicates are found

        """
        cols_to_check = ["B_INST", "B_NUMBER"]
        error_code = "b06b"

        return self.check_duplicates(error_code, cols_to_check)

    def quality_check(self):
        """
        Performs all quality checks and publishes to pubsub with error payload if found
        """

        # Quality checks will populate the self.error_dataframes list if errors are found
        self.b06b_duplicate_records()
        
        # Roll up to the base class, which will publish the errors to pubsub if self.error_dataframes is populated
        super().quality_check()
