import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dataproc_package.verify.BaseQualityChecker import BaseQualityChecker


class TestQualityChecker(BaseQualityChecker):
    def __init__(
        self,
        test_df: DataFrame,
        #rooms_df: DataFrame,
        #test_pk: str,
        mpi_columns_list3: list,
        di_columns_list3: list,
        *args,
        **kwargs,
    ):
        super().__init__(test_df, mpi_columns_list3, di_columns_list3, *args, **kwargs)
        self.test_df = self.df
        self.mpi_column = self.mpi_columns_list3
        self.di_column = self.di_columns_list3
        #self.rooms_df = rooms_df
        #self.test_pk = test_pk

    def test_mpi(self) -> DataFrame:
        """
        test MPI schema application to DF

        """
        #error_code = "sc01a"

        #distinct_inst_code_list = self.get_distinct_inst_code_list()
        mpi_column = self.mpi_column
        error_df = self.df.select(
            mpi_column
        )

        #self.push_error_dataframe_if_errors_found(error_code, error_df)
        print(error_df)
        return error_df

    def quality_check(self):
        """
        Performs all quality checks and publishes to pubsub with error payload if found
        """

        # Quality checks will populate the self.error_dataframes list if errors are found
        self.test_mpi()
        
        # Roll up to the base class, which will publish the errors to pubsub if self.error_dataframes is populated
        super().quality_check()
