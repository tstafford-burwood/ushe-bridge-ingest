import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dataproc_package.verify.BaseQualityChecker import BaseQualityChecker

from google.cloud import bigquery

class TestQualityChecker(BaseQualityChecker):
    def __init__(
        self,
        test_df: DataFrame,
        #rooms_df: DataFrame,
        #test_pk: str,
        mpi_columns_list3: list,
        di_columns_list3: list,
        mpi_names: list,
        mpi_bq_table_reference: str,
        di_bq_table_reference: str,
        *args,
        **kwargs,
    ):
        super().__init__(test_df, mpi_columns_list3, di_columns_list3, *args, **kwargs)
        self.test_df = self.df
        self.mpi_columns_list3 = mpi_columns_list3
        self.di_columns_list3 = di_columns_list3
        self.mpi_names = mpi_names
        self.mpi_bq_table_reference = mpi_bq_table_reference
        self.di_bq_table_reference = di_bq_table_reference
        
        #self.rooms_df = rooms_df
        #self.test_pk = test_pk

    def test_mpi(self) -> DataFrame:
        """
        test MPI schema application on DF

        """
        mpi_column = self.mpi_columns_list3
        mpi_names = self.mpi_names
        print(mpi_column)
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        error_df = self.df.select(
            mpi_column
        )
        for old_name, new_name in zip(mpi_column, mpi_names):
            error_df = error_df.withColumnRenamed(old_name, new_name)

        #self.push_error_dataframe_if_errors_found(error_code, error_df)
        error_df.show()
        #table = client.get_table(self.bq_table_reference)
        source_df = error_df.toPandas()
        job = client.load_table_from_dataframe(
            source_df,
            self.mpi_bq_table_reference,
            job_config=job_config,
        )
        table = client.get_table(self.bq_table_reference)
        print(table)
        return error_df

    def test_di(self) -> DataFrame:
        """
        test DI schema application on DF

        """
        #error_code = "sc01a"

        #distinct_inst_code_list = self.get_distinct_inst_code_list()
        di_column = self.di_columns_list3
        print(di_column)
        error_df = self.df.select(
            di_column
        )

        #self.push_error_dataframe_if_errors_found(error_code, error_df)
        error_df.show()
        return error_df

    def quality_check(self):
        """
        Performs all quality checks and publishes to pubsub with error payload if found
        """

        # Quality checks will populate the self.error_dataframes list if errors are found
        self.test_mpi()
        self.test_di()
        
        # Roll up to the base class, which will publish the errors to pubsub if self.error_dataframes is populated
        super().quality_check()
