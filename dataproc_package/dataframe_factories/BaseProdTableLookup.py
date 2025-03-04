# import pandas as pd
from google.cloud import bigquery
from pyspark.sql import SparkSession


class BigQuerytoDataFrameFactory:
    def __init__(
        self,
        bigquery_database: str,
        table_name: str,
    ):
        self.spark = self.get_spark_session()
        self.biqquery_database = bigquery_database
        self.table_name = table_name
        self.client = self.get_bq_client()
        # self.set_prod_df()

    def get_spark_session(self):
        return SparkSession.builder.config(
            "spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        ).getOrCreate()

    def get_bq_client(self):
        client = bigquery.Client()
        return client

    def set_prod_df(self):
        # sql = f"""
        #     SELECT *
        #     FROM `dev-0-datalake-aa69.production.{self.table_name}`
        # """

        # df_pandas = self.client.query_and_wait(sql).to_dataframe()
        # df_pandas_string = df_pandas.to_string()
        # pd.DataFrame.iteritems = pd.DataFrame.items
        # df = self.spark.createDataFrame(
        #     df_pandas_string, verifySchema=False, samplingRatio=1000
        # )
        # self.df = df
        spark = self.spark
        bigquery_database = self.biqquery_database
        bucket = "gs://dataproc-temp-us-west3-1010883524770-ekmkksqb"
        spark.conf.set("temporaryGcsBucket", bucket)
        table = "dev-0-datalake-aa69." f"{bigquery_database}.{self.table_name}"
        df1 = spark.read.format("bigquery").option("table", table).load()
        # df1.createOrReplaceView("df1")
        # self.df = spark.sql("SELECT * FROM df1")
        self.df = df1

    def get_prod_df(self):
        return self.df
