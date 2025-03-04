from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

class BaseSparkDataframeFactory:
    def __init__(self):
        self.spark = self.get_spark_session()
        super().__init__()

    def get_spark_session(self):
        return SparkSession.builder.getOrCreate()
    

    def set_dataframe(self, df):
        """
        Implement this method in your derived class to set the DataFrame attribute.
        """
        self.df = df

    def get_dataframe(self) -> DataFrame:
        return self.df
