from pyspark.sql import functions as F
from pyspark.sql.types import *

from dataproc_package.dataframe_factories.BaseSparkDataframeFactory import (
    BaseSparkDataframeFactory,
)
from dataproc_package.dataframe_factories.test.TestDataframeSchema import (
    test_schema,
)
from dataproc_package.dataframe_factories.mixins.DynamicFileLoaderMixin import (
    DynamicFileLoaderMixin,
)
class TestDataframeFactory(DynamicFileLoaderMixin, BaseSparkDataframeFactory):
    def __init__(self):
        self.schema = test_schema
        super().__init__()

    def set_dataframe(self, gcs_file_path: str, first_name_column: str, last_name_column: str):
        super().set_dataframe(gcs_file_path)
        self.df = (
            self.df.withColumn(f"{first_name_column}",  F.lower(F.trim(F.regexp_replace(F.col(f"{first_name_column}"), '[^a-zA-Z0-9]', ''))))
            .withColumn(f"{last_name_column}",  F.lower(F.trim(F.regexp_replace(F.col(f"{last_name_column}"), '[^a-zA-Z0-9]', ''))))
        )
        #self.df = self.df.withColumn("B_KEY", F.concat(F.col("B_INST").cast(StringType()), F.col("B_YEAR").cast(StringType()), F.col("B_NUMBER")))
