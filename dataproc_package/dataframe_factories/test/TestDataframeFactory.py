from pyspark.sql import functions as F
from pyspark.sql.types import *

from dga_dataproc_package.dataframe_factories.BaseSparkDataframeFactory import (
    BaseSparkDataframeFactory,
)
from dga_dataproc_package.dataframe_factories.test.TestDataframeSchema import (
    building_schema,
)
from dga_dataproc_package.dataframe_factories.mixins.DynamicFileLoaderMixin import (
    DynamicFileLoaderMixin,
)
class TestDataframeFactory(DynamicFileLoaderMixin, BaseSparkDataframeFactory):
    def __init__(self):
        self.schema = test_schema
        super().__init__()

    def set_dataframe(self, gcs_file_path: str):
        super().set_dataframe(gcs_file_path)
        #self.df = self.df.withColumn("B_KEY", F.concat(F.col("B_INST").cast(StringType()), F.col("B_YEAR").cast(StringType()), F.col("B_NUMBER")))
