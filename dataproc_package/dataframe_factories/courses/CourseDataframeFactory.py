from pyspark.sql import functions as F
from pyspark.sql.types import *

from dga_dataproc_package.dataframe_factories.BaseSparkDataframeFactory import (
    BaseSparkDataframeFactory,
)
from dga_dataproc_package.dataframe_factories.courses.CourseDataframeSchema import (
    course_schema,
)
from dga_dataproc_package.dataframe_factories.mixins.DynamicFileLoaderMixin import (
    DynamicFileLoaderMixin,
)


class CourseDataframeFactory(DynamicFileLoaderMixin, BaseSparkDataframeFactory):
    def __init__(self):
        self.schema = course_schema
        super().__init__()

    def set_dataframe(self, gcs_file_path: str):
        super().set_dataframe(gcs_file_path)
        
        self.df = self.df.withColumn("C_GEN_ED", F.when((F. col("C_GEN_ED") == "H"), "HU").otherwise(F.col("C_GEN_ED")))
        self.df = self.df.withColumn("C_CRS_SBJ", F.when((F.col("C_CRS_SBJ") == ""), F.lit(None)).otherwise(F.col("C_CRS_SBJ")))
        self.df = self.df.withColumn("C_CRS_NUM", F.when((F.col("C_CRS_NUM") == ""), F.lit(None)).otherwise(F.col("C_CRS_NUM")))
        self.df = self.df.withColumn("C_CRS_SEC", F.when((F.col("C_CRS_SEC") == ""), F.lit(None)).otherwise(F.col("C_CRS_SEC")))
        self.df = self.df.withColumn("C_LINE_ITEM", F.when((F.col("C_LINE_ITEM") == ""), F.lit(None)).otherwise(F.col("C_LINE_ITEM")))
        self.df = self.df.withColumn("C_SITE_TYPE", F.when((F.col("C_SITE_TYPE") == ""), F.lit(None)).otherwise(F.col("C_SITE_TYPE")))
        self.df = self.df.withColumn("C_SITE_TYPE2", F.when((F.col("C_SITE_TYPE2") == ""), F.lit(None)).otherwise(F.col("C_SITE_TYPE2")))
        self.df = self.df.withColumn("C_SITE_TYPE3", F.when((F.col("C_SITE_TYPE3") == ""), F.lit(None)).otherwise(F.col("C_SITE_TYPE3")))
        self.df = self.df.withColumn("C_BUDGET_CODE", F.when((F.col("C_BUDGET_CODE") == ""), F.lit(None)).otherwise(F.col("C_BUDGET_CODE")))
        self.df = self.df.withColumn("C_DELIVERY_METHOD", F.when((F.col("C_DELIVERY_METHOD") == ""), F.lit(None)).otherwise(F.col("C_DELIVERY_METHOD")))
        self.df = self.df.withColumn("C_PROGRAM_TYPE", F.when((F.col("C_PROGRAM_TYPE") == ""), F.lit(None)).otherwise(F.col("C_PROGRAM_TYPE")))
        self.df = self.df.withColumn("C_CREDIT_IND", F.when((F.col("C_CREDIT_IND") == ""), F.lit(None)).otherwise(F.col("C_CREDIT_IND")))
        self.df = self.df.withColumn("C_START_TIME", F.when((F.col("C_START_TIME") == ""), F.lit(None)).otherwise(F.col("C_START_TIME")))
        self.df = self.df.withColumn("C_STOP_TIME", F.when((F.col("C_STOP_TIME") == ""), F.lit(None)).otherwise(F.col("C_STOP_TIME")))
        self.df = self.df.withColumn("C_DAYS", F.when((F.col("C_DAYS") == ""), F.lit(None)).otherwise(F.col("C_DAYS")))
        self.df = self.df.withColumn("C_BLDG_SNAME", F.when((F.col("C_BLDG_SNAME") == ""), F.lit(None)).otherwise(F.col("C_BLDG_SNAME")))
        self.df = self.df.withColumn("C_ROOM_NUM", F.when((F.col("C_ROOM_NUM") == ""), F.lit(None)).otherwise(F.col("C_ROOM_NUM")))
        self.df = self.df.withColumn("C_START_TIME2", F.when((F.col("C_START_TIME2") == ""), F.lit(None)).otherwise(F.col("C_START_TIME2")))
        self.df = self.df.withColumn("C_STOP_TIME2", F.when((F.col("C_STOP_TIME2") == ""), F.lit(None)).otherwise(F.col("C_STOP_TIME2")))
        self.df = self.df.withColumn("C_DAYS2", F.when((F.col("C_DAYS2") == ""), F.lit(None)).otherwise(F.col("C_DAYS2")))
        self.df = self.df.withColumn("C_BLDG_SNAME2", F.when((F.col("C_BLDG_SNAME2") == ""), F.lit(None)).otherwise(F.col("C_BLDG_SNAME2")))
        self.df = self.df.withColumn("C_ROOM_NUM2", F.when((F.col("C_ROOM_NUM2") == ""), F.lit(None)).otherwise(F.col("C_ROOM_NUM2")))
        self.df = self.df.withColumn("C_START_TIME3", F.when((F.col("C_START_TIME3") == ""), F.lit(None)).otherwise(F.col("C_START_TIME3")))
        self.df = self.df.withColumn("C_STOP_TIME3", F.when((F.col("C_STOP_TIME3") == ""), F.lit(None)).otherwise(F.col("C_STOP_TIME3")))
        self.df = self.df.withColumn("C_DAYS3", F.when((F.col("C_DAYS3") == ""), F.lit(None)).otherwise(F.col("C_DAYS3")))
        self.df = self.df.withColumn("C_BLDG_SNAME3", F.when((F.col("C_BLDG_SNAME3") == ""), F.lit(None)).otherwise(F.col("C_BLDG_SNAME3")))
        self.df = self.df.withColumn("C_ROOM_NUM3", F.when((F.col("C_ROOM_NUM3") == ""), F.lit(None)).otherwise(F.col("C_ROOM_NUM3")))
        self.df = self.df.withColumn("C_START_DATE", F.when((F.col("C_START_DATE") == ""), F.lit(None)).otherwise(F.col("C_START_DATE")))
        self.df = self.df.withColumn("C_END_DATE", F.when((F.col("C_END_DATE") == ""), F.lit(None)).otherwise(F.col("C_END_DATE")))
        self.df = self.df.withColumn("C_TITLE", F.when((F.col("C_TITLE") == ""), F.lit(None)).otherwise(F.col("C_TITLE")))
        self.df = self.df.withColumn("C_INSTRUCT_ID", F.when((F.col("C_INSTRUCT_ID") == ""), F.lit(None)).otherwise(F.col("C_INSTRUCT_ID")))
        self.df = self.df.withColumn("C_INSTRUCT_NAME", F.when((F.col("C_INSTRUCT_NAME") == ""), F.lit(None)).otherwise(F.col("C_INSTRUCT_NAME")))
        self.df = self.df.withColumn("C_INSTRUCT_TYPE", F.when((F.col("C_INSTRUCT_TYPE") == ""), F.lit(None)).otherwise(F.col("C_INSTRUCT_TYPE")))
        self.df = self.df.withColumn("C_COLLEGE", F.when((F.col("C_COLLEGE") == ""), F.lit(None)).otherwise(F.col("C_COLLEGE")))
        self.df = self.df.withColumn("C_DEPT", F.when((F.col("C_DEPT") == ""), F.lit(None)).otherwise(F.col("C_DEPT")))
        self.df = self.df.withColumn("C_GEN_ED", F.when((F.col("C_GEN_ED") == ""), F.lit(None)).otherwise(F.col("C_GEN_ED")))
        #self.df = self.df.withColumn("C_CRS_EQUIV", F.when((F.col("C_CRS_EQUIV") == ""), F.lit(None)).otherwise(F.col("C_CRS_EQUIV")))
        self.df = self.df.withColumn("C_DELIVERY_MODEL", F.when((F.col("C_DELIVERY_MODEL") == ""), F.lit(None)).otherwise(F.col("C_DELIVERY_MODEL")))
        self.df = self.df.withColumn("C_LEVEL", F.when((F.col("C_LEVEL") == ""), F.lit(None)).otherwise(F.col("C_LEVEL")))
        self.df = self.df.withColumn("C_DEST_SITE", F.when((F.col("C_DEST_SITE") == ""), F.lit(None)).otherwise(F.col("C_DEST_SITE")))
        self.df = self.df.withColumn("C_BLDG_NUM", F.when((F.col("C_BLDG_NUM") == ""), F.lit(None)).otherwise(F.col("C_BLDG_NUM")))
        self.df = self.df.withColumn("C_ROOM_MAX", F.when((F.col("C_ROOM_MAX") == ""), F.lit(None)).otherwise(F.col("C_ROOM_MAX")))
        self.df = self.df.withColumn("C_ROOM_TYPE", F.when((F.col("C_ROOM_TYPE") == ""), F.lit(None)).otherwise(F.col("C_ROOM_TYPE")))
        self.df = self.df.withColumn("C_BLDG_NUM2", F.when((F.col("C_BLDG_NUM2") == ""), F.lit(None)).otherwise(F.col("C_BLDG_NUM2")))
        self.df = self.df.withColumn("C_ROOM_MAX2", F.when((F.col("C_ROOM_MAX2") == ""), F.lit(None)).otherwise(F.col("C_ROOM_MAX2")))
        self.df = self.df.withColumn("C_ROOM_TYPE2", F.when((F.col("C_ROOM_TYPE2") == ""), F.lit(None)).otherwise(F.col("C_ROOM_TYPE2")))
        self.df = self.df.withColumn("C_BLDG_NUM3", F.when((F.col("C_BLDG_NUM3") == ""), F.lit(None)).otherwise(F.col("C_BLDG_NUM3")))
        self.df = self.df.withColumn("C_ROOM_MAX3", F.when((F.col("C_ROOM_MAX3") == ""), F.lit(None)).otherwise(F.col("C_ROOM_MAX3")))
        self.df = self.df.withColumn("C_ROOM_TYPE3", F.when((F.col("C_ROOM_TYPE3") == ""), F.lit(None)).otherwise(F.col("C_ROOM_TYPE3")))        
        self.df = self.df.withColumn("c_key", F.concat(F.col("C_INST").cast("string"), F.col("C_YEAR").cast("string"), F.col("C_TERM").cast("string"), F.col("C_EXTRACT"), F.col("C_CRS_SBJ"), F.col("C_CRS_NUM"), F.col("C_CRS_SEC")))
        self.df = self.df.withColumn("c_crs_unique", F.concat(F.col("C_CRS_SBJ"), F.col("C_CRS_NUM").cast("string"), F.col("C_CRS_SEC").cast("string")))
        self.df = self.df.withColumn("c_instance", F.concat(F.col("C_YEAR").cast("string"), F.col("C_TERM").cast("string"), F.col("C_EXTRACT").cast("string")))
