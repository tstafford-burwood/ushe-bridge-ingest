from dga_dataproc_package.utils import read_yaml_config
from pyspark.sql import SparkSession
from dga_dataproc_package.utils.gcs_reader_helpers import read_json_from_gcs_bucket
from pyspark.sql.types import StructType
from dga_dataproc_package.schema_checkers.BaseSchemaChecker import BaseSchemaChecker

class BuildingSchemaChecker(BaseSchemaChecker):

    def check_schema(self):
        try:
            # Read dataframe with given schema
            expected_schema_dict = read_json_from_gcs_bucket(self.schema_gcs_path)
            expected_schema_struct_type = StructType.fromJson(expected_schema_dict) 
            df_schema = self.df_to_check.schema
            
            assert df_schema == expected_schema_struct_type, 'Schema does not match expected schema.'
            
        except Exception as e:
            raise Exception(f'An error occurred while checking the schema of the dataframe. {e}') from e

            