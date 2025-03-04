from dga_dataproc_package.utils import read_yaml_config

class BaseSchemaChecker:
    def __init__(self, df_to_check, schema_gcs_path: str = None):
        self.config = read_yaml_config('config.yaml')
        self.schema_gcs_path = schema_gcs_path if schema_gcs_path else self.config['schema_gcs_path'] # StructType Schema JSON file path
        self.schema_json = self.read_json_from_gcs_bucket(self.schema_gcs_path)
        self.df_to_check = df_to_check 

    def check_schema(self):
        """
            Implement me in child class.
        """
        raise NotImplementedError