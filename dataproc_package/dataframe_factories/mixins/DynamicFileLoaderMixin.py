from dataproc_package.dataframe_factories.dataframe_readers.CSVDataframeReader import CSVDataframeReader
from dataproc_package.dataframe_factories.dataframe_readers.ExcelDataframeReader import ExcelDataframeReader
from dataproc_package.dataframe_factories.dataframe_readers.ParquetDataframeReader import ParquetDataframeReader
import magic

class DynamicFileLoaderMixin:
    """
    Mixin class to read a dataframe from a file with an unknown file type.
    """

    def __init__(self, *args, **kwargs):
        self.file_type_map = {
            "csv": "csv",
            "tsv": "csv",
            "txt": "csv",
            "xls": "xlsx",
            "xlsx": "xlsx",
            "parqeuet": "parquet"
        }
        self.file_type_class_map = {
            "csv": CSVDataframeReader,
            "xlsx": ExcelDataframeReader,
            "parquet": ParquetDataframeReader
        }
        self.file_type_class = None
        super().__init__(*args, **kwargs)

    def determine_file_type(self, file_path: str):
        """
        Determine the file type of the file at the given path.
        First, attempt to determine the file type based on the file contents.
        If that fails, determine the file type based on the file extension.
        Returns the file type in lowercase.

        Args:
            file_path (str): The path to the file.

        Returns:
            str: The file type.
        """
        try:
            return self.determine_file_type_from_contents(file_path)
        except Exception:
            return self.determine_file_type_from_extension(file_path)
    
    def determine_file_type_from_extension(self, file_path: str):
        """
        Determine the file type of the file at the given path based on the file extension.
        Returns the file type in lowercase.
        """
        file_extension = file_path.split(".")[-1]
        return self.file_type_map[file_extension]


    def determine_file_type_from_contents(self, file_path: str):
        """
        Determine the file type of the file at the given path based on the file contents.
        Returns the file type in lowercase.
        """
        mime = magic.Magic(mime=True)
        mime_type = mime.from_file(file_path)
        # if 'csv' in mime_type or mime_type == 'text/plain':
        #     print('File is of type csv')
        #     return self.file_type_map['csv']
        # elif 'excel' in mime_type or mime_type == 'application/vnd.ms-excel':
        #     print('File is of type xls')
        #     return self.file_type_map['xlsx']
        # elif 'spreadsheetml' in mime_type or mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        #     print('File is of type xlsx')
        #     return self.file_type_map['xlsx']
        # elif 'parquet' in mime_type or mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        #     print('File is of type parquet')
            # return self.file_type_map['parquet']
        return self.file_type_map['parquet']
        # else:
        #     raise ValueError(f"Unsupported file type: {mime_type}")

    def set_dataframe_factory_class(self, file_path: str):
        """
        Determine the dataframe factory class to use for the given file path.

        Args:
            file_path (str): The path to the file.

        Returns:
            class: The dataframe factory class.
        """
        #file_type = self.determine_file_type(file_path)
        file_type = "parquet"
        self.file_type_class = self.file_type_class_map[file_type]
        return self.file_type_class_map[file_type]



    def read_dataframe_from_file(self, file_path: str):
        """
        Read a dataframe from a file at the given path.

        Args:
            file_path (str): The path to the file.

        Returns:
            DataFrame: The dataframe.
        """

        # Ensure file_path is a string
        file_path = str(file_path)
        self.set_dataframe_factory_class(file_path)
        file_type_reader = self.file_type_class(self.spark, self.schema)
        return file_type_reader.read_dataframe_from_file(file_path)
    

    def set_dataframe(self, gcs_file_path: str):
        """
        Set the dataframe from the file at the given path.

        Args:
            gcs_file_path (str): The path to the file in GCS.
        """
        self.df = self.read_dataframe_from_file(gcs_file_path)