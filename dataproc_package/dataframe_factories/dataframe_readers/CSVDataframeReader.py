from dataproc_package.dataframe_factories.dataframe_readers.BaseDataframeReader import BaseDataframeReader
import csv


class CSVDataframeReader(BaseDataframeReader):
    def __init__(self, *args, **kwargs):
        self.default_file_format = "csv"
        self.has_header = "false"
        super().__init__(*args, **kwargs)

    def set_separator(self, gcs_file_path: str):
        # Get the blob (opened as text with detected encoding)
        blob = self.get_gcs_blob(gcs_file_path)

        with blob as file:
            lines = file.readlines()

            # Take a sample of the data for delimiter detection
            sample_data = "\n".join(lines[:min(100, len(lines))])

            # Use csv.Sniffer to detect the delimiter
            sniffer = csv.Sniffer()

            # Fallback: manual counting of likely delimiters
            candidates = [',', ';', '\t', '|']
            separator_counts = {sep: sample_data.count(sep) for sep in candidates}
            print(f'Separator counts: {separator_counts}')
            most_common_separator = max(separator_counts, key=separator_counts.get)

            # Use sniffer to detect the delimiter
            try:
                detected_dialect = sniffer.sniff(sample_data)
                self.separator = detected_dialect.delimiter
                print(f'Detected separator: {self.separator}')
            except csv.Error:
                print("Sniffer failed, using most common separator.")
                self.separator = most_common_separator

            # If there's a mismatch between sniffer and manual counting, use the most common separator
            if most_common_separator != self.separator:
                print("Detected separator does not match most common separator. Using most common separator.")
                self.separator = most_common_separator

            print(f'Chosen separator: {self.separator}')

    def set_has_header(self, gcs_file_path: str):
        blob = self.get_gcs_blob(gcs_file_path)

        with blob as file:
            lines = file.readlines()
            sample_data = "\n".join([line for line in lines[:min(100, len(lines))]])

            # Use csv.Sniffer to detect if there is a header
            sniffer = csv.Sniffer()
            self.has_header = sniffer.has_header(sample_data)
            print(f'Detected header: {self.has_header}')

    def read_dataframe_from_file(self, file_path: str):
        self.set_separator(file_path)
        self.set_has_header(file_path)
        spark_options = {
            "delimiter": self.separator,
            "header": self.has_header
        }

        if not self.schema:
            raise Exception("A schema must be set before reading a dataframe.")

        print(f'Reading file from {file_path} with separator {self.separator}')
        return self.spark.read.format(self.default_file_format).options(**spark_options).load(file_path, schema=self.schema, sep=self.separator)