from google.cloud import storage
import magic


class BaseDataframeReader:
    def __init__(self, spark, schema):
        self.spark = spark
        self.schema = schema
        self.encoding = None

    def set_encoding(self, blob_content):
        # Determine encoding of file content
        mime = magic.Magic(mime_encoding=True)
        detected_encoding = mime.from_buffer(blob_content)
        print(f"Detected encoding: {detected_encoding}")

        # Handle known encodings and fallback for unknown
        if detected_encoding in ["unknown-8bit", "binary", ""]:
            print("Detected unknown encoding. Falling back to ISO-8859-1.")
            self.encoding = 'ISO-8859-1'  # Safe fallback
        else:
            self.encoding = detected_encoding


    def get_gcs_blob(self, gcs_file_path: str):
        # Account for local filepaths in unit tests.
        # Check if file path is formatted as a GCS path or not.
        if "gs://" not in str(gcs_file_path):
            # Local file case
            with open(gcs_file_path, "rb") as file:
                content = file.read()
                self.set_encoding(content)  # Detect encoding based on content
                return open(gcs_file_path, "r", encoding=self.encoding)

        # GCS file handling
        storage_client = storage.Client()
        bucket_name = gcs_file_path.split("/")[2]
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("/".join(gcs_file_path.split("/")[3:]))

        # Read the blob content as binary (bytes) first
        content = blob.download_as_bytes()

        # Detect the encoding of the binary content
        self.set_encoding(content)

        # Return the blob opened in the correct encoding
        return blob.open("r", encoding=self.encoding)
