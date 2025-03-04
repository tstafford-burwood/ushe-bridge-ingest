# def read_json_from_gcs_bucket(bucket_name: str, object_name: str) -> dict:
#     """Reads a json file from a GCS bucket and returns a dictionary.

#     Args:
#         bucket_name (str): Name of the GCS bucket.
#         object_name (str): Name of the object in the GCS bucket.

#     Returns:
#         Dict: Dictionary representation of the json file.
#     """
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(object_name)
#     content = blob.downloadas_string()
#     json_content = json.loads(content)

#     return json_content


def read_pk_from_gcs_input_blob_path(gcs_input_blob_path: str) -> int:
    """
    Read a GCS input file path and return the PK.
    """

    # PK is the folder containing the file
    return int(gcs_input_blob_path.split("/")[-2])


def read_inst_id_from_gcs_input_blob_path(gcs_input_blob_path: str) -> int:
    """
    Read a GCS input file path and return the institution ID.
    """

    return int(gcs_input_blob_path.split("/")[1])


def read_path_from_gcs_input_blob_path(gcs_input_blob_path: str) -> int:
    """
    Read a GCS input file path and return the institution ID.
    """

    return gcs_input_blob_path.split("building_inventorty.txt")[0]
