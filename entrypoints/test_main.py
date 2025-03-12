import json
import sys

from google.cloud import pubsub_v1
from google.cloud import storage

from dataproc_package.dataframe_factories.BaseProdTableLookup import (
    BigQuerytoDataFrameFactory,
)
from dataproc_package.dataframe_factories.test.TestDataframeFactory import (
    TestDataframeFactory,
)
# from dataproc_package.preprocess.test.TestPreprocessor import (
#     TestPreprocessor,
# )
# from dataproc_package.utils.gcs_pubsub_helpers import (
#     publish_to_topic_with_dictionary_payload,
# )
# from dataproc_package.utils.gcs_reader_helpers import (
#     read_pk_from_gcs_input_blob_path,
# )
from dataproc_package.verify.test.TestQualityChecker import (
    TestQualityChecker,
)

if __name__ == "__main__":
    # These should come from workflow args
    workflow_id = sys.argv[0]
    payload_data_json_str = sys.argv[1]
    bucket_id = sys.argv[2]
    project_id = sys.argv[3]
    pubsub_topic_name = sys.argv[4]
    ref_file_bucket_id = sys.argv[5]
    file_name = sys.argv[6]

    payload_data = json.loads(payload_data_json_str)

    test_file_path = (
        f"gs://{bucket_id}/{file_name}"
    )

    with blob.open("r") as file:
            input_json = json.load(file)

    mpi_columns = []
    mpi_names = []
    di_columns = []
    all_columns = []
    first_name_column = None

    for column in input_json['columns']:
        all_columns.append(column['name'])
        if "MPI" in column['outputs']:
            mpi_columns.append(column['name'])
            mpi_names.append(column['outputs']['MPI']['name'])
        elif "DI" in column['outputs']:
            di_columns.append(column['name'])

    mpi_columns_list = [s.replace('(', '_') for s in mpi_columns]
    mpi_columns_list2 = [s.replace(')', '_') for s in mpi_columns_list]
    mpi_columns_list3 = [s.replace('/', '_') for s in mpi_columns_list2]
    print(mpi_columns_list3)
    print(mpi_names)

    di_columns_list = [s.replace('(', '_') for s in di_columns]
    di_columns_list2 = [s.replace(')', '_') for s in di_columns_list]
    di_columns_list3 = [s.replace('/', '_') for s in di_columns_list2]
    print(di_columns_list3)

    all_columns_list = [s.replace('(', '_') for s in all_columns]
    all_columns_list2 = [s.replace(')', '_') for s in all_columns_list]
    all_columns_list3 = [s.replace('/', '_') for s in all_columns_list2]
    print(all_columns_list3)

    for column in input_json['columns']:
        mpi = column['outputs'].get('MPI')
        if mpi and mpi.get('name') == 'first_name':
            first_name = column['name']
            first_name_column = [s.replace('(', '_') for s in first_name]
            first_name_column2 = [s.replace(')', '_') for s in first_name_column]
            first_name_column3 = [s.replace('/', '_') for s in first_name_column2]
        elif mpi and mpi.get('name') == 'last_name':
            last_name = column['name']
            last_name_column = [s.replace('(', '_') for s in last_name]
            last_name_column2 = [s.replace(')', '_') for s in last_name_column]
            last_name_column3 = [s.replace('/', '_') for s in last_name_column2]          
    print(first_name_column3)
    print(last_name_column3)

    test_dataframe_factory = TestDataframeFactory()
    test_dataframe_factory.set_dataframe(test_file_path, first_name_column3, last_name_column3)
    test_dataframe = test_dataframe_factory.get_dataframe()
    test_dataframe.show()

    bucket_name = 'ushe_context_files'
    blob_name = 'context.json'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # with open('gs://ushe_context_files/context.json', 'r') as file:
    #         input_json = json.load(file)
    
    # test_event_pk = read_pk_from_gcs_input_blob_path(
    #     test_file_path
    # )

    # bq_to_df_factory = BigQuerytoDataFrameFactory("production", "student_courses")
    # bq_to_df_factory.set_prod_df()
    # student_courses_prod_df = bq_to_df_factory.get_prod_df()
    # bq_to_df = BigQuerytoDataFrameFactory("production", "courses")
    # bq_to_df.set_prod_df()
    # courses_prod_df = bq_to_df.get_prod_df()

    #try:
    #     # schema checks
    #     # data_schema_checker = CourseSchemaChecker(df_raw)
    #     # data_schema_checker.check_schema()

        #test_data_preprocessor = TestPreprocessor(test_dataframe)

        #test_df_preprocessed = test_data_preprocessor.preprocess()

    test_quality_checker = TestQualityChecker(
        test_dataframe,
        #test_event_pk,
        #student_courses_prod_df,
        #courses_prod_df,
        #project_id,
        #pubsub_topic_name,
        #payload_data["test_file_path"],
        #ref_file_bucket_id,
        mpi_columns_list3,
        di_columns_list3,
        mpi_names
        )
    test_quality_checker.quality_check()

    # except Exception as e:
    #     print(f"An error occurred while processing the dataframes. {e}")
    #     raise e

    # finally:
    #     ##########
    #     # This is where the code to publish a message after the dataproc job ends will go (use helper classes and pk variable value)
    #     ##########
    #     publisher_client = pubsub_v1.PublisherClient()

    #     for pk in [test_event_pk]:
    #         dict_payload = {
    #             "pk": pk,
    #             "status": "qa_qc",
    #         }

    #         publish_to_topic_with_dictionary_payload(
    #             project_id,
    #             "athena-dev-0-dataproc-status-notification",
    #             dict_payload,
    #             publisher_client,
    #         )

    # sink_writer = BucketSinkWriter(df_raw, workflow_id, "error")
    # sink_writer.write_to_sink()

    # sink_writer = BucketSinkWriter(preprocessed_df, workflow_id, "staging")
