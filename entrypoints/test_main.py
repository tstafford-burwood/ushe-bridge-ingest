import json
import sys

from google.cloud import pubsub_v1

from dataproc_package.dataframe_factories.BaseProdTableLookup import (
    BigQuerytoDataFrameFactory,
)
from dataproc_package.dataframe_factories.test.TestDataframeFactory import (
    GradeUpdateDataframeFactory,
)
from dataproc_package.preprocess.test.TestPreprocessor import (
    GradeUpdatePreprocessor,
)
from dataproc_package.utils.gcs_pubsub_helpers import (
    publish_to_topic_with_dictionary_payload,
)
from dataproc_package.utils.gcs_reader_helpers import (
    read_pk_from_gcs_input_blob_path,
)
from dataproc_package.verify.test.TestQualityChecker import (
    GradeUpdateQualityChecker,
)

if __name__ == "__main__":
    # These should come from workflow args
    workflow_id = sys.argv[0]
    payload_data_json_str = sys.argv[1]
    bucket_id = sys.argv[2]
    project_id = sys.argv[3]
    pubsub_topic_name = sys.argv[4]
    ref_file_bucket_id = sys.argv[5]

    payload_data = json.loads(payload_data_json_str)

    grade_update_inventory_file_path = (
        f"gs://{bucket_id}/{payload_data['grade_updates_file_path']}"
    )

    grade_update_dataframe_factory = GradeUpdateDataframeFactory()
    grade_update_dataframe_factory.set_dataframe(grade_update_inventory_file_path)
    grade_update_dataframe = grade_update_dataframe_factory.get_dataframe()
    grade_update_event_pk = read_pk_from_gcs_input_blob_path(
        grade_update_inventory_file_path
    )

    bq_to_df_factory = BigQuerytoDataFrameFactory("production", "student_courses")
    bq_to_df_factory.set_prod_df()
    student_courses_prod_df = bq_to_df_factory.get_prod_df()
    bq_to_df = BigQuerytoDataFrameFactory("production", "courses")
    bq_to_df.set_prod_df()
    courses_prod_df = bq_to_df.get_prod_df()

    try:
        # schema checks
        # data_schema_checker = CourseSchemaChecker(df_raw)
        # data_schema_checker.check_schema()

        grade_update_data_preprocessor = GradeUpdatePreprocessor(grade_update_dataframe)

        grade_update_df_preprocessed = grade_update_data_preprocessor.preprocess()

        grade_update_quality_checker = GradeUpdateQualityChecker(
            grade_update_df_preprocessed,
            grade_update_event_pk,
            student_courses_prod_df,
            courses_prod_df,
            project_id,
            pubsub_topic_name,
            payload_data["grade_updates_file_path"],
            ref_file_bucket_id,
        )
        grade_update_quality_checker.quality_check()

    except Exception as e:
        print(f"An error occurred while processing the dataframes. {e}")
        raise e

    finally:
        ##########
        # This is where the code to publish a message after the dataproc job ends will go (use helper classes and pk variable value)
        ##########
        publisher_client = pubsub_v1.PublisherClient()

        for pk in [grade_update_event_pk]:
            dict_payload = {
                "pk": pk,
                "status": "qa_qc",
            }

            publish_to_topic_with_dictionary_payload(
                project_id,
                "athena-dev-0-dataproc-status-notification",
                dict_payload,
                publisher_client,
            )

    # sink_writer = BucketSinkWriter(df_raw, workflow_id, "error")
    # sink_writer.write_to_sink()

    # sink_writer = BucketSinkWriter(preprocessed_df, workflow_id, "staging")
