

# ASU Dataproc
Repository containing classes and modules for ASU Dataproc jobs. The code in this repository is used to perform movements in the ASU Data Lake, pre-processing, schema validation, QA/QC validation, and ineractions with GCP services (e.g, Pub/Sub)

# High Level Backend Technical Architecture
The backend data pipeline processes are triggered by different interactions with the ASU application. For Type 1 data and other data types that require QA/QC validations and checks, the initial raw file upload will trigger the Dataproc job responsible for conducting the QA/QC checks. This automation occurs when the application sends a PubSub message to a topic that triggers Cloud Functions to parse the PubSub payload and trigger the Workflows orchestration. Based on the parsed payload sent from Cloud Functions to Workflows, the Dataproc job associated with the file(s) dropped will be triggered. QA/QC responses are relayed back to the application thru PubSub messaging. The error responses from QA/QC checks are displayed on the application for an admin to examine, add notes, approve/deny potentially acceptable error outputs etc. Once it is decided that the raw data upload is acceptable, the application sends a PubSub message to GCP that kicks off the sink pipelines. This is a similar process to the QA/QC pipelines in that the application triggers Cloud Functions via PubSub which in turn triggers the Workflows orchestration layer responsible for spinning up the appropriate Dataproc job for a given data file. The difference here is that instead of communicating query responses to the application, this pipeline ends successfully by taking the raw data file from the ingest bucket, converting it to a parquet file, uploading it to the staging cloud storage bucket and uploading the data to a BigQuery table.

For data types and files that do not require QA/QC checks, the file upload on the application will trigger the sink pipeline right away to add the uploaded file to BigQuery for further analysis and data manipulation.

# Codebase Overview

Starting from the top, the cloud_build directory holds the yaml files responsible for the cloud build configurations responsible for actions such as merging code pushes, adding entrypoint files to GCS and building the containers based on your dockerfile configuration.

The asu_apgap_dataproc_package directory holds most of the code related to the data pipeline processes. Outside of the directories that are detailed below this also contains the dockerfiles, scripts for local testing, yaml file responsible for running the pre-commit hooks, and other general configuration reference files.

The dataframe_factories directory holds one directory per data file submission type and is responsible for taking the raw file ingest from cloud storage and creating a dataframe to be used downstream for the QA/QC queries. The schema definitions are found here as well and are checked/applied to the dataframes. This directory is also responsible for the value generators (where applicable) to add additional columns and enhance data.

The preprocess directory is responsible for the necessary preprocessing steps such as the removal of whitespaces. once this step is complete the resulting dataframes are ready for quality checks and the eventual sink to cloud storage and BigQuery

The utils directory holds a handful of helper python scripts that are used for things like reading GCS files, reading yaml config files and publishing payloads to PubSub topics.

The verify directory is where all of the QA/QC checks occur. For any files that require QA/QC checks, there is a directory specific to that file that has all of the queries written in PySpark, the associated unit tests to run locally to test accuracy of those queries and a conftest.py file responsible for defining the fixture data to run the queries against for testing. An important note for developers, the BaseQualityChecker.py has reusable functions related to repeated queries, table lookups etc that are referenced/called in the individual quality checker python scripts.

The entrypoints directory holds all of the python scripts that are run and triggered by the Workflows automation. These scripts sets environment variables coming from the Workflows trigger event to be used throughout the codebase during a particular Dataproc run. For files that require QA/QC processing, there will be on entrypoint script responbsible for the QA/QC Dataproc executions and a second entrypoint script responsible for writing the data to the sink locations once the QA/QC process is approved by an admin within the ASU application. For other data types and files that do not require QA/QC processing, there is 1 entrypoint script responsible for the data movements from ingest to the sink locations.
