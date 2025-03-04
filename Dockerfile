# Inherit from artifact registry image
ARG BASE_IMAGE=us-central1-docker.pkg.dev/br-sbx-app-0/ushe-dataproc-test/dataproc-base:latest
FROM ${BASE_IMAGE}

# Copy dga dataproc package to conda execution directory
COPY ./dataproc_package /packages/dataproc_package

USER root

RUN ${CONDA_HOME}/bin/conda build recipe /packages/dataproc_package/conda.recipe

RUN ${CONDA_HOME}/bin/conda install --use-local dataproc_package

RUN ${CONDA_HOME}/bin/conda install \
    python-magic 


ENV PYTHONPATH="${PYTHONPATH}:/packages/"


USER spark
