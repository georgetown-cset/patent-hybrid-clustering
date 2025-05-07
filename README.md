# Patent hybrid clustering

This repository contains code to build and update a clustering of patents using a hybrid-weight method.

# Contents

Contained in this repository are the following:

1. An Airflow pipeline and supporting code to run updates of the patent clustering
2. An [archive](archive/README.md) of code that was used to research, create, and evaluate the original patent clustering.

Code in the [archive](archive/) folder is described in the [archive README](archive/README.md). The other code in the repository is as follows:

###[schemas](schemas/): 
Schemas of all relevant SQL tables for the final clustering. Intermediate tables and those used in the cluster-building process do not have schemas.
###[scripts](scripts/): 
Scripts used for updating the final clustering with new data.
###[sequences](sequences/):
CSVs containing the sequences that SQL queries run during the cluster update process.
###[sql](sql/):
SQL code used in the cluster process. The sql in the main directory is used for the actual updating of the clusters themselves.

[sql/breakdowns](sql/breakdowns): SQL code to create breakdowns of the clusters based on a variety of features and connect the clusters to the research literature.

[sql/checks](sql/checks): SQL code to validate the outputs in resultant tables after the clustering update has run.

###[tests](tests/):
Unit tests for various scripts used in the update process.

There are two primary scripts in the top-level directory, used to build the Airflow pipeline:

[patent_hybrid_clusters_dag.py](patent_hybrid_clusters_dag.py): The script to build the actual Airflow update DAG.

[push_to_airflow.sh](push_to_airflow.sh): The script to push all relevant files into GCS so that Airflow has access to them for running.


# Setup and Running

The repository is designed to work with `pre-commit` in order to ensure all tests, hooks, and linting run smoothly. To set up and ensure smooth development (using a virtual environment), run:

- `pip install -r requirements.txt`
- `pre-commit install`
- `pre-commit run --all-files`

The update pipeline is designed to be used in an Airflow pipeline inside of Google Cloud. It could likely be adapted relatively easily to a different cloud environment like AWS. Similarly, the SQL itself is designed to work in Google's BigQuery environment, and the pipeline depends on a number of other Google Cloud environments.

To update the DAG, run:

`./push_to_airflow.sh`

The DAG runs automatically; currently it is triggered by a different pipeline in our data workflow (which updates earlier-stage patent data).
