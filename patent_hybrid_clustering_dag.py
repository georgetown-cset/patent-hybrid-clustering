import csv
import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from dataloader.airflow_utils.defaults import (
    DAGS_DIR,
    DATA_BUCKET,
    GCP_ZONE,
    PROJECT_ID,
    get_default_args,
    get_post_success,
)
from dataloader.scripts.populate_documentation import update_table_descriptions
from kubernetes.client import models as k8s


def get_clean_lines(f):
    # Filters commented lines from a csv
    for line in f:
        if not line.startswith("#"):
            yield line


args = get_default_args(pocs=["Rebecca, Katherine Quinn"])

with DAG(
    "patent_hybrid_clustering",
    default_args=args,
    description="Runs patent hybrid clustering",
    schedule_interval=None,
) as dag:
    production_dataset = "patent_clusters"
    tmp_dir = f"{production_dataset}/tmp"
    schema_dir = f"{production_dataset}/schemas"
    sql_dir = f"sql/{production_dataset}"
    sequence_dir = f"sequences/{production_dataset}"
    staging_dataset = f"staging_{production_dataset}"
    backups_dataset = f"{production_dataset}_backups"

    # TODO: a list
    # 1. pull *new* patent families that have titles or abstracts [Rebecca]
    # but were not included in the previous clustering
    # 2. Run LID over the new patent families that don't have language ids [Katherine]
    # 3. Get the list of patents that need translation [Katherine]
    # 4. Run translation over the new patent families to prep for embedding [Katherine]
    # 5. run FAISS over the new patent families [Katherine, Rebecca]
    # 6. get citation links for the new patent families [Rebecca]
    # 7. create overall edge weights for new patent families [Rebecca]
    # 8. run "reattachment" waves to add new patent families into clusters and get final map [Katherine]
    # 10. run keyword extraction on updated clusters [Rebecca]
    # 11. run cluster breakdown SQL queries on updated clusters [Rebecca]
    # 12. run SQL tests and transfer to production and backup tables [Katherine, Rebecca]

    # TODO: do we need this? maybe. Figure out
    clear_tmp_dir = GCSDeleteObjectsOperator(
        task_id="clear_tmp_dir", bucket_name=DATA_BUCKET, prefix=tmp_dir
    )

    curr_downstream_query = clear_tmp_dir

    # TODO: write the actual SQL queries that are in this sequence table
    # TODO (Katherine): add queries to find the data we need to run LID on
    # and add those to the sequence table as well

    with open(
        f"{os.environ.get('DAGS_FOLDER')}/{sequence_dir}/initial_data_query_sequence.csv"
    ) as f:
        for line in csv.DictReader(get_clean_lines(f)):
            query = BigQueryInsertJobOperator(
                task_id=line["table_name"],
                configuration={
                    "query": {
                        "query": "{% include '"
                        + f"{sql_dir}/{line['table_name']}.sql"
                        + "' %}",
                        "useLegacySql": False,
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": staging_dataset,
                            "tableId": line["table_name"],
                        },
                        "allowLargeResults": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE",
                    }
                },
            )
            curr_downstream_query >> query
            curr_downstream_query = query

    # TODO (Katherine): transfer data requiring LID to GCS (leaving an example here)

    export_prev_family_categories = BigQueryToGCSOperator(
        task_id="export_prev_family_categories",
        source_project_dataset_table=f"{production_dataset}.family_categories",
        destination_cloud_storage_uris=f"gs://{DATA_BUCKET}/{tmp_dir}/prev_family_categories/data*.jsonl",
        export_format="NEWLINE_DELIMITED_JSON",
        force_rerun=True,
    )

    # TODO (Katherine): Run LID; save output to a BQ table
    # you'll have to modify the actual commands in here to make it run correctly but here's
    # a sample pod operator

    run_lid = GKEStartPodOperator(
        task_id="run-lid",
        name=f"run-lid",
        project_id=PROJECT_ID,
        location=GCP_ZONE,
        cluster_name="cc2-task-pool",
        do_xcom_push=True,
        cmds=["/bin/bash"],
        arguments=[
            "-c",
            (
                setup_commands
                + f" && python3 download_merged_ids.py --gcs_bucket {DATA_BUCKET} "
                f"--gcs_prefix {raw_merged_ids_dir} --s3_prefix data/merged_ids/{dataset} "
                f"--curr_baseset {curr_baseset}"
            ),
        ],
        namespace="default",
        image=f"gcr.io/{PROJECT_ID}/cc2-task-pool",
        get_logs=True,
        startup_timeout_seconds=300,
        on_finish_action="delete_pod",
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": [
                                        "default-pool",
                                    ],
                                }
                            ]
                        }
                    ]
                }
            }
        },
        annotations={"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
    )

    # TODO (Katherine): transfer LID data from GCS to BigQuery so
    # we can use it in queries (again, I'm leaving example transfer code here)

    load_lid_outputs = [
        GCSToBigQueryOperator(
            task_id="load_patent_categorization_" + table_name,
            bucket=DATA_BUCKET,
            source_objects=[f"{tmp_dir}/output_data/{data_name}.jsonl"],
            schema_object=f"{schema_dir}/{schema_name}.json",
            destination_project_dataset_table=f"{staging_dataset}.{table_name}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
        for table_name, data_name, schema_name in [
            ("staging_categories", "patent_categories", "staging_categories"),
            ("metrics", "metrics", "metrics"),
            ("tie_categories", "tie_categories", "tie_categories"),
        ]
    ]

    # TODO (Katherine): find data that needs to be translated
    # We'll need some BigQuery operators here; there are examples of these above
    # you can either use a single one or another sequence document depending on
    # how many queries you end up with!

    # TODO (Katherine): transfer data requiring translating to GCS

    export_prev_family_categories = BigQueryToGCSOperator(
        task_id="export_prev_family_categories",
        source_project_dataset_table=f"{production_dataset}.family_categories",
        destination_cloud_storage_uris=f"gs://{DATA_BUCKET}/{tmp_dir}/prev_family_categories/data*.jsonl",
        export_format="NEWLINE_DELIMITED_JSON",
        force_rerun=True,
    )

    # TODO (Katherine): run translation in KubernetesPod (again, example below)

    run_translation = GKEStartPodOperator(
        task_id="run-lid",
        name=f"run-lid",
        project_id=PROJECT_ID,
        location=GCP_ZONE,
        cluster_name="cc2-task-pool",
        do_xcom_push=True,
        cmds=["/bin/bash"],
        arguments=[
            "-c",
            (
                setup_commands
                + f" && python3 download_merged_ids.py --gcs_bucket {DATA_BUCKET} "
                f"--gcs_prefix {raw_merged_ids_dir} --s3_prefix data/merged_ids/{dataset} "
                f"--curr_baseset {curr_baseset}"
            ),
        ],
        namespace="default",
        image=f"gcr.io/{PROJECT_ID}/cc2-task-pool",
        get_logs=True,
        startup_timeout_seconds=300,
        on_finish_action="delete_pod",
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": [
                                        "default-pool",
                                    ],
                                }
                            ]
                        }
                    ]
                }
            }
        },
        annotations={"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
    )

    # TODO (Katherine): move translation outputs back to BigQuery so we can make final
    # table for FAISS

    load_patent_family_categorization_outputs = [
        GCSToBigQueryOperator(
            task_id="load_patent_family_categorization_" + table_name,
            bucket=DATA_BUCKET,
            source_objects=[f"{tmp_dir}/family_output_data/{data_name}.jsonl"],
            schema_object=f"{schema_dir}/{schema_name}.json",
            destination_project_dataset_table=f"{staging_dataset}.{table_name}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
        for table_name, data_name, schema_name in [
            ("family_categories", "patent_family_categories", "family_categories"),
            ("tie_family_categories", "tie_family_categories", "tie_family_categories"),
        ]
    ]

    # TODO (Katherine): one more BigQuery operator to merge all our text data for FAISS prep

    # TODO (Rebecca): export both the text data and CPC text to GCS

    # TODO (Rebecca): another operator to run FAISS on both outputs (possibly this one
    # isn't in kubernetes since it uses dataflow? a dataflow operator?)

    # TODO (Rebecca): Get the FAISS data back into BigQuery

    # TODO (Rebecca): a bigquery operator to create overall edge weights
    # possibly here it makes sense to make another sequence file so
    # we can combine this with
    # TODO (Katherine): (Bigquery operators to run the reattachment waves)
    # as there's no reason these couldn't be run in sequence

    # TODO (Rebecca): another data export to GCS for keywords

    # TODO (Rebecca): a KubernetesPod to run keyword extraction

    # TODO (Rebecca): transfer keyword data back to BigQuery

    wait_for_keyword_load = DummyOperator(task_id="wait_for_keyword_load")
    wait_for_queries = DummyOperator(task_id="wait_for_queries")

    # TODO (Rebecca): Update and finalize this to make sure it runs the query sequences

    curr_downstream_query = wait_for_keyword_load
    production_queries = [
        ("family_categories", production_dataset),
    ]
    with open(
        f"{os.environ.get('DAGS_FOLDER')}/{sequence_dir}/patent_clustering_query_sequence.csv"
    ) as f:
        for line in csv.DictReader(get_clean_lines(f)):
            if line["production_dataset"]:
                production_queries.append(
                    (line["table_name"], line["production_dataset"])
                )
                query = BigQueryInsertJobOperator(
                    task_id=line["table_name"],
                    configuration={
                        "query": {
                            "query": "{% include '"
                            + f"{sql_dir}/{line['table_name']}.sql"
                            + "' %}",
                            "useLegacySql": False,
                            "destinationTable": {
                                "projectId": PROJECT_ID,
                                "datasetId": staging_dataset,
                                "tableId": line["table_name"],
                            },
                            "allowLargeResults": True,
                            "createDisposition": "CREATE_IF_NEEDED",
                            "writeDisposition": "WRITE_TRUNCATE",
                        }
                    },
                )
                curr_downstream_query >> query
                curr_downstream_query = query

    # TODO (Rebecca and Katherine): Get everything sequenced correctly; example below

    (
        clear_tmp_dir
        >> get_input
        >> export_input_data
        >> run_categorize_patents
        >> load_patent_categorization_outputs
        >> export_prev_family_categories
        >> run_categorize_patent_family
        >> load_patent_family_categorization_outputs
        >> wait_for_family_load
    )

    curr_downstream_query >> wait_for_queries

    # TODO (Rebecca and Katherine): Add SQL checks and make sure they're set up right
    # Update the below to make sure production tables transfer and non-production ones don't

    checks = []
    for query in os.listdir(f"{os.environ.get('DAGS_FOLDER')}/{sql_dir}"):
        if not query.startswith("check_"):
            continue
        checks.append(
            BigQueryCheckOperator(
                task_id=query.replace(".sql", ""),
                sql=f"{sql_dir}/{query}",
                use_legacy_sql=False,
            )
        )

    wait_for_checks = DummyOperator(task_id="wait_for_checks")

    curr_date = datetime.now().strftime("%Y%m%d")
    non_production_backups = [
        BigQueryToBigQueryOperator(
            task_id=f"back_up_{table_name}",
            source_project_dataset_tables=[f"{staging_dataset}.{table_name}"],
            destination_project_dataset_table=f"{backups_dataset}.{table_name}_{curr_date}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
        for table_name in ["tie_family_categories", "tie_categories", "metrics"]
    ]
    non_production_backups.append(
        BigQueryToBigQueryOperator(
            task_id=f"back_up_curr_patent_metadata",
            source_project_dataset_tables=["unified_patents.classifications"],
            destination_project_dataset_table=f"{staging_dataset}.classifications",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
    )

    success_alert = get_post_success("Patent categorization update succeeded!", dag)

    wait_for_production_backups = DummyOperator(task_id="wait_for_production_backups")
    wait_for_production_backups >> non_production_backups >> success_alert

    with open(
        f"{os.environ.get('DAGS_FOLDER')}/schemas/{production_dataset}/table_descriptions.json"
    ) as f:
        table_desc = json.loads(f.read())
    for table, dataset in production_queries:
        prod_table_name = f"{dataset}.{table}"
        table_copy = BigQueryToBigQueryOperator(
            task_id=f"copy_{table}_to_production",
            source_project_dataset_tables=[f"{staging_dataset}.{table}"],
            destination_project_dataset_table=prod_table_name,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
        pop_descriptions = PythonOperator(
            task_id="populate_column_documentation_for_" + table,
            op_kwargs={
                "input_schema": f"{os.environ.get('DAGS_FOLDER')}/{schema_dir}/{table}.json",
                "table_name": prod_table_name,
                "table_description": table_desc[table],
            },
            python_callable=update_table_descriptions,
        )
        table_backup = BigQueryToBigQueryOperator(
            task_id=f"back_up_{table}",
            source_project_dataset_tables=[f"{staging_dataset}.{table}"],
            destination_project_dataset_table=f"{backups_dataset}.{table}_{curr_date}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )

        (
            wait_for_queries
            >> checks
            >> wait_for_checks
            >> table_copy
            >> pop_descriptions
            >> table_backup
            >> wait_for_production_backups
        )
