import csv
import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers import GCSToGCSOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
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


args = get_default_args(pocs=["Rebecca", "Katherine"])

with DAG(
    "patent_hybrid_clustering",
    default_args=args,
    description="Runs patent hybrid clustering",
    schedule_interval=None,
) as dag:
    production_dataset = "patent_clusters"
    tmp_dir = f"{production_dataset}/tmp"
    schema_dir = f"{production_dataset}/schemas"
    scripts_dir = f"{production_dataset}/scripts"
    index_dir = f"{production_dataset}/indexes"
    sql_dir = f"sql/{production_dataset}"
    sequence_dir = f"sequences/{production_dataset}"
    staging_dataset = f"staging_{production_dataset}"
    backups_dataset = f"{production_dataset}_backups"
    gce_resource_id = "faiss"
    # This is the only region where we can currently create a m1-ultramem-160 instance.
    gce_zone = "us-central1-a"

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

    clear_tmp_dir = GCSDeleteObjectsOperator(
        task_id="clear_tmp_dir", bucket_name=DATA_BUCKET, prefix=tmp_dir
    )

    curr_downstream_query = clear_tmp_dir

    wait_for_initial_queries = DummyOperator(task_id="wait_for_initial_queries")

    with open(f"{DAGS_DIR}/{sequence_dir}/initial_data_query_sequences.csv") as f:
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

    curr_downstream_query >> wait_for_initial_queries

    export_patents_to_lid = BigQueryToGCSOperator(
        task_id="export_patents_to_lid",
        source_project_dataset_table=f"{staging_dataset}.new_metadata_to_lid",
        destination_cloud_storage_uris=f"gs://{DATA_BUCKET}/{tmp_dir}/new_metadata_to_lid/data*.jsonl",
        export_format="NEWLINE_DELIMITED_JSON",
        force_rerun=True,
    )

    run_lid = GKEStartPodOperator(
        task_id="run-lid",
        name="run-lid",
        project_id=PROJECT_ID,
        location=GCP_ZONE,
        cluster_name="cc2-task-pool",
        do_xcom_push=True,
        cmds=["/bin/bash"],
        arguments=[
            "-c",
            (
                f"echo 'starting lid' ; rm -r data || true"
                f"mkdir -p data/input_data && "
                f"mkdir -p data/output_data && "
                f"gsutil -m cp -r gs://{DATA_BUCKET}/{tmp_dir}/new_metadata_to_lid data/input_data && "
                f"python3 lid_new_patents.py --data_folder data"
                f"gsutil -m cp -r data/output_data gs://{DATA_BUCKET}/{tmp_dir}/new_metadata_lid "
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

    load_lid_outputs = GCSToBigQueryOperator(
        task_id="load_lid_outputs",
        bucket=DATA_BUCKET,
        source_objects=[f"{tmp_dir}/new_metadata_lid/lid.jsonl"],
        schema_object=f"{schema_dir}/patent_lid.json",
        destination_project_dataset_table=f"{staging_dataset}.new_patent_lid",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    patents_to_translate = BigQueryInsertJobOperator(
        task_id="new_patents_to_translate",
        configuration={
            "query": {
                "query": "{% include '"
                         + "new_patents_to_translate.sql"
                         + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": staging_dataset,
                    "tableId": "new_patents_to_translate",
                },
                "allowLargeResults": True,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    export_patents_for_translation = BigQueryToGCSOperator(
        task_id="export_patents_for_translation",
        source_project_dataset_table=f"{production_dataset}.new_patents_to_translate",
        destination_cloud_storage_uris=f"gs://{DATA_BUCKET}/{tmp_dir}/new_patents_to_translate/data*.jsonl",
        export_format="NEWLINE_DELIMITED_JSON",
        force_rerun=True,
    )

    run_translation = GKEStartPodOperator(
        task_id="run-translation",
        name="run-translation",
        project_id=PROJECT_ID,
        location=GCP_ZONE,
        cluster_name="cc2-task-pool",
        do_xcom_push=True,
        cmds=["/bin/bash"],
        arguments=[
            "-c",
            (
                f"echo 'starting translation' ; rm -r data || true"
                f"mkdir -p data/input_data && "
                f"mkdir -p data/output_data && "
                f"gsutil -m cp -r gs://{DATA_BUCKET}/{tmp_dir}/new_patents_to_translate data/input_data && "
                f"python3 translate_new_patents.py --data_folder data"
                f"gsutil -m cp -r data/output_output gs://{DATA_BUCKET}/{tmp_dir}/new_patents_to_translate "
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

    load_patent_translations = GCSToBigQueryOperator(
        task_id="load_patent_translations",
        bucket=DATA_BUCKET,
        source_objects=[f"{tmp_dir}/new_patents_to_translate/translated_patents.jsonl"],
        schema_object=f"{schema_dir}/new_translated_patents.json",
        destination_project_dataset_table=f"{staging_dataset}.new_translated_patents",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    patent_text_to_embed = BigQueryInsertJobOperator(
        task_id="patent_text_to_embed",
        configuration={
            "query": {
                "query": "{% include '"
                + f"{sql_dir}/new_patents_to_embed.sql"
                + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": staging_dataset,
                    "tableId": "new_patents_to_embed",
                },
                "allowLargeResults": True,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    export_patents_to_embed = BigQueryToGCSOperator(
        task_id="export_patents_to_embed",
        source_project_dataset_table=f"{staging_dataset}.new_patents_to_embed",
        destination_cloud_storage_uris=f"gs://{DATA_BUCKET}/{tmp_dir}/text_embedding/data*.jsonl",
        export_format="NEWLINE_DELIMITED_JSON",
        force_rerun=True,
    )

    dataflow_options = {
        "project": "gcp-cset-projects",
        "disk_size_gb": "30",
        "max_num_workers": "100",
        "temp_location": f"gs://{DATA_BUCKET}/{tmp_dir}/embedding_tmp/",
        "save_main_session": True,
        "requirements_file": "get_embeddings_requirements.txt",
        "runner": "DataflowRunner",
    }

    run_text_embedding = BeamRunPythonPipelineOperator(
        py_file=f"{DAGS_DIR}/{production_dataset}/get_embeddings.py",
        runner="DataflowRunner",
        task_id="run_text_embedding",
        default_pipeline_options=dataflow_options,
        pipeline_options={
            "input_data": f"gs://{DATA_BUCKET}/{tmp_dir}/embedding/to_embed_*",
            "output_data": f"gs://airflow-data-exchange/{production_dataset}/tmp/text_embeddings",
            "model": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        },
        dataflow_config=DataflowConfiguration(
            job_name="patent-embeddings-update",
            location="us-east1",
            wait_until_finished=True,
        ),
    )

    dataflow_options[
        "temp_location"
    ] = f"gs://{DATA_BUCKET}/{tmp_dir}/cpc_embedding_tmp/"

    run_cpc_embedding = BeamRunPythonPipelineOperator(
        py_file=f"{DAGS_DIR}/{production_dataset}/get_embeddings.py",
        runner="DataflowRunner",
        task_id="run_cpc_embedding",
        default_pipeline_options=dataflow_options,
        pipeline_options={
            "input_data": f"gs://{DATA_BUCKET}/{tmp_dir}/cpc_embedding/to_embed_*",
            "output_data": f"gs://airflow-data-exchange/{production_dataset}/tmp/cpc_embeddings",
            "model": "sentence-transformers/all-mpnet-base-v2",
        },
        dataflow_config=DataflowConfiguration(
            job_name="patent-embeddings-update",
            location="us-east1",
            wait_until_finished=True,
        ),
    )

    (
        export_patents_to_lid
        >> run_lid
        >> load_lid_outputs
        >> patents_to_translate
        >> export_patents_for_translation
        >> run_translation
        >> load_patent_translations
        >> patent_text_to_embed
        >> export_patents_to_embed
        >> run_text_embedding
        >> run_cpc_embedding
    )

    gce_instance_create = ComputeEngineInsertInstanceOperator(
        task_id=f"create_{gce_resource_id}",
        project_id=PROJECT_ID,
        zone=gce_zone,
        body={
            "name": gce_resource_id,
            "machine_type": f"zones/{gce_zone}/machineTypes/m1-ultramem-160",
            "disks": [
                {
                    "boot": True,
                    "auto_delete": True,
                    "initialize_params": {
                        "disk_size_gb": "2000",
                        "disk_type": f"zones/{gce_zone}/diskTypes/pd-balanced",
                        "source_image": "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20240927",
                    },
                }
            ],
            "network_interfaces": [
                {
                    "access_configs": [
                        {"name": "External NAT", "network_tier": "PREMIUM"}
                    ],
                    "stack_type": "IPV4_ONLY",
                    "subnetwork": "regions/us-central1/subnetworks/default",
                }
            ],
            "service_accounts": [
                {
                    "email": "dataloader@gcp-cset-projects.iam.gserviceaccount.com",
                    "scopes": [
                        "https://www.googleapis.com/auth/devstorage.full_control",
                        "https://www.googleapis.com/auth/cloud-platform",
                    ],
                }
            ],
        },
    )

    gce_instance_start = ComputeEngineStartInstanceOperator(
        task_id=f"start-{gce_resource_id}",
        project_id=PROJECT_ID,
        zone=gce_zone,
        resource_id=gce_resource_id,
    )

    indexes = ["cpc", "text"]
    embedding_dir = "{}_embeddings"

    prep_environment_sequence = [
        "sudo apt-get -y update",
        f"gsutil cp gs://{DATA_BUCKET}/{scripts_dir}/similarity.py .",
        "rm -r miniconda3 || true",
        "rm Miniconda3-latest-Linux-x86_64.sh || true",
        "wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh",
        "bash Miniconda3-latest-Linux-x86_64.sh -b",
        "miniconda3/bin/conda install -c pytorch faiss-cpu=1.8.0",
    ]

    for index in indexes:
        prep_environment_sequence.append(f"mkdir {embedding_dir.format(index)}")
        prep_environment_sequence.append(
            f"gsutil -m cp -r gs://{DATA_BUCKET}/{tmp_dir}/{embedding_dir.format(index)}/* "
            f"{embedding_dir.format(index)}/")
    prep_environment_script = " && ".join(prep_environment_sequence)

    prep_environment = BashOperator(
        task_id="prep_environment",
        bash_command=f'gcloud compute ssh airflow@{gce_resource_id} --zone {gce_zone} '
                     f'--command "{prep_environment_script}"',
    )

    gce_instance_create >> gce_instance_start.as_setup() >> prep_environment
    curr = prep_environment

    similarities_dir = "{}_similarities"
    for index in indexes:
        get_similarities_sequence = [
            f"miniconda3/bin/python similarity.py --input_dir {embedding_dir.format(index)} "
            f"--output_dir {similarities_dir.format(index)} --index_file {index}.pickle "
            f"--id_map_file {index}_map.pickle",
            f"gsutil -m cp -r {similarities_dir.format(index)} gs://{DATA_BUCKET}/{tmp_dir}/",
            # keep a snapshot of the last index and map
            f"gsutil cp gs://{DATA_BUCKET}/{index_dir}/{index}.pickle "
            f"gs://{DATA_BUCKET}/{index_dir}/{index}.pickle_prev; "
            f"gsutil cp new_{index}.pickle gs://{DATA_BUCKET}/{index_dir}/{index}.pickle",
            f"gsutil cp gs://{DATA_BUCKET}/{index_dir}/{index}_map.pickle "
            f"gs://{DATA_BUCKET}/{index_dir}/{index}_map.pickle_prev; "
            f"gsutil cp new_{index}_map.pickle gs://{DATA_BUCKET}/{index_dir}/{index}_map.pickle"
        ]
        get_similarities_script = (f"gsutil cp gs://{DATA_BUCKET}/{index_dir}/{index}.pickle .; "
                                   f"gsutil cp gs://{DATA_BUCKET}/{index_dir}/{index}_map.pickle .; " +
                                   (" && ".join(get_similarities_sequence)))

        get_embeddings = BashOperator(
            task_id=f"get_{index}_embeddings",
            bash_command=f'gcloud compute ssh airflow@{gce_resource_id} --zone {gce_zone} '
                         f'--command "{get_similarities_script}"',
        )

        curr >> get_embeddings
        curr = get_embeddings

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=PROJECT_ID,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id=f"stop-{gce_resource_id}",
    )

    gce_instance_delete = ComputeEngineDeleteInstanceOperator(
        task_id=f"delete_{gce_resource_id}",
        project_id=PROJECT_ID,
        zone=gce_zone,
        resource_id=gce_resource_id,
    )

    curr >> gce_instance_stop.as_teardown() >> gce_instance_delete
    # This piping is necessary to make the setup/teardown work
    gce_instance_start >> gce_instance_stop
    # Ensure that delete doesn't run if we're in the error -> teardown condition so we'll have a chance to review
    # the failing data
    curr >> gce_instance_delete

    for index in indexes:
        import_embeddings = GCSToBigQueryOperator(
            task_id=f"import_{index}_embeddings",
            bucket=DATA_BUCKET,
            source_objects=[f"{tmp_dir}/{similarities_dir.format(index)}/*"],
            schema_object=f"{schema_dir}/most_similar.json",
            destination_project_dataset_table=f"{staging_dataset}.new_most_similar_{index}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            # note that this is write append - be careful to clean the table out if you want to retry
            write_disposition="WRITE_APPEND",
            retries=0
        )

    gce_instance_delete >> import_embeddings

    wait_for_faiss_load = DummyOperator(task_id="wait_for_faiss_load")
    wait_for_map_queries = DummyOperator(task_id="wait_for_map_queries")

    curr_downstream_query = wait_for_faiss_load

    with open(f"{DAGS_DIR}/{sequence_dir}/map_building_sequences.csv") as f:
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

    curr_downstream_query >> wait_for_map_queries

    export_keyword_data = BigQueryToGCSOperator(
        task_id="export_keyword_data",
        source_project_dataset_table=f"{staging_dataset}.cluster_family_text_data",
        destination_cloud_storage_uris=f"gs://{DATA_BUCKET}/{tmp_dir}/cluster_family_text_data/data*.jsonl",
        export_format="NEWLINE_DELIMITED_JSON",
        force_rerun=True,
    )

    run_keyword_extraction = GKEStartPodOperator(
        task_id="run-keyword-extraction",
        name="run-keyword-extraction",
        project_id=PROJECT_ID,
        location=GCP_ZONE,
        cluster_name="cc2-task-pool",
        do_xcom_push=True,
        cmds=["/bin/bash"],
        arguments=[
            "-c",
            (
                f"echo 'starting keyword extraction' ; rm -r data || true"
                f"mkdir -p data/input_data && "
                f"mkdir -p data/output_data && "
                f"gsutil -m cp -r gs://{DATA_BUCKET}/{tmp_dir}/cluster_family_text_data data/input_data && "
                f"python3 patent_text_sim.py --input_data_folder data/input_data --output_data_folder data/output_data"
                f"gsutil -m cp -r data/output_data gs://{DATA_BUCKET}/{tmp_dir}/phrases/patent_cluster_phrases.jsonl "
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

    load_keywords = GCSToBigQueryOperator(
        task_id="load_keywords",
        bucket=DATA_BUCKET,
        source_objects=[f"{tmp_dir}/phrases/patent_cluster_phrases.jsonl"],
        schema_object=f"{schema_dir}/phrases.json",
        destination_project_dataset_table=f"{staging_dataset}.phrases",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    wait_for_keyword_load = DummyOperator(task_id="wait_for_keyword_load")
    wait_for_queries = DummyOperator(task_id="wait_for_queries")

    (
        wait_for_map_queries
        >> export_keyword_data
        >> run_keyword_extraction
        >> load_keywords
        >> wait_for_keyword_load
    )

    curr_downstream_query = wait_for_keyword_load
    # add any tables we want in production that aren't breakdowns
    production_queries = [
        ("cluster_assignment", production_dataset),
    ]
    with open(f"{DAGS_DIR}/{sequence_dir}/patent_clustering_query_sequence.csv") as f:
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

    curr_downstream_query >> wait_for_queries

    # TODO (Rebecca and Katherine): Add SQL checks and make sure they're set up right
    # Update the below to make sure production tables transfer and non-production ones don't

    checks = []
    for query in os.listdir(f"{DAGS_DIR}/{sql_dir}"):
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

    # TODO: Rebecca and Katherine: deal with backups
    # the only non-production backups we want are whatever the FAISS outputs are

    copy_cpc_embeddings = GCSToGCSOperator(
        task_id="copy_cpc_embeddings",
        source_bucket="airflow-data-exchange",
        source_objects=["patent_clusters/tmp/cpc_embeddings"],
        destination_bucket="patent_clustering",
        destination_object="cpc_embedding_output/",
        match_glob="**/*.embedded"
    )

    copy_text_embeddings = GCSToGCSOperator(
        task_id="copy_text_embeddings",
        source_bucket="airflow-data-exchange",
        source_objects=["patent_clusters/tmp/text_embeddings"],
        destination_bucket="patent_clustering",
        destination_object="embedding_output/",
        match_glob="**/*.embedded"
    )

    copy_indexes = GCSToGCSOperator(
        task_id="copy_text_embeddings",
        source_bucket="airflow-data-exchange",
        source_objects=["patent_clusters/indexes"],
        destination_bucket="airflow-data-exchange",
        destination_object="patent-clusters/indexes-backup/",
        match_glob="**/*.pickle"
    )

    curr_date = datetime.now().strftime("%Y%m%d")
    non_production_backups = [
        BigQueryToBigQueryOperator(
            task_id=f"back_up_{table_name}",
            source_project_dataset_tables=[f"{staging_dataset}.{table_name}"],
            destination_project_dataset_table=f"{backups_dataset}.{table_name}_{curr_date}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
        for table_name in ["most_similar_cpc", "most_similar_text", "hybrid_sts_scaled_weights",
                           "patent_lid", "translated_patents"]
    ]

    success_alert = get_post_success("Patent clustering update succeeded!", dag)

    wait_for_production_backups = DummyOperator(task_id="wait_for_production_backups")
    wait_for_production_backups >> non_production_backups >> success_alert

    with open(f"{DAGS_DIR}/schemas/{production_dataset}/table_descriptions.json") as f:
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
            >> copy_cpc_embeddings
            >> copy_text_embeddings
            >> copy_indexes
            >> table_copy
            >> pop_descriptions
            >> table_backup
            >> wait_for_production_backups
        )
