from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from dataloader.airflow_utils.defaults import (
    DAGS_DIR,
    DATA_BUCKET,
    GCP_ZONE,
    PROJECT_ID,
    get_default_args,
    get_post_success,
)


args = get_default_args()
args["on_failure_callback"] = None
args["retries"] = 1

with (DAG(
    "faiss_check",
    default_args=args,
    description="Runs faiss",
    schedule_interval=None,
) as dag):
    production_dataset = "patent_clustering"
    tmp_dir = f"{production_dataset}/tmp"
    schema_dir = f"{production_dataset}/schemas"
    scripts_dir = f"{production_dataset}/scripts" # added this
    index_dir = f"{production_dataset}/indexes" # added this
    sql_dir = f"sql/{production_dataset}"
    sequence_dir = f"sequences/{production_dataset}"
    staging_dataset = f"staging_{production_dataset}"
    backups_dataset = f"{production_dataset}_backups"
    gce_resource_id = "faiss"
    # This is the only region where we can currently create a m1-ultramem-160 instance.
    gce_zone = "us-central1-a"

    # TODO: add as_setup, as_teardown
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
        # TODO - ask them to update their output directory.
        prep_environment_sequence.append(f"mkdir {embedding_dir.format(index)}")
        prep_environment_sequence.append(
            f"gsutil -m cp -r gs://{DATA_BUCKET}/{production_dataset}/{embedding_dir.format(index)}/* "
            f"{embedding_dir.format(index)}/")
    prep_environment_script = " && ".join(prep_environment_sequence)

    prep_environment = BashOperator(
        task_id="prep_environment",
        bash_command=f'gcloud compute ssh airflow@{gce_resource_id} --zone {gce_zone} '
                     f'--command "{prep_environment_script}"',
    )

    gce_instance_create >> gce_instance_start >> prep_environment
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

    curr >> gce_instance_stop >> gce_instance_delete

    for index in indexes:
        import_embeddings = GCSToBigQueryOperator(
            task_id=f"import_{index}_embeddings",
            bucket=DATA_BUCKET,
            source_objects=[f"{tmp_dir}/{similarities_dir.format(index)}/*"],
            schema_object=f"{schema_dir}/most_similar.json",
            destination_project_dataset_table=f"{staging_dataset}.most_similar_{index}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            # note that this is write append - be careful to clean the table out if you want to retry
            write_disposition="WRITE_APPEND",
            retries=0
        )

        gce_instance_delete >> import_embeddings




