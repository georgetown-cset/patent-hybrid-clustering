from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
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

args = get_default_args()
args["on_failure_callback"] = None
args["retries"] = 0

with DAG(
    "faiss_for_clusters",
    default_args=args,
    description="Runs faiss. Need to input a production dataset, "
                + "since it is modified to allow for different clutsering tasks. Set 'production_dataset' in "
                + "the config json. To swtich from papers to patents, need to modify the 'indexes' list in the"
                + "DAG itself.",
    schedule_interval=None,
) as dag:

    production_dataset = "papers_clusters" #"{{dag_run.conf['production_dataset']}}"
    tmp_dir = f"{production_dataset}/tmp"
    schema_dir = f"{production_dataset}/schemas"
    scripts_dir = f"{production_dataset}/scripts"
    index_dir = f"{production_dataset}/indexes"
    staging_dataset = f"staging_{production_dataset}"
    gce_resource_id = "faiss-for-clusters-ultramem"
    # This is the only region where we can currently create a m1-ultramem-160 instance.
    gce_zone = "us-central1-a"

    gce_instance_start = ComputeEngineStartInstanceOperator(
        task_id=f"start-{gce_resource_id}",
        project_id=PROJECT_ID,
        zone=gce_zone,
        resource_id=gce_resource_id,
    )

    embedding_dir = "text_embeddings"

    prep_environment_sequence = [
        #"mkdir testing_test",
        #"sudo apt-get -y update",
        f"gsutil cp gs://{DATA_BUCKET}/{scripts_dir}/similarity.py .",
        #"rm -r miniconda3 || true",
        #"rm Miniconda3-latest-Linux-x86_64.sh || true",
        #"rm Miniconda3-py312_25.1.1-2-Linux-x86_64.sh || true",
        #"wget https://repo.anaconda.com/miniconda/Miniconda3-py312_25.1.1-2-Linux-x86_64.sh",
        #"bash Miniconda3-py312_25.1.1-2-Linux-x86_64.sh -b",
        #"miniconda3/bin/conda install -c pytorch faiss-cpu=1.8.0",
        f"rm -r {embedding_dir} || true",
        f"mkdir {embedding_dir}",
        f"gsutil -m cp -r gs://{DATA_BUCKET}/{tmp_dir}/{embedding_dir}/* "
        f"{embedding_dir}/"
    ]

    prep_environment_script = " && ".join(prep_environment_sequence)

    #prep_environment_script = "mkdir testing_test_baaarrrgggggssss"

    prep_environment = BashOperator(
        task_id="prep_environment",
        bash_command=f"gcloud compute ssh airflow@{gce_resource_id} --zone={gce_zone} "
        f'--command="{prep_environment_script}"',
    )

    gce_instance_start.as_setup() >> prep_environment

    similarities_dir = "text_similarities"

    get_similarities_sequence = [
        f"miniconda3/bin/python similarity.py --input_dir {embedding_dir} "
        f"--output_dir {similarities_dir} --index_file text.pickle "
        f"--id_map_file text_map.pickle",
        f"gsutil -m cp -r {similarities_dir} gs://{DATA_BUCKET}/{tmp_dir}/",
        # keep a snapshot of the last index and map
        f"gsutil cp gs://{DATA_BUCKET}/{index_dir}/text.pickle "
        f"gs://{DATA_BUCKET}/{index_dir}/text.pickle_prev; "
        f"gsutil cp new_text.pickle gs://{DATA_BUCKET}/{index_dir}/text.pickle",
        f"gsutil cp gs://{DATA_BUCKET}/{index_dir}/text_map.pickle "
        f"gs://{DATA_BUCKET}/{index_dir}/text_map.pickle_prev; "
        f"gsutil cp new_text_map.pickle gs://{DATA_BUCKET}/{index_dir}/text_map.pickle",
    ]
    get_similarities_script = (
        f"gsutil cp gs://{DATA_BUCKET}/{index_dir}/text.pickle .; "
        f"gsutil cp gs://{DATA_BUCKET}/{index_dir}/text_map.pickle .; "
        + (" && ".join(get_similarities_sequence))
    )

    #get_similarities_script = "mkdir testing_testing_part2_aaahhhhh"

    get_embeddings = BashOperator(
        task_id=f"get_text_embeddings",
        bash_command=f"gcloud compute ssh airflow@{gce_resource_id} --zone={gce_zone} "
        f'--command="{get_similarities_script}"',
    )

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=PROJECT_ID,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id=f"stop-{gce_resource_id}",
    )

    prep_environment >> get_embeddings >> gce_instance_stop.as_teardown() #>> gce_instance_delete
    # This piping is necessary to make the setup/teardown work
    #gce_instance_start >> gce_instance_stop >> gce_instance_delete
    # Ensure that delete doesn't run if we're in the error -> teardown condition so we'll have a chance to review
    # the failing data

    import_embeddings = GCSToBigQueryOperator(
        task_id=f"import_text_embeddings",
        bucket=DATA_BUCKET,
        source_objects=[f"{tmp_dir}/{similarities_dir}/*"],
        schema_object=f"{schema_dir}/most_similar.json",
        destination_project_dataset_table=f"{staging_dataset}.most_similar_text",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        # note that this is write append - be careful to clean the table out if you want to retry
        write_disposition="WRITE_APPEND",
        retries=0,
    )

    #gce_instance_delete >> import_embeddings

    gce_instance_stop.as_teardown() >> import_embeddings
