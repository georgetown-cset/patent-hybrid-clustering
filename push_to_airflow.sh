DAGS_FOLDER_GCS="us-east1-production-cc2-202-b42a7a54-bucket/dags"

gcloud storage cp patent_hybrid_clustering_dag.py gs://$DAGS_FOLDER_GCS/
#gcloud storage cp faiss_dag.py gs://$DAGS_FOLDER_GCS/
gcloud storage rm gs://$DAGS_FOLDER_GCS/sql/patent_clusters/*

gcloud storage cp sql/*.sql gs://$DAGS_FOLDER_GCS/sql/patent_clusters/
gcloud storage cp sql/breakdowns/* gs://$DAGS_FOLDER_GCS/sql/patent_clusters/
gcloud storage cp sql/checks/* gs://$DAGS_FOLDER_GCS/sql/patent_clusters/

gcloud storage cp schemas/* gs://airflow-data-exchange/patent_clusters/schemas/
gcloud storage cp schemas/* gs://$DAGS_FOLDER_GCS/patent_clusters/schemas/
gcloud storage cp scripts/* gs://airflow-data-exchange/patent_clusters/model/
gcloud storage cp scripts/similarity.py gs://airflow-data-exchange/patent_clusters/scripts/
gcloud storage cp sequences/* gs://$DAGS_FOLDER_GCS/sequences/patent_clusters/
gcloud storage cp scripts/get_embeddings* gs://$DAGS_FOLDER_GCS/patent_clusters/

# mop_bq_to_bq_metadata DAG: uses the template DAG in airflow_sql_sequences_templates (sql_updater_with_checks_dag.py)
gcloud storage cp config/mop_bq_to_bq_metadata.json gs://$DAGS_FOLDER_GCS/sql_updater_with_checks_config/
gcloud storage cp -r sql/map_of_patents/* gs://$DAGS_FOLDER_GCS/sql/mop_bq_to_bq_metadata/
gcloud storage cp -r sql/map_of_patents_checks/* gs://$DAGS_FOLDER_GCS/sql/mop_bq_to_bq_metadata/
