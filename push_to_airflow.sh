gsutil cp patent_hybrid_clustering_dag.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
#gsutil cp faiss_dag.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gsutil rm gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/patent_clusters/*

gsutil cp sql/*.sql gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/patent_clusters/
gsutil cp sql/breakdowns/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/patent_clusters/
gsutil cp sql/checks/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/patent_clusters/

gsutil cp schemas/* gs://airflow-data-exchange/patent_clusters/schemas/
gsutil cp schemas/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/patent_clusters/schemas/
gsutil cp scripts/* gs://airflow-data-exchange/patent_clusters/model/
gsutil cp scripts/similarity.py gs://airflow-data-exchange/patent_clusters/scripts/
gsutil cp sequences/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sequences/patent_clusters/
gsutil cp scripts/get_embeddings* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/patent_clusters/
