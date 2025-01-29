gsutil cp patent_hybrid_clustering_dag.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
#gsutil cp faiss_dag.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gsutil rm gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/patent_clustering/*

gsutil cp sql/*.sql gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/patent_clustering/
gsutil cp sql/breakdowns/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/patent_clustering/
gsutil cp sql/checks/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/patent_clustering/

gsutil cp schemas/* gs://airflow-data-exchange/patent_clustering/schemas/
gsutil cp schemas/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/schemas/patent_clustering/
gsutil cp scripts/* gs://airflow-data-exchange/patent_clustering/model/
gsutil cp experiments/faiss/similarity.py gs://airflow-data-exchange/patent_clustering/scripts/
gsutil cp sequences/* gs://airflow-data-exchange/patent_clustering/sequences/
