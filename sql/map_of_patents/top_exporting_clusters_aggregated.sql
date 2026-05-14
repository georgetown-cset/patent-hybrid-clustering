-- top exporting clusters *to* cluster_id
WITH top_exporting_clusters AS (
  SELECT DISTINCT
    cluster_a AS cluster_id,
    cluster_b AS exporter,
    SUM(ct_ab) AS num_exports,
    ROW_NUMBER() OVER (PARTITION BY cluster_a ORDER BY SUM(ct_ab) DESC) AS exporter_rank,
    ARRAY_AGG(STRUCT(year, ct_ab AS export_count) ORDER BY year DESC) AS export_counts
  FROM staging_patent_clusters.import_export_view
  WHERE ct_ab IS NOT NULL
  GROUP BY cluster_a, cluster_b
)

SELECT
  top_exporting_clusters.cluster_id,
  ARRAY_AGG(STRUCT(
    exporter,
    cset_extracted_phrase,
    cluster_title,
    cluster_summary,
    num_exports,
    exporter_rank,
    export_counts
  )) AS exporting_cluster_counts
FROM
  top_exporting_clusters
LEFT JOIN staging_map_of_patents.locations_with_metadata ON exporter = locations_with_metadata.cluster_id
WHERE exporter_rank < 11
GROUP BY cluster_id
