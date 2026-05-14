-- top importing clusters *FROM* cluster_id
WITH top_importing_clusters AS (
  SELECT
    cluster_a AS cluster_id,
    cluster_b AS importer,
    SUM(ct_ba) AS num_imports,
    ROW_NUMBER() OVER (PARTITION BY cluster_a ORDER BY SUM(ct_ba) DESC) AS importer_rank,
    ARRAY_AGG(STRUCT(year, ct_ba AS import_count) ORDER BY year DESC) AS import_counts
  FROM staging_patent_clusters.import_export_view
  WHERE ct_ba IS NOT NULL
  GROUP BY cluster_a, cluster_b
)

SELECT
  top_importing_clusters.cluster_id,
  ARRAY_AGG(STRUCT(
    importer,
    cset_extracted_phrase,
    cluster_title,
    cluster_summary,
    num_imports,
    importer_rank,
    import_counts
  )) AS importing_cluster_counts
FROM
  top_importing_clusters
LEFT JOIN staging_map_of_patents.locations_with_metadata ON importer = locations_with_metadata.cluster_id
WHERE importer_rank < 11
GROUP BY cluster_id
