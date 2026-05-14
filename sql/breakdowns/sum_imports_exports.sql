-- get the number of imports and exports over all years by distinct citing cluster and cited cluster pair
SELECT
  cluster_a,
  cluster_b,
  SUM(ct_ab) AS num_imports,
  SUM(ct_ba) AS num_exports
FROM
  staging_patent_clusters.import_export_view
GROUP BY
  cluster_a,
  cluster_b
ORDER BY
  cluster_a
