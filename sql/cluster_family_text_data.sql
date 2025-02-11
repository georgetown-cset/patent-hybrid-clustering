/*
Create table for downstream keyword extraction.
*/
WITH cluster_text AS (
  SELECT
    cluster_id,
    family_id,
    COALESCE(patents_to_embed.text, new_patents_to_embed.text) AS title_abstract
  FROM
    staging_patent_clusters.cluster_assignment_staging
  LEFT JOIN
    staging_patent_clusters.patents_to_embed
    USING
      (family_id)
  LEFT JOIN
    staging_patent_clusters.new_patents_to_embed
    USING
      (family_id)
)

-- Structure data for pipeline expectations
SELECT
  cluster_id,
  title_abstract AS text_corp
FROM
  cluster_text
ORDER BY
  cluster_id, family_id