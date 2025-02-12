WITH
weights AS (
  SELECT
    family_id,
    1 / COUNT(DISTINCT family_reference) AS weight
  FROM staging_patent_clusters.new_family_references
  GROUP BY family_id
),

network AS (
  SELECT
    family_id,
    family_reference,
    weight
  FROM weights
  LEFT JOIN staging_patent_clusters.new_family_references USING (family_id)
)

SELECT * FROM network
