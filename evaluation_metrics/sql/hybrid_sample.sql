CREATE OR REPLACE TABLE kq57_sandbox.patent_network_samples AS (
  WITH
    links AS (
      SELECT
        family_id,
        COALESCE(COUNT(DISTINCT family_reference),0) + COALESCE(COUNT(DISTINCT family_citation),0) AS n_links
      FROM staging_patent_clusters.family_refererences
      FULL JOIN
        (
          SELECT
            family_reference AS family_id,
            family_id AS family_citation
          FROM staging_patent_clusters.family_refererences
        ) AS family_cits
      USING(family_id)
      GROUP BY family_id
    ),
    cpc_text AS (
      SELECT
        family_id,
        text AS text_cpc
      FROM staging_patent_clusters.family_cpc_text
    ),
    title_abstract_text AS (
      SELECT
        family_id,
        text AS text_title_abstract
      FROM staging_patent_clusters.patents_to_embed
    ),
    categories AS (
      SELECT
        DISTINCT
        family_id,
        category,
        ROW_NUMBER() OVER(PARTITION BY category) AS row_id
      FROM patent_categories.categories
      WHERE category IN ('biotechnology','telecommunications','food_and_tobacco','mining_and_quarrying','real_estate')
      ORDER BY RAND()
    )

  SELECT
    family_id,
    category,
    n_links,
    text_cpc,
    text_title_abstract
  FROM categories
  LEFT JOIN links USING(family_id)
  LEFT JOIN cpc_text USING(family_id)
  LEFT JOIN title_abstract_text USING(family_id)
  WHERE (family_id IN (SELECT DISTINCT family_id FROM links)
  OR family_id IN (SELECT DISTINCT family_id FROM cpc_text)
  OR family_id IN (SELECT DISTINCT family_id from title_abstract_text))
  AND row_id <= 200
)