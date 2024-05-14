CREATE OR REPLACE TABLE staging_patent_clusters.patent_network_sample AS (
  WITH
    expanded_network AS (
      SELECT
        family_id,
        family_reference
      FROM staging_patent_clusters.family_refererences
      WHERE family_id IN (SELECT DISTINCT family_id FROM kq57_sandbox.patent_network_samples)
      OR family_reference IN (SELECT DISTINCT family_id AS family_reference FROM kq57_sandbox.patent_network_samples)
    ),
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
        category
      FROM patent_categories.categories
    )

  SELECT
    DISTINCT
    family_id,
    n_links,
    category,
    text_cpc,
    text_title_abstract
  FROM expanded_network
  FULL JOIN (SELECT DISTINCT family_reference AS family_id FROM expanded_network) USING(family_id)
  LEFT JOIN links USING(family_id)
  LEFT JOIN categories USING(family_id)
  LEFT JOIN cpc_text USING(family_id)
  LEFT JOIN title_abstract_text USING(family_id)
  LEFT JOIN (SELECT DISTINCT family_id FROM kq57_sandbox.patent_network_samples) USING(family_id)
)