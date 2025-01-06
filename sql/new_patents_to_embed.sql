WITH
-- get patents that we translated
translated AS (
  SELECT
    patent_id,
    CAST(family_id AS STRING) AS family_id,
    title,
    title_original,
    abstract,
    abstract_original
  FROM staging_patent_clusters.new_translated_patents
),

-- get patents we didn't translate
non_translated AS (
  SELECT
    patent_id,
    family_id,
    title,
    title_original,
    abstract,
    abstract_original
  FROM staging_patent_clusters.new_metadata
  WHERE patent_id NOT IN (SELECT patent_id FROM translated)
),

-- combine translated and non-translated patents
all_patents AS (
  SELECT * FROM translated
  UNION DISTINCT SELECT * FROM non_translated
),

-- Remove nulls from our titles and abstracts so when we join things there won't be errors
no_nulls AS (
  SELECT
    patent_id,
    family_id,
    COALESCE(title, "") AS title,
    COALESCE(abstract, "") AS abstract,
    COALESCE(title_original, "") AS title_original,
    COALESCE(abstract_original, "") AS abstract_original
  FROM all_patents
),

-- Pick our preferred titles and abstracts for each individual patent
-- Our preference here is English titles and abstracts, unless there's both a title
-- and an abstract for the non-English and only one or the other for English
get_text AS (
  SELECT
    patent_id,
    family_id,
    CASE
      WHEN
        (
          (title = "" OR abstract = "") AND title_original != "" AND abstract_original != ""
        ) THEN title_original || " " || abstract_original
      ELSE
        title || " " || abstract
    END
    AS text
  FROM
    no_nulls),

-- Remove patents from our list that don't have text, so when we order within family
-- we're only looking at the patents that contain text
no_null_text AS (
  SELECT DISTINCT
    patent_id,
    family_id,
    text
  FROM
    get_text
  WHERE
    TRIM(text) != ""),


-- Put our dates in so we can prioritize our patents within family id by date
-- We're using first_priority_date first, then application date, then publication_date
add_dates AS (
  SELECT
    patent_id,
    COALESCE(no_null_text.family_id, "X-" || patent_id) AS family_id,
    text,
    COALESCE(first_priority_date, application_date, publication_date) AS patent_date
  FROM
    no_null_text
  LEFT JOIN
    unified_patents.dates
    USING
      (patent_id)),

-- Add an ordering column within families
-- Each patent is ordered first by its date (ideally picking the priority document)
-- Then by the length of text if we don't get an ordering just from date
get_ordering AS (
  SELECT
    family_id,
    patent_id,
    text,
    patent_date,
    RANK() OVER (PARTITION BY family_id ORDER BY patent_date ASC, LENGTH(text) DESC) AS ordering
  FROM
    add_dates)

-- Pick the top document based on our ordering
SELECT
  family_id,
  text
FROM
  get_ordering
WHERE
  ordering = 1
