import csv
import json
import os

from google.cloud import bigquery

# Loading citation network based on full network
# To load the citation weights, for each patent family, the weight from one to anther is found by
# doing 1/n where n is the number of outgoing citations (references)


citation_query = """

WITH
weights AS (
  SELECT
    family_id,
    1/COUNT(DISTINCT family_reference) AS weight
  FROM staging_patent_clusters.family_refererences
  GROUP BY family_id
),
network AS (
  SELECT
    family_id,
    family_reference,
    weight
  FROM weights
  LEFT JOIN staging_patent_clusters.family_refererences USING(family_id)
)

SELECT * FROM network

"""

text_query = """SELECT
  text_match.family_id,
  similar.family_id AS family_reference,
  similar.similarity as weight
FROM
  tmp.full_title_abstract_embeddings_HNSW_cos_sim_20240707 AS text_match
CROSS JOIN
  UNNEST(most_similar) AS similar
WHERE
  similar.family_id != text_match.family_id
  """

cpc_query = """SELECT
  text_match.family_id,
  similar.family_id AS family_reference,
  similar.similarity as weight
 FROM
  gcp-cset-projects.tmp.cpc_embeddings_HNSW_out_20240720 as text_match
CROSS JOIN
  UNNEST(most_similar) AS similar
WHERE
  similar.family_id != text_match.family_id
"""


def get_weights(client, query, step):
    result = client.query(query)

    weights = []
    for i, row in enumerate(result):
        if i % 1000000 == 0:
            print(step, i)
        weights.append(
            {
                "family_id": row["family_id"],
                "family_link": row["family_reference"],
                "weight": row["weight"],
            }
        )

    return weights


def combine_weights(citation_weights, text_weights, cpc_weights):
    all_weights = {}
    for row in citation_weights:
        f1 = min([row["family_id"], row["family_link"]])
        f2 = max([row["family_id"], row["family_link"]])
        weight = row["weight"]
        all_weights[(f1, f2)] = {
            "weight_citation": weight,
            "weight_text": 0,
            "weight_cpc": 0,
        }

    for row in text_weights:
        f1 = min([row["family_id"], row["family_link"]])
        f2 = max([row["family_id"], row["family_link"]])
        weight = row["weight"]
        try:
            all_weights[(f1, f2)]["weight_text"] = weight
        except KeyError:
            all_weights[(f1, f2)] = {
                "weight_citation": 0,
                "weight_text": weight,
                "weight_cpc": 0,
            }

    for row in cpc_weights:
        f1 = min([row["family_id"], row["family_link"]])
        f2 = max([row["family_id"], row["family_link"]])
        weight = row["weight"]
        try:
            all_weights[(f1, f2)]["weight_cpc"] = weight
        except KeyError:
            all_weights[(f1, f2)] = {
                "weight_citation": 0,
                "weight_text": 0,
                "weight_cpc": weight,
            }
    return all_weights


def scale_weights(all_weights):
    # For the STS scaling, we need to find two parameters alpha and beta.

    sum_DC = sum([v["weight_citation"] for v in all_weights.values()])
    sum_ST = sum([v["weight_text"] for v in all_weights.values()])
    sum_SC = sum([v["weight_cpc"] for v in all_weights.values()])

    alpha = 1 / (sum_DC / sum_SC + 1 + sum_DC / sum_ST)
    beta = alpha * sum_DC / sum_ST

    # Now, we compute the weights for each hybrid network

    hybrid_sts_scaled_weights = []

    for k in all_weights.keys():
        family_id = k[0]
        family_link = k[1]

        citation_weight = all_weights[k]["weight_citation"]
        text_weight = all_weights[k]["weight_text"]
        cpc_weight = all_weights[k]["weight_cpc"]

        sts_weight = (
            alpha * citation_weight
            + beta * text_weight
            + (1 - alpha - beta) * cpc_weight
        )
        hybrid_sts_scaled_weights.append(
            {"family_id": family_id, "family_link": family_link, "weight": sts_weight}
        )

    return hybrid_sts_scaled_weights


def save_results(weights, directory, filename):
    with open(os.path.join(directory, filename), "w") as jfil:
        for row in weights:
            json.dump(row, jfil)
            jfil.write("\n")


if __name__ == "__main__":
    bigquery_client = bigquery.Client()
    print("Get citation weights")
    citations = get_weights(bigquery_client, citation_query, "citation")
    print("Get text weights")
    text = get_weights(bigquery_client, text_query, "text")
    print("Get cpc weights")
    cpc = get_weights(bigquery_client, cpc_query, "cpc")
    print("Combine weights")
    weights = combine_weights(citations, text, cpc)
    print("Scale weights")
    hybrid_weights = scale_weights(weights)
    print("Save results")
    save_results(hybrid_weights, "data", "patent_hybrid_sts_scaled_weights.jsonl")
