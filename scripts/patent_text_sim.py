import json
import os
import sys
import yake
import csv
from collections import defaultdict
from typing import Optional
from google.cloud import bigquery
from wordfreq import zipf_frequency

"""
Replace the data function here with GCP bucket download. 
"""

client = bigquery.Client(project='gcp-cset-projects')

df = client.query("""

WITH cluster_text AS (
  SELECT
    cluster_id,
    family_id, 
    title_abstract
  FROM `staging_patent_clusters.cluster_family_text_data`
)

# Structure data for pipeline expectations
# Replace tab/non-breaking spaces at the start
SELECT
  CONCAT('{"cluster_id": ', cluster_id, '}') AS cluster_id,
  CONCAT('{"text_corp": "', REPLACE(title_abstract, '"', ''), '"}]') AS text_corp
FROM cluster_text
ORDER BY cluster_id, family_id

""").to_dataframe().to_csv('patent_cluster_data.csv', index=False, quoting=csv.QUOTE_NONE, escapechar=' ')

class Postprocessor:
    def __init__(self):
        """
        A class to postprocess phrases from yake for science map purposes
        """
        self.already_seen = defaultdict(set)

    def build_already_seen(self, cluster: int, phrase: str):
        """
        Builds a list of words that have already been seen in our multi-word phrases for each cluster.
        :param cluster: The cluster whose phrases we're evaluating
        :param phrase: The phrase
        :return: None
        """
        # If we have a multi-word phrase
        if " " in phrase.strip():
            # Add all the words in it to our dict
            self.already_seen[cluster].update([i.lower() for i in phrase.split(" ")])

    def remove_generics(self, cluster: int, phrase: str) -> Optional[str]:
        """
        Removes generic one-word phrases from our phrase list.
        A phrase is considered generic if a word from it was already seen in a multi-word phrase
        in the same cluster, or if it is a very common word in English.
        :param cluster: The cluster containing the phrases
        :param phrase: The phrase
        :return:
        """
        # if phrase is not a single word
        if len(phrase.split()) > 1:
          # remove duplicates and rebuild string
          unique_words = set(phrase.split())
          unique_words_string = ' '.join(unique_words)
          return unique_words_string
        # if phrase is a single word
        else:
          if " " not in phrase.strip():
              # if our single word is generic or in one of the cluster's multi-word phrases, remove it
              if (
                  zipf_frequency(phrase, "en") >= 4
                  or phrase.lower() in self.already_seen[cluster]
              ):
                  return
          return phrase

def get_cluster_text() -> defaultdict(str):
    """
    Read in cluster text to use in yake algorithm.
    :return: clust_text (dict of extracted phrases for clusters)
    """
    clust_text = defaultdict(str)
    with open('patent_cluster_data(1).csv') as f:
        # Skip header row of column names
        next(f)
        for line in f:
            line = '['+line
            # Some titles/abstracts just don't work, so skip them
            try:
              js = json.loads(line, strict=False)
              clust_text[int(js[0]['cluster_id'])] += " " + js[1]["text_corp"]
            except:
              continue
    return clust_text

def run_yake(clust_text) -> list:
    """
    Run yake phrase extraction algorithm on cluster texts
    :param clust_text: defaultdict of cluster texts
    :return: yake_output (phrases output by yake algorithm)
    """
    yake_output = []
    kw_extractor = yake.KeywordExtractor(n=3, dedupLim=0.5, top=20)
    for cluster in clust_text:
        keywords = kw_extractor.extract_keywords(clust_text[cluster])
        for kw in keywords:
            yake_output.append(
                {"cluster_id": cluster, "cset_extracted_phrase": kw[0], "score": kw[1]}
            )
    return yake_output

def extract_phrases() -> None:
    """
    Runs phrase extraction
    :param n_workers: Number of CPU workers used in multi-processing
    """
    print("Get cluster title + abstracts")
    cluster_text = get_cluster_text()
    print("Run yake algorithm on cluster texts")
    output = run_yake(cluster_text)
    print("Postprocess the results of yake algorithm")
    p = Postprocessor()
    new_extracted = []
    for row in output:
        p.build_already_seen(row["cluster_id"], row["cset_extracted_phrase"])
    #print(vars(p))
    for row in output:
        row["cset_extracted_phrase"] = p.remove_generics(
            row["cluster_id"], row["cset_extracted_phrase"]
        )
        if row["cset_extracted_phrase"]:
          print(row)
          new_extracted.append(row)
    with open('patent_cluster_phrases.jsonl', 'w') as out:
        for row in new_extracted:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    extract_phrases()