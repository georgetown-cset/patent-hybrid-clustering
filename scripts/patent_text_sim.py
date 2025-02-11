import argparse
import json
import os
from collections import OrderedDict, defaultdict
from typing import Optional

import yake
from wordfreq import zipf_frequency

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
            # remove duplicates while maintaining order and rebuild string
            unique_words = " ".join(OrderedDict.fromkeys(phrase.split()))
            return unique_words
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


def get_cluster_text(text_dir: str) -> defaultdict(str):
    """
    Read in cluster text to use in yake algorithm.
    :return: clust_text (dict of extracted phrases for clusters)
    """
    clust_text = defaultdict(str)
    text_files = os.listdir(text_dir)
    for filename in text_files:
        with open(os.path.join(text_dir, filename)) as f:
            for line in f:
                js = json.loads(line)
                clust_text[int(js["cluster_id"])] += " " + js["text_corp"]
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


def extract_phrases(text_dir: str, output_dir: str) -> None:
    """
    Runs phrase extraction
    :param n_workers: Number of CPU workers used in multi-processing
    """
    print("Get cluster title + abstracts")
    cluster_text = get_cluster_text(text_dir)
    print("Run yake algorithm on cluster texts")
    output = run_yake(cluster_text)
    print("Postprocess the results of yake algorithm")
    p = Postprocessor()
    new_extracted = []
    for row in output:
        p.build_already_seen(row["cluster_id"], row["cset_extracted_phrase"])
    for row in output:
        row["cset_extracted_phrase"] = p.remove_generics(
            row["cluster_id"], row["cset_extracted_phrase"]
        )
        if row["cset_extracted_phrase"]:
            new_extracted.append(row)
    with open(os.path.join(output_dir, "patent_cluster_phrases.jsonl"), "w") as out:
        for row in new_extracted:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_data_folder", default="data/input_data")
    parser.add_argument("--output_data_folder", default="data/output_data")
    args = parser.parse_args()
    extract_phrases(args.input_data_folder, args.output_data_folder)
