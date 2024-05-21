"""
Results from runs with a ~242K and ~475K sample are in `profiling`, generated like this:
kernprof -l -v --unit 1 flat_ip_index.py > flat_ip_index.py_242K_sample.lprof.txt
kernprof -l -v --unit 1 flat_ip_index.py --input_dir medium_embedding_sample --output_dir medium_embedding_sample_out >
  flat_ip_index.py_475K_sample.lprof.txt

This script used https://github.com/facebookresearch/faiss/blob/main/tutorial/python/1-Flat.py as a starting point.
It calculates the brute-force cosine similarities. The difference in time to calculate the top 10 between the two
sample sizes scales exactly as we would expect.
"""

import argparse
import json
import os
import shutil

import faiss
import line_profiler
import numpy as np
from tqdm import tqdm


@profile  # noqa: F821
def run(input_dir: str, output_dir: str):
    # 384 is the embedding vector size
    index = faiss.IndexFlatIP(384)
    numeric_to_family_id = {}
    embeddings = []
    curr_id = 0
    for fi in tqdm(os.listdir(input_dir)):
        with open(os.path.join(input_dir, fi)) as f:
            for line in f:
                js = json.loads(line)
                embeddings.append(np.array(js["text"]))
                numeric_to_family_id[curr_id] = js["family_id"]
                curr_id += 1
    print(f"Indexing {len(embeddings)} text embeddings")
    # Specifying an id is not supported with flat indexes, but faiss assigns a numeric id to entries in this index in
    # the order they are added. If we needed to, we could use the numeric_to_family_id dict defined above to turn these
    # ids back into family ids
    np_embeddings = np.array(embeddings)
    index.add(np_embeddings)
    # Get top 10 most similar ids for all embeddings
    similarities, ids = index.search(np_embeddings, 10)
    # Write out results in a human-readable form
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)
    # Start a new file after we've written this many records
    file_length = 100
    curr_file = None
    for num_id, (sims, sim_ids) in enumerate(zip(similarities.tolist(), ids.tolist())):
        if num_id % file_length == 0:
            if curr_file:
                curr_file.close()
            curr_file = open(
                os.path.join(output_dir, f"top_10_{num_id / file_length}.jsonl"),
                mode="w",
            )
        row = {"family_id": numeric_to_family_id[num_id]}
        row["most_similar"] = [
            {"family_id": numeric_to_family_id[sim_id], "similarity": sim}
            for sim, sim_id in zip(sims, sim_ids)
        ]
        curr_file.write(json.dumps(row) + "\n")
    curr_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", default="small_embedding_sample")
    parser.add_argument("--output_dir", default="small_embedding_sample_out")
    args = parser.parse_args()

    run(args.input_dir, args.output_dir)
