"""
Results from brute-force runs with a ~242K and ~475K sample are in `profiling`, generated like this:
kernprof -l -v --unit 1 run_experiments.py --input_dir small_embedding_sample --output_dir small_embedding_sample_out
  --index_name IndexFlatIP > IndexFlatIP_242K_profile.txt
kernprof -l -v --unit 1 run_experiments.py --input_dir medium_embedding_sample --output_dir medium_embedding_sample_out
  --index_name IndexFlatIP > IndexFlatIP_475K_profile.txt


This script used https://github.com/facebookresearch/faiss/blob/main/tutorial/python/1-Flat.py and
https://github.com/facebookresearch/faiss/blob/main/tutorial/python/2-IVFFlat.py as a starting point.
It calculates the brute-force inner products (IndexFlatIP). The difference in time to calculate the top 10 between
the two sample sizes scales exactly as we would expect.

It also calculates "Inverted file with exact post-verification" similarities (IndexIVFFlat) - see
https://github.com/facebookresearch/faiss/wiki/Faiss-indexes
"""

import argparse
import json
import os
import shutil

import faiss
import line_profiler
import numpy as np
from tqdm import tqdm

EMBEDDING_SIZE = 384


@profile  # noqa: F821
def get_IndexFlatL2(np_embeddings):
    index = faiss.IndexFlatL2(EMBEDDING_SIZE)
    index.add(np_embeddings)
    return index



@profile  # noqa: F821
def get_IndexFlatIP(np_embeddings):
    index = faiss.IndexFlatIP(EMBEDDING_SIZE)
    # Specifying an id is not supported with flat indexes, but faiss assigns a numeric id to entries in this index in
    # the order they are added. If we needed to, we could use the numeric_to_family_id dict defined in `run` to turn
    # these ids back into family ids
    index.add(np_embeddings)
    return index


@profile  # noqa: F821
def get_IndexIVFFlat(np_embeddings):
    quantizer = faiss.IndexFlatL2(EMBEDDING_SIZE)
    total_cells = 100
    index = faiss.IndexIVFFlat(quantizer, EMBEDDING_SIZE, total_cells)
    index.train(np_embeddings)
    index.add(np_embeddings)
    return index


@profile
def get_IndexHNSWFlat(np_embeddings):
    # From https://www.pinecone.io/learn/series/faiss/vector-indexes/
    num_connections = 64  # number of connections each vertex will have
    ef_search = 32  # depth of layers explored during search
    ef_construction = 64  # depth of layers explored during index construction
    # initialize index
    index = faiss.IndexHNSWFlat(EMBEDDING_SIZE, num_connections)
    # set efConstruction and efSearch parameters
    index.hnsw.efConstruction = ef_construction
    index.hnsw.efSearch = ef_search
    # add data to index
    index.add(np_embeddings)
    return index


@profile  # noqa: F821
def run(input_dir: str, output_dir: str, index_name: str):
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
    np_embeddings = np.array(embeddings)
    if index_name == "IndexFlatIP":
        index = get_IndexFlatIP(np_embeddings)
    if index_name == "IndexFlatL2":
        index = get_IndexFlatL2(np_embeddings)
    elif index_name == "IndexIVFFlat":
        index = get_IndexIVFFlat(np_embeddings)
    elif index_name == "IndexHNSWFlat":
        index = get_IndexHNSWFlat(np_embeddings)
    # Get top 10 most similar ids for all embeddings
    similarities, ids = index.search(np_embeddings, 10)
    # Write out results in a human-readable form
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)
    # Start a new file after we've written this many records
    file_length = 1000
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
            if sim_id != -1
        ]
        curr_file.write(json.dumps(row) + "\n")
    curr_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", default="small_embedding_sample")
    parser.add_argument("--output_dir", default="small_embedding_sample_out")
    parser.add_argument("--index_name", default="IndexFlatIP",
                        choices=["IndexFlatIP", "IndexFlatL2", "IndexIVFFlat", "IndexHNSWFlat"])
    args = parser.parse_args()

    run(args.input_dir, args.output_dir, args.index_name)
