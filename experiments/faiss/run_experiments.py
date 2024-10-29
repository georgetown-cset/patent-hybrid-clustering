"""
Generates similarity scores using FAISS. Run like
`python3 run_experiments.py --input_dir test_set --output_dir test_set_hnsw --index_name IndexHNSWFlat`
See README for configuration details.

Profiling results from runs with various indexes and datasets are in `profiling`, generated like this:
kernprof -l -v --unit 1 run_experiments.py --input_dir small_embedding_sample --output_dir small_embedding_sample_out
  --index_name IndexFlatIP > IndexFlatIP_242K_profile.txt
To generate comparable profiling results, uncomment the `line_profiler` import and the
`@profile` decorators

This script used these starting points:
https://github.com/facebookresearch/faiss/blob/main/tutorial/python/1-Flat.py and
https://github.com/facebookresearch/faiss/blob/main/tutorial/python/2-IVFFlat.py
https://www.pinecone.io/learn/series/faiss/vector-indexes/
"""

import argparse
import json
import os
import shutil

import faiss

# import line_profiler
import numpy as np
from tqdm import tqdm

EMBEDDING_SIZE = 384
TOP_N = 11


# @profile  # noqa: F821
def get_IndexFlatL2(np_embeddings):
    """
    Generates a faiss IndexFlatL2
    :param np_embeddings: Embeddings to generate the index from
    :return: an IndexFlatL2
    """
    index = faiss.IndexFlatL2(EMBEDDING_SIZE)
    index.add(np_embeddings)
    return index


# @profile  # noqa: F821
def get_IndexFlatIP(np_embeddings):
    """
    Generates a faiss IndexFlatIP
    :param np_embeddings: Embeddings to generate the index from
    :return: an IndexFlatIP
    """
    index = faiss.IndexFlatIP(EMBEDDING_SIZE)
    index.add(np_embeddings)
    return index


# @profile  # noqa: F821
def get_IndexIVFFlat(np_embeddings):
    """
    Generates a faiss IndexIVFFlat
    :param np_embeddings: Embeddings to generate the index from
    :return: an IndexIVFFlat
    """
    quantizer = faiss.IndexFlatL2(EMBEDDING_SIZE)
    total_cells = 100
    index = faiss.IndexIVFFlat(quantizer, EMBEDDING_SIZE, total_cells)
    index.train(np_embeddings)
    index.add(np_embeddings)
    return index


# @profile  # noqa: F821
def get_IndexHNSWFlat(np_embeddings):
    """
    Generates a faiss IndexHNSWFlat
    :param np_embeddings: Embeddings to generate the index from
    :return: an IndexHNSWFlat
    """
    # From https://www.pinecone.io/learn/series/faiss/vector-indexes/
    num_connections = 64  # number of connections each vertex will have
    ef_search = 32  # depth of layers explored during search
    ef_construction = 64  # depth of layers explored during index construction
    # initialize index
    index = faiss.IndexHNSWFlat(
        EMBEDDING_SIZE, num_connections, faiss.METRIC_INNER_PRODUCT
    )
    # set efConstruction and efSearch parameters
    index.hnsw.efConstruction = ef_construction
    index.hnsw.efSearch = ef_search
    # add data to index
    index.add(np_embeddings)
    return index


# @profile  # noqa: F821
def run(input_dir: str, output_dir: str, index_name: str) -> None:
    """
    Reads a directory of JSONL files containing patent embeddings, generates the specified faiss index, and writes the
    top `TOP_N` most similar patent families to the output directory in JSONL form
    :param input_dir: directory of JSONL files containing patent embeddings
    :param output_dir: directory where output JSONL files containing most similar patents should be written
    :param index_name: Name of the faiss index to use
    :return: None
    """
    # Specifying an id is not supported with flat indexes, but faiss assigns a numeric id to entries in these indexes in
    # the order they are added. This dict will record the mapping between the order a family id was added to the index
    # and the family id
    numeric_to_family_id = {}
    seen_family_ids = set()
    embeddings = []
    curr_id = 0
    for fi in tqdm(os.listdir(input_dir)):
        with open(os.path.join(input_dir, fi)) as f:
            for line in f:
                js = json.loads(line)
                if js["family_id"] in seen_family_ids:
                    print("warning, duplicate family_id: " + js["family_id"])
                    continue
                seen_family_ids.add(js["family_id"])
                norm = np.linalg.norm(js["text"])
                norm_vec = [i / norm for i in js["text"]]
                embeddings.append(np.array(norm_vec))
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
    # Get top n most similar ids for all embeddings
    similarities, ids = index.search(np_embeddings, TOP_N)
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
                os.path.join(output_dir, f"top_{TOP_N}_{num_id / file_length}.jsonl"),
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
    parser.add_argument(
        "--index_name",
        default="IndexFlatIP",
        choices=["IndexFlatIP", "IndexFlatL2", "IndexIVFFlat", "IndexHNSWFlat"],
    )
    args = parser.parse_args()

    run(args.input_dir, args.output_dir, args.index_name)
