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
import pickle
import shutil

import faiss

# import line_profiler
import numpy as np
from numpy.typing import ArrayLike
from tqdm import tqdm

TOP_N = 11
OUTPUT_PREFIX = "new_"


# @profile  # noqa: F821
def get_IndexFlatL2(np_embeddings: ArrayLike, embedding_size: int):
    """
    Generates a faiss IndexFlatL2
    :param np_embeddings: Embeddings to generate the index from
    :param embedding_size: Size of the embedding vectors
    :return: an IndexFlatL2
    """
    index = faiss.IndexFlatL2(embedding_size)
    index.add(np_embeddings)
    return index


# @profile  # noqa: F821
def get_IndexFlatIP(np_embeddings: ArrayLike, embedding_size: int):
    """
    Generates a faiss IndexFlatIP
    :param np_embeddings: Embeddings to generate the index from
    :param embedding_size: Size of the embedding vectors
    :return: an IndexFlatIP
    """
    index = faiss.IndexFlatIP(embedding_size)
    index.add(np_embeddings)
    return index


# @profile  # noqa: F821
def get_IndexIVFFlat(np_embeddings: ArrayLike, embedding_size: int):
    """
    Generates a faiss IndexIVFFlat
    :param np_embeddings: Embeddings to generate the index from
    :return: an IndexIVFFlat
    """
    quantizer = faiss.IndexFlatL2(embedding_size)
    total_cells = 100
    index = faiss.IndexIVFFlat(quantizer, embedding_size, total_cells)
    index.train(np_embeddings)
    index.add(np_embeddings)
    return index


# @profile  # noqa: F821
def get_IndexHNSWFlat(np_embeddings: ArrayLike, embedding_size: int):
    """
    Generates a faiss IndexHNSWFlat
    :param np_embeddings: Embeddings to generate the index from
    :param embedding_size: Size of the embedding vectors
    :return: an IndexHNSWFlat
    """
    # From https://www.pinecone.io/learn/series/faiss/vector-indexes/
    num_connections = 64  # number of connections each vertex will have
    ef_search = 32  # depth of layers explored during search
    ef_construction = 64  # depth of layers explored during index construction
    # initialize index
    index = faiss.IndexHNSWFlat(
        embedding_size, num_connections, faiss.METRIC_INNER_PRODUCT
    )
    # set efConstruction and efSearch parameters
    index.hnsw.efConstruction = ef_construction
    index.hnsw.efSearch = ef_search
    # add data to index
    index.add(np_embeddings)
    return index


# @profile  # noqa: F821
def run(
    input_dir: str, output_dir: str, index_name: str, index_file: str, id_map_file: str
) -> None:
    """
    Reads a directory of JSONL files containing patent embeddings, generates the specified faiss index, and writes the
    top `TOP_N` most similar patent families to the output directory in JSONL form
    :param input_dir: directory of JSONL files containing patent embeddings
    :param output_dir: directory where output JSONL files containing most similar patents should be written
    :param index_name: Name of the faiss index to use
    :param index_file: Name of file containing existing index, if any. Will be overwritten by updated index.
    :param id_map_file: Name of file containing map between CSET and FAISS ids. Will be overwritten by updated map.
    :return: None
    """
    # Specifying an id is not supported with flat indexes, but faiss assigns a numeric id to entries in these indexes in
    # the order they are added. This dict will record the mapping between the order a family id was added to the index
    # and the family id
    numeric_to_family_id = {}
    if os.path.exists(id_map_file):
        with open(id_map_file, mode="rb") as f:
            numeric_to_family_id = pickle.load(f)
    seen_family_ids = {v for _, v in numeric_to_family_id.items()}
    embeddings = []
    curr_id = max({k for k in numeric_to_family_id}) + 1 if numeric_to_family_id else 0
    min_id = curr_id
    embedding_size = None
    for fi in tqdm(os.listdir(input_dir)):
        with open(os.path.join(input_dir, fi)) as f:
            for line in f:
                js = json.loads(line)
                if js["family_id"] in seen_family_ids:
                    print("warning, duplicate family_id: " + js["family_id"])
                    continue
                seen_family_ids.add(js["family_id"])
                if not embedding_size:
                    embedding_size = len(js["text"])
                norm = np.linalg.norm(js["text"])
                norm_vec = [i / norm for i in js["text"]]
                embeddings.append(np.array(norm_vec))
                numeric_to_family_id[curr_id] = js["family_id"]
                curr_id += 1
    print(f"Indexing {len(embeddings)} text embeddings")
    np_embeddings = np.array(embeddings)
    if os.path.exists(index_file):
        with open(index_file, mode="rb") as f:
            index = pickle.load(f)
            index.add(np_embeddings)
    elif index_name == "IndexFlatIP":
        index = get_IndexFlatIP(np_embeddings, embedding_size)
    elif index_name == "IndexFlatL2":
        index = get_IndexFlatL2(np_embeddings, embedding_size)
    elif index_name == "IndexIVFFlat":
        index = get_IndexIVFFlat(np_embeddings, embedding_size)
    elif index_name == "IndexHNSWFlat":
        index = get_IndexHNSWFlat(np_embeddings, embedding_size)
    # Get top n most similar ids for all input embeddings
    similarities, ids = index.search(np_embeddings, TOP_N)
    print(ids)
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
        row = {"family_id": numeric_to_family_id[num_id + min_id]}
        row["most_similar"] = [
            {"family_id": numeric_to_family_id[sim_id], "similarity": sim}
            for sim, sim_id in zip(sims, sim_ids)
            if sim_id != -1
        ]
        curr_file.write(json.dumps(row) + "\n")
    curr_file.close()
    with open(f"{OUTPUT_PREFIX}{index_file}", mode="wb") as f:
        pickle.dump(index, f)
    with open(f"{OUTPUT_PREFIX}{id_map_file}", mode="wb") as f:
        pickle.dump(numeric_to_family_id, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_dir", default="small_embedding_sample", help="Directory of embeddings"
    )
    parser.add_argument(
        "--output_dir",
        default="small_embedding_sample_out",
        help="Directory where lists of most similar documents should be written",
    )
    parser.add_argument(
        "--index_name",
        default="IndexHNSWFlat",
        choices=["IndexFlatIP", "IndexFlatL2", "IndexIVFFlat", "IndexHNSWFlat"],
        help="FAISS name of index",
    )
    parser.add_argument(
        "--index_file",
        help="Name of index file (can be null). "
        f"New index will be written to this filename prefixed with `{OUTPUT_PREFIX}`.",
    )
    parser.add_argument(
        "--id_map_file",
        help="Name of file containing id map (can be null). "
        f"New index will be written to this filename prefixed with `{OUTPUT_PREFIX}`.",
    )
    args = parser.parse_args()

    run(
        args.input_dir,
        args.output_dir,
        args.index_name,
        args.index_file,
        args.id_map_file,
    )
