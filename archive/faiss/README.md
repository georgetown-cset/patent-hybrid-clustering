# FAISS experiments

## Setup

To run FAISS on a new VM, do the following:

1. Create an appropriately sized VM. To calculate similarities on the title/abstract embeddings in
`gs://patent-clustering/embedding_output`, I used a `m1-megamem-96` instance with a 2 TB disk and ended up using about
1.3 TB and 700+ GB of memory while the experiment was running. To calculate similarities on the CPC text embeddings in
`gs://patent-clustering/cpc_embedding_output`, I had to use a `m1-ultramem-160` instance to have sufficient memory,
but a 2 TB disk was still sufficient. The code has been run successfully on Ubuntu 20.04 and 24.04.
1. Install dependencies:
```bash
sudo apt-get update
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
<restart your shell here>
conda install -c pytorch faiss-cpu=1.8.0
```
1. Choose your index. `IndexFlatL2` can be used to calculate similarities for a small ground truth set to test your
configuration or compare to a different index (see `score.py`). `IndexHNSWFlat` is what we have used for large-scale
similarity calculations - see results below
1. Run your experiment, e.g. `python3 run_experiments.py --input_dir test_set --output_dir test_set_hnsw
--index_name IndexHNSWFlat`. You should use a screen or tmux session to ensure your experiment does not die due to a
lost ssh connection.

## Summary of time to search from `profiling`

| Dataset size | Method | Time to search |
| ------------ | ------ |----------------|
| 242K | IndexFlatIP | 29.5s          |
| 242K | IndexIVFFlat | 11.4s          |
| 242K | IndexHNSWFlat | 16.7s          |
| 475K | IndexFlatIP | 113.7s         |
| 475K | IndexIVFFlat | 56.8s          |
| 475K | IndexHNSWFlat | 41.9s          |

A top-11 run with IndexHNSWFlat on all title + abstract embeddings finished in about 20 hours - see breakdown
in `profiling/IndexHNSWFlat_full_top11_profile.txt`.

## Search accuracy

Pretty poor so far for IndexIVFFlat. We can increase nprobes but if IndexHNSWFlat works for us we might as well
just start there.

```
jm3312@patent-clustering-faiss:~$ python3 score.py --ground_truth small_embedding_sample_IndexFlatL2_out --comparison small_embedding_sample_IndexIVFFlat_out
Average top n overlap: 54%
Average top n overlap, excluding first result: 49%
Median top n overlap: 50.0%
Median top n overlap, excluding first result: 44.0%
(base) jm3312@patent-clustering-faiss:~$ python3 score.py --ground_truth small_embedding_sample_IndexFlatL2_out --comparison small_embedding_sample_IndexHNSWFlat_out
Average top n overlap: 98%
Average top n overlap, excluding first result: 98%
Median top n overlap: 100.0%
Median top n overlap, excluding first result: 100.0%

In this experiment I normalized the vectors to have a magnitude of 1 and switched to an inner product metric in IndexHNSWFlat to calculate cosine similarities

(base) jm3312@patent-clustering-faiss:~$ python3 score.py --ground_truth small_embedding_sample_IndexFlatIP_out --comparison small_embedding_sample_IndexHNSWFlat_out
Average top n overlap: 99%
Average top n overlap, excluding first result: 99%
Median top n overlap: 100.0%
Median top n overlap, excluding first result: 100.0%

jm3312@patent-clustering-faiss:~$ python3 score.py --ground_truth medium_embedding_sample_IndexFlatL2_out --comparison medium_embedding_sample_IndexIVFFlat_out
Average top n overlap: 57%
Average top n overlap, excluding first result: 52%
Median top n overlap: 60%
Median top n overlap, excluding first result: 56%
jm3312@patent-clustering-faiss:~$ python3 score.py --ground_truth medium_embedding_sample_IndexFlatL2_out --comparison medium_embedding_sample_IndexHNSWFlat_out
Average top n overlap: 98%
Average top n overlap, excluding first result: 98%
Median top n overlap: 100%
Median top n overlap, excluding first result: 100%

```
