# FAISS experiments

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
