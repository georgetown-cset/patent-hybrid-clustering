# FAISS experiments

## Summary of time to search from `profiling`

| Dataset size | Method | Time to search |
| ------------ | ------ |----------------|
| 242K | IndexFlatIP | 29.5s          |
| 242K | IndexIVFFlat | 11.4s          |
| 475K | IndexFlatIP | 113.7s         |
| 475K | IndexIVFFlat | 56.8s          |

## Search accuracy

Pretty poor so far for IndexIVFFlat. Includes an experiment where I increased the number of probes to 10, which didn't have much effect.

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

I'm not yet clear on how to choose the best number of cells. I should also try to run the test on the artificial
dataset in the tutorial.
