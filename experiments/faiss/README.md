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
(base) jm3312@patent-clustering-faiss:~$ python3 score.py --ground_truth small_embedding_sample_out --comparison small_embedding_sample_IndexIVFFlat_out
Average top n overlap: 11%
Average top n overlap, excluding first result: 3%
Median top n overlap: 10.0%
Median top n overlap, excluding first result: 0.0%
(base) jm3312@patent-clustering-faiss:~$ python3 score.py --ground_truth small_embedding_sample_out --comparison small_embedding_sample_IndexIVFFlat_out_10probes
Average top n overlap: 12%
Average top n overlap, excluding first result: 4%
Median top n overlap: 10.0%
Median top n overlap, excluding first result: 0.0%
(base) jm3312@patent-clustering-faiss:~$ python3 score.py --ground_truth medium_embedding_sample_out --comparison medium_embedding_sample_IndexIVFFlat_out
Average top n overlap: 9%
Average top n overlap, excluding first result: 2%
Median top n overlap: 10%
Median top n overlap, excluding first result: 0%
```

I'm not yet clear on how to choose the best number of cells. I should also try to run the test on the artificial
dataset in the tutorial.
