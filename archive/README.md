This folder stores all the experiments and files created in the development of the hybrid patent clusters.

There are three categories of work here:
1. Work used in determining which techniques or models should be incorporated into the final clusters. This often includes code for techniques or models that weren't ultimately incorporated, but which we tested for this purpose. It also includes cost analyses, comparisons of various techniques to determine the best option, and similar.
2. Work used to build our initial clustering. This is in the archive because we reserve the core repository for the clustering update code -- as this is what is actively in use. However, this is important to retain so that we can recluster in the future.
3. Work used to analyze our clustering and evaluate its quality (including visualizing the clustering). This is key code to understand how we evaluate the clustering, but is not needed for the actual cluster update process.

What is included:

###[cpc_codes](cpc_codes)
Folder for how we extracted the descriptive text of the current cpc/ipc codes for patents and linked it to their codes, along with the associated text.

###[embedding_experiments](embedding_experiments)

Folder containing experiments for finding embeddings with different models, including computing costs.

###[evaluation_metrics](evaluation_metrics)

Some of the small scale experiments and testing, as well as code for evaluation of results of some of the large scale experiments.

###[faiss](faiss)

Implementing the FAISS algorithm, and updating the FAISS dag.

###[scripts](scripts)

Some scripts we used to build the original clustering, that have been updated to different but similar scripts for the regular clustering updates.

###[sql](sql)

[sql/initial_map](sql/initial_map): SQL scripts used to build or evaluate the original map or analyze raw patent data in preparation for its creation.

[sql/metrics](sql/metrics): Old sql scripts used for extracting evaluation metrics during development.

###[visualizations](visualizations)

Folder for generating optimal cluster layout for visualization

There is also a [requirements.txt](requirements.txt) for requirements needed in this directory but not in the main code. It can be installed with:

`pip install -r requirements.txt`

But should be installed after installing the primary [requirements.txt](../requirements.txt) in the main directory.