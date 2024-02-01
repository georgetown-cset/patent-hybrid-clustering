# Experiments

# embedding_costs.py

The embedding_cost experiment is designed to be run on a VM, since it is testing how fast things work in that environment. The VM patent-hybrid-clustering is set up for this purpose, with CUDA and a GPU installed. However, in order to run correctly, you will have to modify your local environment variables.

We recommend putting the following in your .bashrc (you can edit this by using `vim ~/.bashrc` on the VM).

```
export PATH=/usr/local/cuda-12.3/bin${PATH:+:${PATH}}
export LD_LIBRARY_PATH=/usr/local/cuda-12.3/lib64\${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
export CUDA_VISIBLE_DEVICES=0
```
When this is done, either `source` your .bashrc or log out and log back in.

Next you should set up your must set up your virtual environment, then install [requirements.txt](../requirements.txt).

At this point, you can run:

```time python3 embedding_costs.py```

This will give you amount of time embedding takes.

