import argparse
from google.cloud import bigquery
import numpy as np
import igraph as ig
import time
import csv
import pandas as pd
import multiprocessing as mp

def find_edge_graph(table: str):
    client = bigquery.Client(project='gcp-cset-projects')

    cluster_edge_query = f"""
    SELECT
      *
    FROM {table}
    WHERE link_rank <= 10 
    """

    result = client.query(cluster_edge_query)
    cluster_citations = []
    for row in result:
        temp_dict = {'cluster_id': row['family_cluster'], 'ref_id': row['ref_cluster'], 'weight': row['weight']}
        cluster_citations.append(temp_dict)

    citation_edges = [[row['cluster_id'], row['ref_id']] for row in cluster_citations]
    Gm = ig.Graph(edges=citation_edges,
                           edge_attrs={'weight': [row['weight'] for row in cluster_citations]})

    return Gm
def layout_i(i: int, Gm) -> None:
    """
    :param i:
    :param Gm: igraph object with edges and nodes of hybrid graph
    """
    # get layout
    coords = Gm.layout(layout = 'drl', weights='weight')
    x,y = np.array(coords.coords).T
    np.savetxt(f"layouts/x{i}.txt", x)
    np.savetxt(f"layouts/y{i}.txt", y)

def run_multi_layout(n_layouts: int, Gm) -> None:
    """
    :param n_layouts: number of layouts to compute
    :param Gm: igraph object with the edges and nodes of the hybrid graph
    :return:
    """
    for i in range(n_layouts):
        layout_i(i, Gm)

    """processes = []
    for i in range(n_layouts):
        p = mp.Process(target = layout_i, args=(i, Gm))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()"""

def map_similarity_calc(n_layouts: int, j: int, data_dict: dict) -> None:
    r = np.array([])
    for i in range(0, n_layouts):
        if i != j:
            x1 = data_dict[f"x{i}"]
            x2 = data_dict[f"x{j}"]
            y1 = data_dict[f"y{i}"]
            y2 = data_dict[f"y{j}"]

            #grand-mean scaling the coordinates so they are comparable, making them all positive to not care about
            #sign-flipping
            x1 = abs((x1 - np.mean(x1)) / np.std(x1))
            x2 = abs((x2 - np.mean(x2)) / np.std(x2))
            y1 = abs((y1 - np.mean(y1)) / np.std(y1))
            y2 = abs((y2 - np.mean(y2)) / np.std(y2))

            dist = np.average(np.sqrt((x1-x2)**2 + (y1-y2)**2))
            r = np.append(r,[dist],axis=0)
    mean_dist = np.mean(r)
    with open("layouts/map_distance.csv","a") as f:
        writer = csv.writer(f)
        writer.writerow([j, mean_dist])

def run_multi_map_similarity_calc(n_layouts: int, data_dict: dict) -> None:
    for j in range(n_layouts):
        map_similarity_calc(n_layouts, j, data_dict)

def find_best_layout(nodes, n_layouts:int) -> None:
    data_dict = {}
    for j in range(n_layouts):
        for k in ["x","y"]:
            data_dict[f"{k}{j}"] = np.loadtxt(f"layouts/{k}{j}.txt")
    with open("layouts/map_distance.csv","w") as f:
        writer = csv.writer(f)
        writer.writerow(["map","dist"])
    run_multi_map_similarity_calc(n_layouts, data_dict)
    sim_arry = pd.read_csv("layouts/map_distance.csv")

    min_distance = sim_arry["dist"].min()
    best_map_number = sim_arry.loc[sim_arry["dist"] == min_distance, "map"].values[0]

    x_best = np.loadtxt(f"layouts/x{best_map_number}.txt")
    y_best = np.loadtxt(f"layouts/y{best_map_number}.txt")

    df = pd.DataFrame(
        data=np.column_stack((nodes, x_best, y_best)), columns=["cluster_id", "x", "y"]
    )

    df["cluster_id"] = df["cluster_id"].astype(int)  # convert cluster_id to integer
    df.to_csv("layouts/clust_locations.csv", index=False)

    np.savetxt(f"layouts/x_best.txt", x_best)
    np.savetxt(f"layouts/y_best.txt", y_best)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('bq_table')
    parser.add_argument('n_layouts', type=int)
    args = parser.parse_args()

    t1 = time.time()
    g_citations = find_edge_graph(args.bq_table)
    t2 = time.time()
    print("Time to find edges: ", t2-t2, " seconds")

    t3 = time.time()
    run_multi_layout(args.n_layouts, g_citations)
    t4 = time.time()
    print(f"Time for {args.n_layouts}: ", t4-t3, " seconds")

    nodes = np.array(g_citations.vs['name'])

    find_best_layout(nodes, args.n_layouts)

    print("\n Total Time: ", t4-t1, " seconds")
