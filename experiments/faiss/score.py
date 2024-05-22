import argparse
import json
import os
import statistics


def get_scores(scores_dir: str) -> iter:
    top_n = {}
    top_n_1 = {}
    for fi in os.listdir(scores_dir):
        for line in open(os.path.join(scores_dir, fi)):
            js = json.loads(line)
            top_n[js["family_id"]] = set([s["family_id"] for s in js["most_similar"]])
            top_n_1[js["family_id"]] = set(
                [s["family_id"] for s in js["most_similar"][1:]]
            )
    return top_n, top_n_1


def get_overlap(ground_truth: dict, comparison: dict):
    overlap = []
    for fid in ground_truth:
        intersection = ground_truth[fid].intersection(comparison[fid])
        overlap.append(round(100 * float(len(intersection)) / len(ground_truth[fid])))
    return overlap


def run(ground_truth: str, comparison: str):
    """
    Calculates the average and median percentage of family ids in top-n
    similarity list in ground truth set that appear in comparison set, with
    and without including the first result which should be a self-match
    :param ground_truth: name of directory of jsonl containing ground truth
      similarity scores
    :param comparison: name of directory of jsonl containing similarity scores
      computed via a faster method
    :return: None
    """
    gt_top_n, gt_top_n_1 = get_scores(ground_truth)
    cmp_top_n, cmp_top_n_1 = get_scores(comparison)
    top_n_overlap = get_overlap(gt_top_n, cmp_top_n)
    top_n_1_overlap = get_overlap(gt_top_n_1, cmp_top_n_1)
    print(f"Average top n overlap: {round(sum(top_n_overlap)/len(top_n_overlap))}%")
    print(
        f"Average top n overlap, excluding first result: {round(sum(top_n_1_overlap)/len(top_n_1_overlap))}%"
    )
    print(f"Median top n overlap: {statistics.median(top_n_overlap)}%")
    print(
        f"Median top n overlap, excluding first result: {statistics.median(top_n_1_overlap)}%"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ground_truth")
    parser.add_argument("--comparison")
    args = parser.parse_args()

    run(args.ground_truth, args.comparison)
