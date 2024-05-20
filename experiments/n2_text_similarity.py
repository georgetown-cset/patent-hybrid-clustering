"""
1. I loaded some embeddings into BQ:
  `bq load --replace --source_format NEWLINE_DELIMITED_JSON --autodetect
    tmp.brute_force_test_embeddings gs://patent-clustering/embedding_output/embedded_-0000* n2_text_similarity_input_schema.json`
2. I truncated to 1500 (wanted about 1M comparisons for my sample):
  `create or replace table tmp.brute_force_test_embeddings as select * from tmp.brute_force_test_embeddings limit 1500
3. I computed the unique combinations of the elements:
  ```
  create or replace table tmp.brute_force_embeddings_pairs as
  select
    embeddings1.family_id as family_id1,
    embeddings2.family_id as family_id2,
    embeddings1.text as text1,
    embeddings2.text as text2
  from
    tmp.brute_force_test_embeddings as embeddings1
  cross join
    tmp.brute_force_test_embeddings as embeddings2
  where embeddings1.family_id < embeddings2.family_id
  ```
  It's not obvious how to do this in a memory-efficient way in Beam based on what I've read so far, although if all
  else fails we could work around this in various hacky ways.
4. I exported the data as jsonl to `gs://jtm23/brute_force_embeddings_pairs/data*` and ran this script on it
5. The resulting job https://console.cloud.google.com/dataflow/jobs/us-east1/2024-05-20_13_48_24-11470885542461538291;step=;mainTab=JOB_GRAPH;graphView=0?project=gcp-cset-projects&pageState=(%22dfTime%22:(%22l%22:%22dfJobMaxTime%22))
   ran in 13 min 39 sec on 10 cpus (Beam can scale up far more than this). Using the pricing here:
   https://cloud.google.com/dataflow/pricing I get a cost of $0.056*1.235+$0.003557*4.633+$0.011*0.8065 (assuming
   our discount is exceeded) = $0.0945 to run similarity for 1,124,250 pairs. We will have to run this on ~80M^2/2
   pairs, so we would expect a cost of $0.0945*80M^2/(2*1,124,250) = ~$269M. Even if this is an overestimate
   by several orders of magnitude, we can't use this method on the full corpus.
"""

import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from sklearn.metrics.pairwise import cosine_similarity


class CosineSimilarity(beam.DoFn):
    def process(self, js):
        similarity = cosine_similarity([js["text1"]], [js["text2"]])[0][0]
        yield js["family_id1"], {"family_id": js["family_id2"], "similarity": similarity}
        yield js["family_id2"], {"family_id": js["family_id1"], "similarity": similarity}


def run(input_data: str, output_data: str,  pipeline_args: dict):
    """
    Generate text similarity of precomputed pairs of patent families, outputting top 10 most similar patent families
    for each
    :input_data: A GCS directory of JSONL inputs, with text1 and text2 columns corresponding to text embeddings for
      family_id1 and family_id2
    :output_data: Location on GCS where outputs should be written. The "top_10" column contains the top 10 most similar
      family ids to "family_id"
    :param pipeline_args: Args that control e.g. beam runner
    :return: None
    """
    options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read Data" >> beam.io.ReadFromText(input_data)
            | "JSONify" >> beam.Map(lambda x: json.loads(x))
            | "Cosine similarity" >> beam.ParDo(CosineSimilarity())
            | "Group by key" >> beam.GroupByKey()
            | "Top 10" >> beam.Map(lambda x: {"family_id": x[0], "top_10": sorted(x[1], key=lambda f: f["similarity"], reverse=True)[:10]})
            | "Stringify" >> beam.Map(lambda x: json.dumps(x))
            | "Write Data" >> beam.io.WriteToText(output_data)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_data", default="gs://jtm23/brute_force_embeddings_pairs/data*"
    )
    parser.add_argument(
        "--output_data", default="gs://jtm23/brute_force_embeddings_outputs/data"
    )
    args, pipeline_args = parser.parse_known_args()

    run(args.input_data, args.output_data, pipeline_args)