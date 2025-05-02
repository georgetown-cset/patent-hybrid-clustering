"""
Adapted from https://cloud.google.com/dataflow/docs/notebooks/huggingface_text_embeddings

Sample Dataflow params:
python3 get_embeddings.py --project gcp-cset-projects --runner DataflowRunner --disk_size_gb 30
  --job_name patent-embeddings-test --save_main_session --region us-east1
  --temp_location gs://cset-dataflow-test/example-tmps/ --requirements_file get_embeddings_requirements.txt
"""

import argparse
import json
import tempfile

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.embeddings.huggingface import (
    SentenceTransformerEmbeddings,
)
from apache_beam.options.pipeline_options import PipelineOptions


def run(
    input_data: str, output_data: str, model_name: str, pipeline_args: dict
) -> None:
    """
    Use the specified model to generate embeddings for `input_data`, and write them to `output_data`.
    The default input directory contains the results of this query, exported to JSONL

    create or replace table tmp.patent_embeding_test_input as
    select distinct
      family_id,
      CONCAT(COALESCE(title, ""), " ", COALESCE(abstract, "")) as text
    from
      unified_patents.metadata
    where family_id is not null
    limit 10000

    :input_data: A GCS directory of JSONL inputs, with a "text" column we want to extract embeddings from
    :output_data: Location on GCS where outputs should be written. The "text" column will contain the embeddings
      of the input "text" column
    :param model_name: Name of the sentence transformers model to use
    :param pipeline_args: Args that control e.g. beam runner
    :return: None
    """
    options = PipelineOptions(pipeline_args)
    artifact_location_t5 = tempfile.mkdtemp(prefix="huggingface_")
    embedding_transform = SentenceTransformerEmbeddings(
        model_name=model_name, columns=["text"]
    )
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read Data" >> beam.io.ReadFromText(input_data)
            | "JSONify" >> beam.Map(lambda x: json.loads(x))
            | "MLTransform"
            >> MLTransform(write_artifact_location=artifact_location_t5).with_transform(
                embedding_transform
            )
            | "Stringify" >> beam.Map(lambda x: json.dumps(x))
            | "Write Data" >> beam.io.WriteToText(output_data)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_data",
        default="gs://airflow-data-exchange/tmp/cpc_embedding/to_embed_*",
    )
    parser.add_argument(
        "--output_data", default="gs://patent-clustering/cpc_embedding_output/embedded"
    )
    parser.add_argument("--model", default="sentence-transformers/all-mpnet-base-v2")
    # parser.add_argument(
    #     "--model", default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    # )
    args, pipeline_args = parser.parse_known_args()

    run(args.input_data, args.output_data, args.model, pipeline_args)
