"""
Adapted from https://cloud.google.com/dataflow/docs/notebooks/huggingface_text_embeddings
"""

import argparse
import tempfile
import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.embeddings.huggingface import SentenceTransformerEmbeddings


content = [
    {'x': 'How do I get a replacement Medicare card?'},
    {'x': 'What is the monthly premium for Medicare Part B?'},
    {'x': 'How do I terminate my Medicare Part B (medical insurance)?'},
    {'x': 'How do I sign up for Medicare?'},
    {'x': 'Can I sign up for Medicare Part B if I am working and have health insurance through an employer?'},
    {'x': 'How do I sign up for Medicare Part B if I already have Part A?'},
    {'x': 'What are Medicare late enrollment penalties?'},
    {'x': 'What is Medicare and who can get it?'},
    {'x': 'How can I get help with my Medicare Part A and Part B premiums?'},
    {'x': 'What are the different parts of Medicare?'},
    {'x': 'Will my Medicare premiums be higher because of my higher income?'},
    {'x': 'What is TRICARE ?'},
    {'x': "Should I sign up for Medicare Part B if I have Veterans' Benefits?"}
]


def run(model_name: str):
    artifact_location_t5 = tempfile.mkdtemp(prefix='huggingface_')
    embedding_transform = SentenceTransformerEmbeddings(model_name=model_name, columns=['x'])
    with beam.Pipeline() as pipeline:
        data_pcoll = (
                pipeline
                | "CreateData" >> beam.Create(content))
        transformed_pcoll = (
                data_pcoll
                | "MLTransform" >> MLTransform(write_artifact_location=artifact_location_t5).with_transform(
            embedding_transform))

        transformed_pcoll | 'LogOutput' >> beam.Map(print)

        transformed_pcoll | "PrintEmbeddingShape" >> beam.Map(lambda x: print(f"Embedding shape: {len(x['x'])}"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
    args = parser.parse_args()

    run(args.model)