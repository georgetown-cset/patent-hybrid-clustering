from transformers import BertTokenizer, BertConfig,BertModel, LongformerModel, LongformerConfig, LongformerTokenizer
from google.cloud import bigquery
import pickle
from accelerate import init_empty_weights, load_checkpoint_and_dispatch, infer_auto_device_map, Accelerator
import torch
import os
import argparse



def get_test_embedding_set(patent_num: int):
    """
    We want an at least sort-of-representative test set of patents to embed.
    For this, useful stats to know -- as of January 19 2024:
    mean title length = ~55 (53 incl. non-English), mean abstract length = ~934 (872 incl. non-English)
    median title length = 49 (? incl. non-English, mean abstract length = 926 (872 incl. non-English)
    90th percentile title length = 95 (96 incl. non-English), 90th percentile abstract length = 1380 (1368 incl. non-English)
    10th percentile title length = 20 (17 incl. non-English), 10th percentile abstract length = 455 (300 incl. non-English)
    For now we are calculating with just title and abstract; if we decide to use descriptions or claims our
    cost estimates would change significantly.
    :return:
    """
    get_embedding_query = f"""SELECT
                              patent_id,
                              family_id,
                              title,
                              abstract,
                              title_original,
                              abstract_original,
                              language,
                            FROM (
                              SELECT
                                metadata.*,
                                ROW_NUMBER() OVER (ORDER BY LENGTH(COALESCE(title || abstract, title_original || abstract_original))) AS seqnum,
                                COUNT(*) OVER () AS cnt
                              FROM
                                unified_patents.metadata
                              WHERE
                                (title IS NOT NULL
                                  AND abstract IS NOT NULL)
                                OR (title_original IS NOT NULL
                                  AND abstract_original IS NOT NULL) )
                            WHERE
                              MOD(seqnum, CAST((cnt / {patent_num}) AS int64)) = 1
                              ORDER BY LENGTH(COALESCE(title || abstract, title_original || abstract_original))"""
    client = bigquery.Client()
    query_job = client.query(get_embedding_query)
    results = query_job.result()
    to_embed = []
    for result in results:
        to_embed.append({"patent_id": result["patent_id"],
                         "family_id": result["family_id"],
                         "title": result["title"],
                         "abstract": result["abstract"],
                         "title_original": result["title_original"],
                         "abstract_original": result["abstract_original"],
                         "language": result["language"]})
    return to_embed

def batch_patents(tokenizer, patents, batch_size):
    print("Making text to embed")
    title_abs = [(d.get("title") or d.get("title_original")) + tokenizer.sep_token
                 + (d.get('abstract') or d.get("abstract_original")) for d in patents]
    # matched = list(zip(title_abs, [d.get("patent_id") for d in patents]))
    batched = [[title_abs[i + j * batch_size] for i in range(batch_size) if (i + j * batch_size) < len(title_abs)]
               for j in range((len(title_abs) // batch_size) + 1)]
    # our method here leaves an empty array at the end when values are even; annoying
    if batched[-1] == []:
        batched = batched[:-1]
    return batched

def test_bert_model(patents, bert_model, batch_size):
    if not os.path.exists("offload"):
        os.mkdir("offload")
    # device = torch.device("cuda")
    print("Getting config")
    config = BertConfig.from_pretrained(bert_model)
    print("Building tokenizer")
    tokenizer = BertTokenizer.from_pretrained(bert_model)
    print("Building model")
    if not os.path.exists(f"save_{bert_model.replace('/', '_')}"):
        os.mkdir(f"save_{bert_model.replace('/', '_')}")
        model = BertModel(config)
        accelerator = Accelerator()
        accelerator.save_model(model=model, save_directory=f"save_{bert_model.replace('/', '_')}", max_shard_size="50MB")
    else:
        with init_empty_weights():
            model = BertModel(config)
    model.tie_weights()
    device_map = infer_auto_device_map(model, )
    model = load_checkpoint_and_dispatch(model, checkpoint=f"save_{bert_model.replace('/', '_')}", device_map="auto",
                                         max_memory={'mps': '50MB', 'cpu': '18000MB'}, offload_folder="offload")
    model = model.to_bettertransformer()
    batched = batch_patents(tokenizer, patents, batch_size=batch_size)
    print("Tokenizing")
    with torch.no_grad():
        for i, batch in enumerate(batched):
            inputs = tokenizer(batch, padding=True, truncation=True, return_tensors="pt", max_length=512)
            # print("Running model")

            result = model(**inputs)
            # print("Extracting embeddings")
            # take the first token in the batch as the embedding
            embeddings_batch = result.last_hidden_state[:, 0, :]
            if i == 0:
                embeddings = embeddings_batch
            else:
                embeddings = torch.cat((embeddings, embeddings_batch))
            torch.cuda.empty_cache()
        return embeddings

def test_longformer_model(patents, longformer_model, batch_size):
    print("Getting config")
    config = LongformerConfig.from_pretrained(longformer_model)
    print("Building tokenizer")
    tokenizer = LongformerTokenizer.from_pretrained(longformer_model)
    if not os.path.exists(f"save_{longformer_model.replace('/', '_')}"):
        os.mkdir(f"save_{longformer_model.replace('/', '_')}")
        model = LongformerModel(config)
        accelerator = Accelerator()
        accelerator.save_model(model=model, save_directory=f"save_{longformer_model.replace('/', '_')}",
                               max_shard_size="50MB")
    else:
        with init_empty_weights():
            model = LongformerModel(config)
    model = load_checkpoint_and_dispatch(model, checkpoint=f"save_{longformer_model.replace('/', '_')}",
                                         device_map="auto", max_memory={'mps': '50MB', 'cpu': '18000MB'},
                                         offload_folder="offload")
    batched = batch_patents(tokenizer, patents, batch_size=batch_size)
    print("Tokenizing")
    with torch.no_grad():
        for i, batch in enumerate(batched):
            # max_len = len(max(batch, key=len))
            # pad_to = (512 * (max_len // 512)) + 1
            inputs = tokenizer(batch, padding=True, truncation=True, return_tensors="pt", max_length=pad_to)
            # print("Running model")

            result = model(**inputs)
            # print("Extracting embeddings")
            # take the first token in the batch as the embedding
            embeddings_batch = result.last_hidden_state[:, 0, :]
            if i == 0:
                embeddings = embeddings_batch
            else:
                embeddings = torch.cat((embeddings, embeddings_batch))
            torch.cuda.empty_cache()
        return embeddings


def save_embeddings(patents, embedded):
    with open("../data/embeddings.pkl", "wb") as out:
        pickle.dump([{"patent_id": patent["patent_id"], "embeddings": embedded[i]} for i, patent in enumerate(patents)],
                    out, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("model", help="The model to run; current options are multilingual, patents, and longformer")
    parser.add_argument("patent_num", type=int)
    parser.add_argument("--batch_size", type=int, default=16, help="The batch size")
    args = parser.parse_args()
    if not args.patent_num:
        parser.print_help()
    print("Getting embedding set")
    data_to_embed = get_test_embedding_set(args.patent_num)
    if args.model == "multilingual":
        print("Running Multilingual BERT")
        embedded = test_bert_model(data_to_embed, "bert-base-multilingual-cased", args.batch_size)
    elif args.model == "patents":
        print("Running BERT for patents")
        embedded = test_bert_model(data_to_embed, "anferico/bert-for-patents", args.batch_size)
    elif args.model == "longformer":
        print("Running Longformer")
        embedded = test_longformer_model(data_to_embed, "allenai/longformer-base-4096", args.batch_size)
    else:
        parser.print_help()
    print("Saving embeddings to pickle")
    save_embeddings(data_to_embed, embedded)
