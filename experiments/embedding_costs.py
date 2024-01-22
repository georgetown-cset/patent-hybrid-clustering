from transformers import BertTokenizer, BertModel
from google.cloud import bigquery
import pickle

def get_test_embedding_set():
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
    get_embedding_query = """SELECT
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
                              MOD(seqnum, CAST((cnt / 500) AS int64)) = 1"""
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



def test_multilingual_bert(patents):
    print("Building tokenizer and model")
    tokenizer = BertTokenizer.from_pretrained('bert-base-multilingual-cased')
    model = BertModel.from_pretrained("bert-base-multilingual-cased").to("cuda")
    print("Making text to embed")
    title_abs = [(d.get("title") or d.get("title_original")) + tokenizer.sep_token
                 + (d.get('abstract') or d.get("abstract_original")) for d in patents]
    print("Tokenizing")
    inputs = tokenizer(title_abs, padding=True, truncation=True, return_tensors="pt", max_length=512)
    print("Running model")
    result = model(**inputs)
    print("Extracting embeddings")
    # take the first token in the batch as the embedding
    embeddings = result.last_hidden_state[:, 0, :]
    return embeddings

def save_embeddings(patents, embedded):
    with open("../data/embeddings.pkl", "wb") as out:
        pickle.dump([{"patent_id": patent["patent_id"], "embeddings": embedded[i]} for i, patent in enumerate(patents)],
                    out, protocol=pickle.HIGHEST_PROTOCOL)

if __name__ == "__main__":
    print("Getting embedding set")
    data_to_embed = get_test_embedding_set()
    print("Running Multilingual BERT")
    embedded = test_multilingual_bert(data_to_embed)
    print("Saving embeddings to pickle")
    save_embeddings(data_to_embed, embedded)