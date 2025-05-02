import csv

import pandas as pd
import pycld2 as cld2
import regex
from alive_progress import alive_bar
from google.cloud import bigquery

# https://github.com/aboSamoor/polyglot/issues/71
RE_BAD_CHARS = regex.compile(r"[\p{Cc}\p{Cs}]+")

client = bigquery.Client()
table_id = "gcp-cset-projects.staging_patent_clusters.patents_lid"


def remove_bad_chars(text: str) -> str:
    """
    Removes bad characters from text
    :param text: Text to clean up
    :return: Cleaned-up text
    """
    return RE_BAD_CHARS.sub("", text)


def connect_data():
    """
    Get data to LID
    :return: Result of query
    """
    QUERY = """
        SELECT
            patent_id,
            title_original,
            abstract_original
        FROM `staging_patent_clusters.metadata_d_p_removed`
        WHERE
            title is null
            and abstract is null
            and title_original is not null
            and abstract_original is not null
        """

    query_job = client.query(QUERY)  # API request
    return query_job.result()


def translate(rows) -> list:
    """
    Run LID
    :param rows: The rows of data to LID
    :return: The LID results as a list
    """
    total_count = 11335511
    results = []
    with alive_bar(total_count) as bar:
        for row in rows:
            isReliable, textBytesFound, details = cld2.detect(
                remove_bad_chars(row["abstract_original"])
            )
            results.append([row["patent_id"], isReliable, details[0]])
            bar()
    return results


def write_results(results_frame: list) -> None:
    """
    Write the results out
    :param results_frame: The LID results
    :return: None
    """
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("patent_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("reliable", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("details", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("language_long", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("language", bigquery.enums.SqlTypeNames.STRING),
        ]
    )

    job = client.load_table_from_dataframe(
        results_frame, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


if __name__ == "__main__":
    rows = connect_data()
    results = translate(rows)
    results_frame = pd.DataFrame(results, columns=["patent_id", "reliable", "details"])
    for index, row in results_frame.iterrows():
        details = row["details"]
        results_frame.loc[index, "language_long"] = details[0]
        results_frame.loc[index, "language"] = details[1]
    results_frame = results_frame.astype(
        {
            "patent_id": str,
            "reliable": str,
            "details": str,
            "language_long": str,
            "language": str,
        }
    )
    write_results(results_frame)
