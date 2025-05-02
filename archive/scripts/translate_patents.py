import json

import pycld2 as cld2
import regex
from google.cloud import bigquery
from google.cloud import translate_v2 as translate
from typing import Optional


class Translator:
    def __init__(self, output_file: str) -> None:
        """
        Initialize a translator object
        :param output_file: The filename to write translations out to
        """
        self.bigquery_client = bigquery.Client()
        self.translate_client = translate.Client()
        # let's limit ourselves to $500 worth of text
        # which should be more than enough based on our calculations
        self.max_chars_to_translate = 25 * 10**6
        self.patents = []
        # https://github.com/aboSamoor/polyglot/issues/71
        self.re_bad_chars = regex.compile(r"[\p{Cc}\p{Cs}]+")
        self.output_file = output_file
        self.num_chars_translated = 0

    def get_patents_to_translate(self) -> None:
        """
        Get and set up translation of the patents we want to translate
        :return: None
        """
        query = """SELECT DISTINCT
                      patent_id,
                      family_id,
                      title_original,
                      abstract_original
                    FROM
                      staging_patent_clusters.patents_to_translate
                """
        result = self.bigquery_client.query(query)
        for i, row in enumerate(result):
            if i % 100 == 0:
                print(f"On patent {i}")
            translated_title = self.translate_text(row["title_original"])
            translated_abstract = self.translate_text(row["abstract_original"])
            if translated_abstract and self.validate_translation(translated_abstract):
                self.patents.append(
                    {
                        "patent_id": row["patent_id"],
                        "family_id": row["family_id"],
                        "title": translated_title,
                        "abstract": translated_abstract,
                        "title_original": row["title_original"],
                        "abstract_original": row["abstract_original"],
                    }
                )

    def translate_text(self, text: Optional[str]) -> Optional[str]:
        """
        Translate text to English
        :param text: The text to translate
        :return: the translated text if translated, None otherwise
        """
        if text:
            self.num_chars_translated += len(text)
            if self.num_chars_translated > self.max_chars_to_translate:
                raise ValueError(
                    f"Requested translation of over {self.max_chars_to_translate} "
                    f"characters, review input data."
                )
            result = self.translate_client.translate(text, target_language="en")
            return result["translatedText"]

    def remove_bad_chars(self, text: str) -> str:
        """
        Remove bad characters from text to help validate translation
        :param text:
        :return: Cleaned-up text
        """
        return self.re_bad_chars.sub("", text)

    def validate_translation(self, text: str) -> bool:
        """
        Validate that translation is reliable and returned English
        :param text: Text to validate
        :return: True if translated reliable, False if not
        """
        isReliable, textBytesFound, details = cld2.detect(self.remove_bad_chars(text))
        language_found = details[0][0]
        if isReliable and language_found == "ENGLISH":
            return True
        return False

    def write_output(self) -> None:
        """
        Write translated text to an output file
        :return: None
        """
        out = open(self.output_file, "w")
        for patent in self.patents:
            out.write(json.dumps(patent, ensure_ascii=False) + "\n")
        out.close()


def main():
    translate = Translator("../../data/translated_patents.jsonl")
    translate.get_patents_to_translate()
    translate.write_output()


if __name__ == "__main__":
    main()
