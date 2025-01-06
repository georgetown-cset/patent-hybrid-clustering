import argparse
import json
import os

import pycld2 as cld2
import regex
from google.cloud import translate_v2 as translate


class Translator:
    def __init__(self, output_file):
        self.translate_client = translate.Client()
        # let's limit ourselves to $500 worth of text
        # which should be more than enough based on our calculations
        self.max_chars_to_translate = 25 * 10**6
        self.patents = []
        # https://github.com/aboSamoor/polyglot/issues/71
        self.re_bad_chars = regex.compile(r"[\p{Cc}\p{Cs}]+")
        self.output_file = output_file
        self.num_chars_translated = 0

    def get_patents_to_translate(self, data_folder):
        data_files = os.listdir(os.path.join(data_folder, "input_data"))
        data_raw = []
        for file in data_files:
            with open(os.path.join(data_folder, "input_data/" + file), "r") as fil:
                json_list = list(fil)
                for row in json_list:
                    data_raw.append(json.loads(row))
                print(os.path.join(data_folder, "input_data/" + file))

        for i, row in enumerate(data_raw):
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

    def translate_text(self, text):
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

    def remove_bad_chars(self, text):
        return self.re_bad_chars.sub("", text)

    def validate_translation(self, text):
        isReliable, textBytesFound, details = cld2.detect(self.remove_bad_chars(text))
        language_found = details[0][0]
        if isReliable and language_found == "ENGLISH":
            return True
        return False

    def write_output(self):
        with open(self.output_file, "w") as fil:
            for patent in self.patents:
                fil.write(json.dumps(patent, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_folder", default="data")
    args = parser.parse_args()

    translator = Translator(args.data_folder + "/output_data/translated_patents.jsonl")
    translator.get_patents_to_translate(args.data_folder)
    translator.write_output()
