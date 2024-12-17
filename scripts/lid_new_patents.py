import pycld2 as cld2
import regex
import argparse
import json
import os

RE_BAD_CHARS = regex.compile(r"[\p{Cc}\p{Cs}]+")

def remove_bad_chars(text):
    return RE_BAD_CHARS.sub("", text)

def load_data(data_folder):
    data_files = os.listdir(data_folder)
    data_raw = {}
    for file in data_files:
        with open(os.path.join(data_folder,file), 'r') as fil:
            json_list = list(fil)
            for row in json_list:
                data_raw.append(json.loads(row))
    return data_raw

def lid(data):
    results = []
    for row in data:


if __name__ == 'main':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_folder')
    args = parser.parse_args()

    data = load_data(args.data_folder)