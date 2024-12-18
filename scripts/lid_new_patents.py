import pycld2 as cld2
import regex
import argparse
import json
import os

def remove_bad_chars(text):
    RE_BAD_CHARS = regex.compile(r"[\p{Cc}\p{Cs}]+")
    return RE_BAD_CHARS.sub("", text)

def load_data(data_folder):
    data_files = os.listdir(os.path.join(data_folder,'input_data'))
    data_raw = []
    for file in data_files:
        with open(os.path.join(data_folder,'input_data/'+file), 'r') as fil:
            json_list = list(fil)
            for row in json_list:
                data_raw.append(json.loads(row))
    return data_raw

def lid(data):
    results = []
    for row in data:
        isReliable, textBytesFound, details = cld2.detect(remove_bad_chars(row['abstract_original']))
        results.append({'patent_id':row['patent_id'], 'family_id':row['family_id'], 'reliable': isReliable, 'language_long': details[0][0], 'language': details[0][1]})
    return results

def save_lid(data_folder, results):
    with open(os.path.join(data_folder,'output_data/lid.jsonl'),'w') as fil:
        for row in results:
            json.dump(row,fil)
            fil.write('\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_folder', default='data')
    args = parser.parse_args()
    print('loading data')
    data = load_data(args.data_folder)
    print('finding lid')
    results = lid(data)
    print('saving file')
    save_lid(args.data_folder,results)


