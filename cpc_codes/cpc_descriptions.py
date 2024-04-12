from google.cloud import bigquery
import xml.etree.ElementTree as ET
import os
import argparse
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json

"""
This script parses the xml files for cpc codes downloaded from https://www.cooperativepatentclassification.org/cpcSchemeAndDefinitions/bulk
Considered are files from "Complete CPC Definitions in XML format"

To extract the files, make sure to:
(1) Downlaoad and unzip the required xml data files.
(2) Make sure the table you want to create in BQ isn't the exact same between runs (or at least give it a 2h break between creating and overwriting the same table)
(3) Run the script.

"""

def find_cpc_files(xml_dir):
    """
    Function which finds all the xml files in a directory and returns a list of them for use in parsing later.
    :param :xml_dir: String for the directory of interest
    :return: list of strings representing the files
    """
    print('Finding files')
    xml_files = os.listdir('../cpc_codes/'+xml_dir)
    xml_files.sort()
    xml_files = [fil for fil in xml_files if fil.endswith(".xml")]
    return(xml_files)

def parse_xml_descritption_file(xml_dir, xml_file):
    """
    Reads in an xml file and returns a list of dictionary items, with the cpc code and corresponding DESCRIPTION.
    :param xml_dir: String type, directory where the xml files are
    :param xml_file: String type, name of the actual file
    :return: List of dictionaries, where each item in the list is a dictionary with the cpc code and corresponding DESCRIPTION
    """
    tree = ET.parse(xml_dir + '/' + xml_file) #parse the xml file
    root = tree.getroot() #get the xml tree
    code_descriptions = []
    for item in root.findall('.//definition-item'): #look at each cpc item in the file
        for symbol in item.findall('classification-symbol'): #find the cpc name
            code = symbol.text
        description = ''
        for text in item.findall('definition-title'): #find the cpc description
            description += text.text
            for child in text: #for annoying reasons there are often links to other codes, in the form of nested children
                if child.text is not None:
                    description += child.text
                if child.tail is not None:
                    description += child.tail
        code_descriptions.append({'code':code, 'description':description})
    return code_descriptions

def parse_xml_title_file(xml_dir, xml_file):
    """
    Reads in an xml file and returns a list of dictionary items, with the cpc code and corresponding TITLES.
    :param xml_dir: String type, directory where the xml files are
    :param xml_file: String type, name of the actual file
    :return: List of dictionaries, where each item in the list is a dictionary with the cpc code and corresponding TITLE
    """
    tree = ET.parse(xml_dir + '/' + xml_file) #parse the xml file
    root = tree.getroot() #get the xml tree

    code_titles = []

    for item in root.findall('.//classification-item'):
        code = ''
        for symbol in item.findall('classification-symbol'):
            code = symbol.text
        title = ''
        for text in item.findall('class-title'): #find the cpc description
            for title_part in text.findall('title-part'):
                for sub_text in title_part.findall('text'):
                    if sub_text.text is not None:
                        title += sub_text.text + '; '
                    for emph in sub_text.findall('u'):
                        if emph.text is not None:
                            title += emph.text + '; '
                for cpc_specific_text in title_part.findall('CPC-specific-text'):
                    for sub_text in cpc_specific_text.findall('text'):
                        if sub_text.text is not None:
                            title += sub_text.text + '; '
        code_titles.append({'code':code, 'title':title.rstrip('; ')})
    return code_titles

def get_cpc_descriptions(xml_directory):
    xml_files = find_cpc_files(xml_directory)
    code_descriptions = []
    print('Loading description files')
    for fil in xml_files:
        code_descriptions.extend(parse_xml_descritption_file(xml_directory, fil))
    return code_descriptions

def get_cpc_titles(xml_directory):
    xml_files = find_cpc_files(xml_directory)
    code_titles = []
    print('Loading title files')
    for fil in xml_files:
        code_titles.extend(parse_xml_title_file(xml_directory, fil))
    return code_titles

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("title_directory")
    parser.add_argument("description_directory")
    parser.add_argument("local_output_file")
    parser.add_argument("bq_table")
    args = parser.parse_args()

    print('Finding code titles')
    code_titles = get_cpc_titles(args.title_directory)

    print('Finding code descriptions')
    code_descriptions = get_cpc_descriptions(args.description_directory)

    cpc_title_dict = {}
    for row in code_titles:
        code = row['code']
        title = row['title']
        if code in cpc_title_dict.keys():
            if cpc_title_dict[code] != '':
                cpc_title_dict[code] += '; ' + title
            else:
                cpc_title_dict[code] = title
        else:
            cpc_title_dict[code] = title

    cpc_description_dict = {}
    for row in code_descriptions:
        cpc_description_dict[row['code']] = row['description']

    all_cpc_codes = {k: {'title': cpc_title_dict[k], 'description': None} for k in cpc_title_dict.keys()}
    for key in cpc_description_dict.keys():
        code = key
        description = cpc_description_dict[key]
        try:
            all_cpc_codes[key]['description'] = description
        except KeyError:
            all_cpc_codes[key] = {'title': None, 'description': description}

    all_cpc_codes_json = [{'code':k, 'title': all_cpc_codes[k]['title'], 'description': all_cpc_codes[k]['description']} for k in all_cpc_codes.keys()]

    print('Saving json file at: ' + args.local_output_file + '.jsonl')

    with open(args.local_output_file + '.jsonl', 'w') as f:
        for d in all_cpc_codes_json:
            json.dump(d, f)
            f.write('\n')

    print('Saving results to BQ')

    client = bigquery.Client()

    schema = [
        bigquery.SchemaField("code", "STRING", mode='REQUIRED', description="CPC code"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE", description='Title of CPC code'),
        bigquery.SchemaField("description", 'STRING', mode='NULLABLE', description="Full description of CPC code")
    ]

    table_id = 'gcp-cset-projects.' + args.bq_table

    try:
        table = client.delete_table(table_id)
    except NotFound:
        pass

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)

    errors = client.insert_rows_json(table_id, all_cpc_codes_json)
    if errors == []:
        print('Done!')
    else:
        print('Errors from inserting rows: {}'.format(errors))

