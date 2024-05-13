import argparse
import copy
import json
import os
import xml.etree.ElementTree as ET
from collections import defaultdict

"""
This script parses the xml files for cpc codes downloaded from https://www.cooperativepatentclassification.org/cpcSchemeAndDefinitions/bulk
Considered are files from "Complete CPC Definitions in XML format"

To extract the files, make sure to:
(1) Downlaoad and unzip the required xml data files.
(2) Make sure the table you want to create in BQ isn't the exact same between runs (or at least give it a 2h break between creating and overwriting the same table)
(3) Run the script.

"""


def find_cpc_files(xml_dir: str):
    """
    Function which finds all the xml files in a directory and returns a list of them for use in parsing later.
    :param :xml_dir: String for the directory of interest
    :return: list of strings representing the files
    """
    print("Finding files")
    xml_files = os.listdir("../cpc_codes/" + xml_dir)
    xml_files = [fil for fil in xml_files if fil.endswith(".xml")]
    xml_files.sort()
    return xml_files


def parse_xml_title_file(
    xml_dir: str, xml_file: str, levels: dict, hierarchy: defaultdict, code_titles: dict
):
    """
    Reads in an xml file and returns a list of dictionary items, with the cpc code and corresponding TITLES.
    :param xml_dir: String type, directory where the xml files are
    :param xml_file: String type, name of the actual file
    :param levels: Dict type, the most recent code at each hierarchy level
    :param: hierarchy: Default dict of list type, a mapping of each parent code to all of its child codes
    :param: code_titles: Dict type, the code linked to the lowest-level text of that code
    :return: List of any codes at the base level of the hierarchy
    """
    initial_level = 1
    tree = ET.parse(xml_dir + "/" + xml_file)  # parse the xml file
    root = tree.getroot()  # get the xml tree
    base_levels = []

    for i, item in enumerate(root.findall(".//classification-item")):
        code = ""
        level = int(item.attrib["level"])
        for symbol in item.findall("classification-symbol"):
            code = symbol.text
        if i == 0:
            initial_level = level
            if initial_level == 2:
                base_levels.append((code, level))
        if level > initial_level:
            parent_code = levels[level - 1]
            hierarchy[(parent_code, level - 1)].append((code, level))
        levels[level] = code
        title = ""
        for text in item.findall("class-title"):  # find the cpc description
            for title_part in text.findall("title-part"):
                for ref in title_part.findall("reference"):
                    pass
                for sub_text in title_part.findall("text"):
                    if sub_text.text is not None:
                        title += sub_text.text + "; "
                for cpc_specific_text in title_part.findall("CPC-specific-text"):
                    for cpc_ref in cpc_specific_text.findall("reference"):
                        pass
                    for sub_text in cpc_specific_text.findall("text"):
                        if sub_text.text is not None:
                            title += sub_text.text + "; "
        code_titles[(code, level)] = title.rstrip("; ")
    return base_levels


def combine_xml_titles(
    current_level: list,
    level_index: int,
    text_dict: dict,
    final_text: dict,
    previous_levels: dict,
    hierarchy: defaultdict,
    code_titles: dict,
):
    """
    Recursive function to combine the hierarchical CPC titles across the levels
    :param current_level: List type, all the current codes at the level we're evaluating
    :param level_index: Integer type, index of where we are in the list we're evaluating
    :param text_dict: Dictionary type, maps codes to their gradually expanding hierarchical text pair
    :param final_text: Dictionary type, maps codes to what we want to write out in our jsonl file
    :param previous_levels: Dictionary type, maps codes at the current level to codes at the previous level
    :param hierarchy: Dictionary type, maps codes at the current level to a list of codes at the next level
    :param code_titles: Dictionary type, maps codes to their non-hierarchical text pair
    :return: None
    """
    while current_level:
        for i in range(level_index, len(current_level)):
            code_tuple = current_level[i]
            text = f"{text_dict[previous_levels[code_tuple]]}; {code_titles[code_tuple]}".strip(
                "; "
            )
            final_text[code_tuple] = {
                "code": code_tuple[0],
                "text": text,
                "level": code_tuple[1],
            }
            text_dict[code_tuple] = text
            if code_tuple not in hierarchy:
                return None
            else:
                next_level = hierarchy[code_tuple]
                previous_levels.update(
                    {code_string: code_tuple for code_string in next_level}
                )
                for next_level_index, current_tuple in enumerate(next_level):
                    combine_xml_titles(
                        next_level,
                        next_level_index,
                        text_dict,
                        final_text,
                        previous_levels,
                        hierarchy,
                        code_titles,
                    )
        return


def setup_combine_xml_titles(
    hierarchy: defaultdict, code_titles: dict, current_level: list
):
    """
    Do setup for combining xml titles; basically, do everything that's needed before jumping into
    recursion
    :param hierarchy: Dictionary type, maps codes at the current level to a list of codes at the next level
    :param code_titles: Dictionary type, maps codes to their non-hierarchical text pair
    :param current_level: List type, all the current codes at the top level of the hierarchy
    :return: Dictionary type, maps codes to the jsonl text to print out
    """
    final_text = {}
    text_dict = {}
    previous_levels = {}
    for base_code in current_level:
        previous_levels[base_code] = "0"
    text_dict["0"] = ""
    combine_xml_titles(
        current_level, 0, text_dict, final_text, previous_levels, hierarchy, code_titles
    )
    return final_text


def get_cpc_titles(xml_directory: str):
    """
    Run all code to get CPC titles
    First we parse the title file and build a hierarchy
    Then we recurse through the hierarchy to build out our text
    :param xml_directory: String type, the directory where the CPC title XML lives
    :return: Dictionary type, maps codes to the jsonl text to print out
    """
    xml_files = find_cpc_files(xml_directory)
    code_titles = {}
    print("Loading title files")
    levels = {}
    hierarchy = defaultdict(list)
    initial_levels = []
    for fil in xml_files:
        top_levels = parse_xml_title_file(
            xml_directory, fil, levels, hierarchy, code_titles
        )
        if top_levels:
            initial_levels.extend(top_levels)
    initial_levels = list(set(initial_levels))
    return setup_combine_xml_titles(hierarchy, code_titles, initial_levels)


def save_data(filename: str, data: dict):
    """
    Save the jsonl data to file
    :param filename: String type, filename to write output to
    :param data: Dictionary type, maps codes to the jsonl text to print out
    :return:
    """
    print("Saving json file at: " + filename + ".jsonl")

    with open(filename + ".jsonl", "w") as f:
        for d in data:
            json.dump(data[d], f)
            f.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("title_directory")
    parser.add_argument("local_output_file")
    args = parser.parse_args()

    print("Finding code titles")
    code_titles = get_cpc_titles(args.title_directory)

    save_data(args.local_output_file, code_titles)
