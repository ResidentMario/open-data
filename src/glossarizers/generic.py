"""
Generic IO methods which are common across all glossarizers, but not important enough to need their own package.
"""

import os
import json


def preexisting_cache(folder_filepath, use_cache):
    # If the file already exists and we specify `use_cache=True`, simply return.
    preexisting = os.path.isfile(folder_filepath)
    if preexisting and use_cache:
        return


def write_resource_file(roi_repr, resource_filename):
    with open(resource_filename, 'w') as fp:
        json.dump(roi_repr, fp, indent=4)


def write_glossary_file(glossary_repr, glossary_filename):
    with open(glossary_filename, "w") as fp:
        json.dump(glossary_repr, fp, indent=4)


def load_glossary_todo(resource_filename, glossary_filename, use_cache=True):
    # Begin by loading in the data that we have.
    with open(resource_filename, "r") as fp:
        resource_list = json.load(fp)

    # If use_cache is True, remove resources which have already been processed. Otherwise, only exclude "ignore" flags.
    # Note: "removed" flags are not ignored. It's not too expensive to check whether or not this was a fluke or if the
    # dataset is back up or not.
    if use_cache:
        resource_list = [r for r in resource_list if "processed" not in r['flags'] and "ignore" not in r['flags']]
    else:
        resource_list = [r for r in resource_list if "ignore" not in r['flags']]

    # Check whether or not the glossary file exists.
    preexisting = os.path.isfile(glossary_filename)

    # If it does, load it. Otherwise, load an empty list.
    if preexisting:
        with open(glossary_filename, "r") as fp:
            glossary = json.load(fp)
    else:
        glossary = []

    return resource_list, glossary
