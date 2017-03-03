"""
Generic IO methods which are common across all glossarizers, but not important enough to need their own package.
"""

import os
import json


def return_if_preexisting_and_use_cache(folder_slug, endpoint_type, use_cache):
    # If the file already exists and we specify `use_cache=True`, simply return.
    resource_filename = "../../../data/" + folder_slug + "/resource lists/" + endpoint_type + ".json"
    preexisting = os.path.isfile(resource_filename)
    if preexisting and use_cache:
        return


def write_resource_file(folder_slug, endpoint_type, roi_repr):
    resource_filename = "../../../data/" + folder_slug + "/resource lists/" + endpoint_type + ".json"
    with open(resource_filename, 'w') as fp:
        json.dump(roi_repr, fp, indent=4)


def write_glossary_file(folder_slug, endpoint_type, glossary_repr):
    glossary_filename = "../../../data/" + folder_slug + "/glossaries/" + endpoint_type + ".json"
    with open(glossary_filename, "w") as fp:
        json.dump(glossary_repr, fp, indent=4)


def load_glossary_todo(folder_slug, endpoint_type, use_cache):
    # Begin by loading in the data that we have.
    resource_filename = "../../../data/" + folder_slug + "/resource lists/" + endpoint_type + ".json"
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
    glossary_filename = "../../../data/" + folder_slug + "/glossaries/" + endpoint_type + ".json"
    preexisting = os.path.isfile(glossary_filename)

    # If it does, load it. Otherwise, load an empty list.
    if preexisting:
        with open(glossary_filename, "r") as fp:
            glossary = json.load(fp)
    else:
        glossary = []

    return resource_list, resource_filename, glossary, glossary_filename


write_resource_representation_docstring = """
Fetches a resource representation for a single resource type from a Socrata portal.

Parameters
----------
domain: str, default "data.cityofnewyork.us"
    The open data portal URI.
folder_slug: str, default "nyc"
    The subfolder of the "data" directory into which the resource representation will be placed.
use_cache: bool, default True
    If a resource representation already exists, whether to simply exit out or blow it away and create a new one
    (overwriting the old one).
credentials: str or dict, default "../../auth/nyc-open-data.json"
    Either a filepath to the file containing your API credentials for the given Socrata instance, or a dictionary
    containing the same information.
endpoint_type: str, default "table"
    The resource type to fetch a representation for.

Returns
-------
Nothing; writes to a file.
"""

write_glossary_docstring = """
Writes a dataset representation. This is the hard part!

Parameters
----------
domain: str, default "data.cityofnewyork.us"
    The open data portal URI.
folder_slug: str, default "nyc"
    The subfolder of the "data" directory into which the resource glossary will be placed.
use_cache: bool, default True
    If a glossary already exists, whether to simply exit out or blow it away and create a new one (overwriting the
    old one).
endpoint_type: str, default "table"
    The resource type to build a glossary for.
timeout: int, default 60
    The maximum amount of time to spend downloading data before killing the process. This is implemented so that
    occassional very large datasets do not crash the process.

Returns
-------
Nothing; writes to a file.
"""