import json
import numpy as np
import pandas as pd
import os
from tqdm import tqdm
import requests


def write_resource_representation(domain="https://data.gov.sg", folder_slug="singapore", use_cache=True,
                                  endpoint_type="table"):
    # If the file already exists and we specify `use_cache=True`, simply return.
    resource_filename = "../../../data/" + folder_slug + "/resource lists/" + endpoint_type + ".json"
    preexisting = os.path.isfile(resource_filename)
    if preexisting and use_cache:
        return

    package_list_slug = "{0}/api/3/action/package_list".format(domain)
    package_list = requests.get(package_list_slug).json()

    if 'success' not in package_list or package_list['success'] != True:
        raise requests.RequestException("The CKAN catalog page did not resolve successfully.")

    resources = package_list['result']

    roi_repr = []
    for resource in resources:
        metadata = requests.get("{0}/api/3/action/package_metadata_show?id={1}".format(domain, resource)).json()

        license = metadata['result']['license']
        publisher = metadata['result']['publisher']['name']
        keywords = metadata['result']['keywords']
        description = metadata['result']['description']
        topics = metadata['result']['topics']
        name = metadata['result']['title']
        sources = metadata['result']['sources']
        update_frequency = metadata['result']['frequency']

        last_updated = str(pd.Timestamp(metadata['result']['last_updated']))

        available_formats = [m['format'].lower() for m in metadata['result']['resources']]
        canonical = metadata['result']['resources'][0]
        preferred_format = canonical['format'].lower()
        slug = canonical['url']

        landing_page = "{0}/dataset/{1}".format(domain, name)

        import pdb; pdb.set_trace()
        roi_repr.append({
            'id': {
                'landing_page': landing_page,
                'resource': slug,
                'protocol': 'https',
                'name': name,
                'description': description,
            },
            'provenance': {
                'publisher': publisher,
                'sources': sources
            },
            'usage': {
                'last_updated': last_updated,
                'update_frequency': update_frequency
            },
            'tags': {
                'tags_provided': keywords,
                'topics_provided': topics
            },
            'format': {
                'available_formats': available_formats,
                'preferred_format': preferred_format
            },
            'external': {
                'license': license
            },
            'flags': []
        })

    # Write to file and exit.
    with open(resource_filename, 'w') as fp:
        json.dump(roi_repr, fp, indent=4)
