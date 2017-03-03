import json
import pandas as pd
import os
from tqdm import tqdm
import requests


def write_resource_representation(domain="https://data.gov.sg", folder_slug="singapore", use_cache=True,
                                  endpoint_type="resources"):
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

    try:
        for resource in tqdm(resources):
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

            canonical = metadata['result']['resources'][0]
            preferred_format = canonical['format'].lower()
            slug = canonical['url']

            # Slug: "https://storage.data.gov.sg/3g-public-cellular-mobile-telephone-services/[...]"
            # We need: "3g-public-cellular-mobile-telephone-services"
            # Because landing page is: "https://data.gov.sg/dataset/3g-public-cellular-mobile-telephone-services"
            landing_page = "{0}/dataset/{1}".format(domain, slug.split("/")[3])

            # CKAN treats resources as resources. A single endpoint may host a few different datasets, differentiated
            # in the interface by a tab menu, or it may host the same dataset in multiple formats (in which case you
            # get a menu of options in the interface). The metadata export does not make it immediately obvious which
            # of the two is the case. Instead, we use the following heuristic to determine.

            # https://data.gov.sg/api/3/action/package_metadata_show?id=abc-waters-sites
            # A metdata export from the Singapore open data portal of a dataset with two formats available contains a
            # "resources" key, in which there exists a list of two dicts, a key of which is url. The two URLs are:
            # "https://geo.data.gov.sg/abcwaterssites/2016/10/28/kml/abcwaterssites.zip"
            # "https://geo.data.gov.sg/abcwaterssites/2016/10/28/shp/abcwaterssites.zip"

            # A metadata export from the Singapre open data portal of a dataset with two files available:
            # "https://storage.data.gov.sg/3g-public-cellular-mobile-telephone-services/resources/[long name 1].csv"
            # "https://storage.data.gov.sg/3g-public-cellular-mobile-telephone-services/resources/[long name 2].csv"

            # In the second case the names (stripping out the extension) are distinct. In the first case, they are not.
            # This is the heuristic we use to determine whether we have two exports of the same data, or two different
            # datasets proper.
            multiple_datasets = len(set([m['url'].split("/")[-1].split(".")[0]\
                                         for m in metadata['result']['resources']])) > 1

            if multiple_datasets:
                for dataset in metadata['result']['resources']:
                    roi_repr.append({
                        'id': {
                            'landing_page': landing_page,
                            'resource': dataset['url'],
                            'protocol': 'https',
                            'name': "{0} - {1}".format(name, dataset['title']),  # composite name.
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
                            'available_formats': [dataset['format'].lower()],
                            'preferred_format': dataset['format'].lower()
                        },
                        'external': {
                            'license': license
                        },
                        'flags': []
                    })

            else:
                available_formats = [m['format'].lower() for m in metadata['result']['resources']]

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

    finally:
        # Write to file and exit.
        with open(resource_filename, 'w') as fp:
            json.dump(roi_repr, fp, indent=4)
