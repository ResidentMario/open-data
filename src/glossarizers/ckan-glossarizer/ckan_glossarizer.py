import json
import pandas as pd
import os
from tqdm import tqdm
import requests


def write_resource_representation(domain="data.gov.sg", folder_slug="singapore", use_cache=True,
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
            metadata = requests.get("https://{0}/api/3/action/package_metadata_show?id={1}".format(domain,
                                                                                                   resource)).json()

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


def write_glossary(domain="data.gov.sg", folder_slug="singapore", endpoint_type="everything", use_cache=True,
                   timeout=60):
    """
    Writes a dataset representation. This is the hard part!

    Parameters
    ----------
    domain: str, default "data.cityofnewyork.us"
        The open data portal URI.
    folder_slug: str, default "nyc"
        The subfolder of the "data" directory into which the resource glossary will be placed.
    endpoint_type: str, default "everything"
        The resource type to build a glossary for.
    use_cache: bool, default True
        If a glossary already exists, whether to simply exit out or blow it away and create a new one (overwriting the
        old one).
    timeout: int, default 60
        The maximum amount of time to spend downloading data before killing the process. This is implemented so that
        occassional very large datasets do not crash the process.

    Returns
    -------
    Nothing; writes to a file.
    """
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

    # Whether we succeed or fail, we'll want to save the data we have at the end with a try-finally block.
    try:
        # Import the necessary library.
        import sys; sys.path.insert(0, "../../limited-requests")
        import limited_requests

        # Create a q for managing jobs.
        q = limited_requests.q()

        for i, resource in tqdm(list(enumerate(resource_list))):

            # Get the sizing information.
            sizings = limited_requests.limited_get(resource['id']['resource'], q, timeout=timeout)

            # If successful, append the result to the glossary...
            if sizings:

                for sizing in sizings:

                    # ...but with one caveat. When this process is run on a link, there is a strong possibility
                    # that it will result in the concatenation of a landing page. There's no automated way to
                    # determine whether or not a specific resource is or is not a landing page other than to
                    # inspect it outselves. For example, you can probably tell that
                    # "http://datamine.mta.info/user/register" is a landing page, but how about
                    # "http://ddcftp.nyc.gov/rfpweb/rfp_rss.aspx?q=open"? Or
                    # "https://a816-healthpsi.nyc.gov/DispensingSiteLocator/mainView.do"?

                    # Nevertheless, there is one fairly strong signal we can rely on: landing pages will be HTML
                    # front-end, and python-magic should in *most* cases determine this fact for us and return it
                    # in the file typing information. So we can use this to hopefully eliminate many of the
                    # problematic endpoints.

                    # However, realistically there would need to be some kind of secondary list mechanism that's
                    # maintained by hand for excluding specific pages. That, however, is a TODO.
                    if sizing['extension'] != "htm" and sizing['extension'] != "html":
                        # Remove the "processed" flag from the resource going into the glossary, if one exists.
                        glossarized_resource = resource.copy()
                        glossarized_resource['flags'] = [flag for flag in glossarized_resource['flags'] if
                                                         flag != 'processed']

                        # Attach sizing information.
                        glossarized_resource['sizing'] = {
                            'filesize': sizing['filesize']
                        }

                        # Attach format information.
                        glossarized_resource['format'] = {
                            'preferred_format': sizing['extension'],
                            'preferred_mimetype': sizing['mimetype']
                        }

                        # If no repairable errors were caught, write in the information.
                        # (if a non-repairable error was caught the data gets sent to the outer finally block)
                        glossarized_resource['id']['dataset'] = sizing['dataset']
                        glossary.append(glossarized_resource)

                        # Update the resource list to make note of the fact that this job has been processed.
                        resource['flags'].append("processed")

            # If unsuccessful, append a signal result to the glossary.
            else:
                glossarized_resource = resource.copy()

                glossarized_resource['flags'] = [flag for flag in glossarized_resource['flags'] if
                                                 flag != 'processed']

                glossarized_resource['sizing'] = {"filesize": ">{0}s".format(str(timeout))}
                glossarized_resource['id']['dataset'] = "."

                glossary.append(glossarized_resource)

            # Either way, update the resource list to make note of the fact that this job has been processed.
            if 'processed' not in resource['flags']:
                resource["flags"].append("processed")

    # Whether we succeeded or got caught on a fatal error, in either case clean up.
    finally:
        # Save output.
        with open(glossary_filename, "w") as fp:
            json.dump(glossary, fp, indent=4)
        with open(resource_filename, "w") as fp:
            json.dump(resource_list, fp, indent=4)
