"""
This module implements methodologies for glossarizing Socrata endpoints.
"""

import pysocrata
import json
import numpy as np
import os
from tqdm import tqdm


def write_resource_representation(domain="data.cityofnewyork.us", folder_slug="nyc", use_cache=True,
                                  credentials="../../auth/nyc-open-data.json",
                                  endpoint_type="table"):
    """
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
    # TODO: Link datatype has not been implemented yet.
    if endpoint_type == "link":
        raise ValueError("The link datatype has not been implemented yet.")

    # If the file already exists and we specify `use_cache=True`, simply return.
    resource_filename = "../../../data/" + folder_slug + "/resource lists/" + endpoint_type + ".json"
    preexisting = os.path.isfile(resource_filename)
    if preexisting and use_cache:
        return

    # Otherwise, continue.
    # First of all, load credentials.
    if isinstance(credentials, str):
        with open("../../../auth/nyc-open-data.json", "r") as fp:
            auth = json.load(fp)
    else:
        auth = credentials

    # Attach the domain.
    auth['domain'] = domain

    # If the metadata doesn't already exist, use pysocrata to fetch portal metadata. Otherwise, use what's provided.
    resources = pysocrata.get_datasets(**auth)

    # We exclude stories manually---this is a type of resource the Socrata API considers to be a dataset that we
    # are not interested in.
    resources = [d for d in resources if d['resource']['type'] != 'story']

    # Munge the Socrata API output a bit beforehand: in this case reworking the name references to match our
    # volcabulary.
    types = [d['resource']['type'] for d in resources]
    volcab_map = {'dataset': 'table', 'href': 'link', 'map': 'geospatial dataset', 'file': 'blob'}
    types = list(map(lambda d: volcab_map[d], types))

    # Get the resources of interest.
    # endpoints = [d['resource']['id'] for d in resources]
    indices = np.nonzero([t == endpoint_type for t in types])
    roi = np.array(resources)[indices]

    # Build the data representation.
    roi_repr = []
    for resource in roi:
        endpoint = resource['resource']['id']

        # The slug format depends on the API signature, which is in turn dependent on the dataset type.
        if endpoint_type == "table":
            slug = "https://" + domain + "/api/views/" + endpoint + "/rows.csv?accessType=DOWNLOAD"
        elif endpoint_type == "geospatial dataset":
            slug = "https://" + domain + "/api/geospatial/" + endpoint + "?method=export&format=GeoJSON"
        elif endpoint_type == "blob":
            slug = "https://data.cityofnewyork.us/download/" + endpoint + "/application%2Fzip"
        else:
            raise ValueError  # Links have not been implemented yet. This code shouldn't execute, gets caught at start.

        roi_repr.append(
            {
                'endpoint': endpoint,
                'resource': slug,
                'flags': []
             }
        )

    # Write to file and exit.
    with open(resource_filename, 'w') as fp:
        json.dump(roi_repr, fp, indent=4)


def write_dataset_representation(domain="data.cityofnewyork.us", folder_slug="nyc", use_cache=True,
                                 endpoint_type="table", timeout=60):
    """
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
        # What we do with the data depends on the endpoint type.

        # tables:
        # We take advantage of information provided on the Socrata portal pages to avoid having to work with the
        # datasets directly. The facilities provided by the endpoint-pager module are used to handle reading in data
        # from the portal web interface, which displays, among other things, row and column counts.
        if endpoint_type == "table":
            # Import the necessary library.
            import sys; sys.path.insert(0, "../../endpoint-pager/")
            import pager

            for resource in tqdm(resource_list):
                # Catch an error where the dataset has been deleted, warn but continue.
                try:
                    rowcol = pager.page_socrata(domain, resource['endpoint'], timeout=10)
                except pager.DeletedEndpointException:
                    print("WARNING: the '{0}' endpoint appears to have been removed.".format(resource['endpoint']))
                    resource['flags'].append('removed')
                    continue

                # If no repairable errors were caught, write in the information.
                # (if a non-repairable error was caught the data gets sent to the outer finally block)
                glossary.append({
                    'rows': rowcol['rows'],
                    'columns': rowcol['columns'],
                    'flags': resource['flags'],
                    'resource': resource['resource'],
                    'endpoint': resource['endpoint'],
                    'dataset': '.'
                })

                # Update the resource list to make note of the fact that this job has been processed.
                resource['flags'].append("processed")

        # geospatial datasets, blobs, links:
        # ...
        else:
            # Import the necessary library.
            import sys; sys.path.insert(0, "../../limited-requests")
            import limited_requests

            # Create a q for managing jobs.
            q = limited_requests.q()

            for i, resource in tqdm(list(enumerate(resource_list))):

                # Get the sizing information.
                sizings = limited_requests.limited_get(resource['resource'], q, timeout=timeout)

                # If successful, append the result to the glossary.
                if sizings:  # If successful.

                    for sizing in sizings:
                        glossary.append({
                            'rows': int(sizing['rows']),
                            'columns': int(sizing['columns']),
                            'filesize': int(sizing['filesize']),
                            'flags': resource['flags'],
                            'resource': sizing['resource'],
                            'endpoint': resource['endpoint'],
                            'dataset': sizing['dataset']
                        })

                # If unsuccessful, append a signal result to the glossary.
                else:
                    glossary.append({
                        'rows': "?",
                        'columns': "?",
                        'filesize': ">60s",
                        'flags': resource['flags'],
                        'resource': resource['resource'],
                        'endpoint': resource['endpoint'],
                        'dataset': "."
                    })

                # Either way, update the resource list to make note of the fact that this job has been processed.
                resource['flags'].append("processed")

    # Whether we succeeded or got caught on a fatal error, in either case clean up.
    finally:
        # If a driver was open, close the driver instance.
        if endpoint_type == "table":
            pager.driver.quit()

        # Save output.
        with open(glossary_filename, "w") as fp:
            json.dump(glossary, fp, indent=4)
        with open(resource_filename, "w") as fp:
            json.dump(resource_list, fp, indent=4)
