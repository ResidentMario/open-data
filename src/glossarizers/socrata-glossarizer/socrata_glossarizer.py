"""
This module implements methodologies for glossarizing Socrata endpoints.
"""

import pysocrata
import json
import numpy as np
import pandas as pd
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
    for resource in tqdm(roi):
        endpoint = resource['resource']['id']

        # The permalink format is standard.
        permalink = "https://{0}/d/{1}".format(domain, endpoint)

        # The slug format depends on the API signature, which is in turn dependent on the dataset type.
        if endpoint_type == "table":
            slug = "https://" + domain + "/api/views/" + endpoint + "/rows.csv?accessType=DOWNLOAD"
        elif endpoint_type == "geospatial dataset":
            slug = "https://" + domain + "/api/geospatial/" + endpoint + "?method=export&format=GeoJSON"
        elif endpoint_type == "blob" or endpoint_type == "link":
            slug = pager.page_socrata_for_resource_link(domain, endpoint)
        else:
            raise ValueError  # This code shouldn't execute, gets caught at start.

        roi_repr.append({
            'id': {
                'permalink': permalink,
                'resource': slug,
                'protocol': 'https',
                'name': resource['resource']['name'],
                'description': resource['resource']['description']
            },
            'provenance': {
                'attribution': resource['resource']['attribution']
            },
            'usage': {
                'created': str(pd.Timestamp(resource['resource']['createdAt'])),
                'last_updated': str(pd.Timestamp(resource['resource']['updatedAt'])),
                'page_views': resource['resource']['page_views']['page_views_total']
            },
            'contents': {
                'column_names': resource['resource']['columns_name']
            },
            'flags': []
        })

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
                    rowcol = pager.page_socrata_for_endpoint_size(domain, resource['id']['permalink'], timeout=10)
                except pager.DeletedEndpointException:
                    print("WARNING: the '{0}' endpoint appears to have been removed.".format(
                        resource['id']['endpoint']))
                    resource['flags'].append('removed')
                    continue

                # Remove the "processed" flag from the resource going into the glossary, if one exists.
                glossarized_resource = resource.copy()
                glossarized_resource['flags'] = [flag for flag in glossarized_resource['flags'] if flag != 'processed']

                # Attach sizing information.
                glossarized_resource['sizing'] = {
                    'rows': rowcol['rows'],
                    'columns': rowcol['columns'],
                }

                # Attach format information.
                glossarized_resource['format'] = {
                    'available_formats': ['csv', 'json', 'rdf', 'rss', 'tsv', 'xml'],
                    'preferred_format': 'csv',
                    'preferred_mimetype': 'text/csv'
                }

                # If no repairable errors were caught, write in the information.
                # (if a non-repairable error was caught the data gets sent to the outer finally block)
                glossarized_resource['id']['dataset'] = '.'
                glossary.append(glossarized_resource)

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
        # If a driver was open, close the driver instance.
        if endpoint_type == "table":
            pager.driver.quit()

        # Save output.
        with open(glossary_filename, "w") as fp:
            json.dump(glossary, fp, indent=4)
        with open(resource_filename, "w") as fp:
            json.dump(resource_list, fp, indent=4)
