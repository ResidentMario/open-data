"""
This module implements methodologies for glossarizing Socrata endpoints.
"""

import pysocrata
import json
import numpy as np
import pandas as pd
from tqdm import tqdm
from .generic import (preexisting_cache, load_glossary_todo,
                      write_resource_file, write_glossary_file,
                      write_resource_representation_docstring, write_glossary_docstring)
from selenium.common.exceptions import TimeoutException


def get_resource_representation(domain, credentials, endpoint_type):
    import pdb; pdb.set_trace()

    # Load credentials.
    with open(credentials, "r") as fp:
        auth = json.load(fp)
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
    indices = np.nonzero([t == endpoint_type for t in types])
    roi = np.array(resources)[indices]

    # Conditional pager import (this inits PhantomJS, don't necessarily want to if we don't have to).
    if endpoint_type == "blob" or endpoint_type == "link":
        from .pager import page_socrata_for_resource_link

    # Build the data representation.
    roi_repr = []
    for metadata in tqdm(roi):
        endpoint = metadata['resource']['id']

        # The landing_page format is standard.
        landing_page = "https://{0}/d/{1}".format(domain, endpoint)

        # The slug format depends on the API signature, which is in turn dependent on the dataset type.
        if endpoint_type == "table":
            slug = "https://" + domain + "/api/views/" + endpoint + "/rows.csv?accessType=DOWNLOAD"
        elif endpoint_type == "geospatial dataset":
            slug = "https://" + domain + "/api/geospatial/" + endpoint + "?method=export&format=GeoJSON"
        else:  # endpoint_type == "blob" or endpoint_type == "link":
            # noinspection PyUnboundLocalVariable
            slug = page_socrata_for_resource_link(domain, landing_page)

        name = metadata['resource']['name']
        description = metadata['resource']['description']
        sources = [metadata['resource']['attribution']]

        created = str(pd.Timestamp(metadata['resource']['createdAt']))
        last_updated = str(pd.Timestamp(metadata['resource']['updatedAt']))
        page_views = metadata['resource']['page_views']['page_views_total']

        column_names = metadata['resource']['columns_name']

        topics_provided = [metadata['classification']['domain_category']]
        keywords_provided = metadata['classification']['domain_tags']

        roi_repr.append({
            'id': {
                'landing_page': landing_page,
                'resource': slug,
                'protocol': 'https',
                'name': name,
                'description': description
            },
            'provenance': {
                'sources': sources
            },
            'usage': {
                'created': created,
                'last_updated': last_updated,
                'page_views': page_views
            },
            'contents': {
                'column_names': column_names
            },
            'tags': {
                'topics_provided': topics_provided,
                'keywords_provided': keywords_provided
            },
            'flags': []
        })

    return roi_repr


def write_resource_representation(domain="data.cityofnewyork.us", out="nyc-tables.json", use_cache=True,
                                  credentials="../../../auth/nyc-open-data.json"):
    """
    Fetches a resource representation for a single resource type from a Socrata portal.

    Parameters
    ----------
    domain: str, default "data.cityofnewyork.us"
        The open data portal URI.
    out: str, filepath
        Where to write the file to.
    use_cache: bool, default True
        If a resource representation already exists, whether to simply exit out or blow it away and create a new one
        (overwriting the old one).
    credentials: str or dict, default "../../auth/nyc-open-data.json"
        Either a filepath to the file containing your API credentials for the given Socrata instance, or a dictionary
        containing the same information.

    Returns
    -------
    Nothing; writes to a file.
    """
    # If the file already exists and we specify `use_cache=True`, simply return.
    if preexisting_cache(out, use_cache):
        return

    # Generate to file and exit.
    roi_repr = []
    import pdb; pdb.set_trace()
    for endpoint_type in ['table', 'geospatial dataset', 'blob', 'link']:
        roi_repr += get_resource_representation(domain, credentials, endpoint_type)
    write_resource_file(roi_repr, out)

write_resource_representation.__doc__ = write_resource_representation_docstring


def get_sizings(uri, q, timeout=60):
    import limited_process
    import datafy
    import sys

    # kwargs = {}

    def _size_up(uri, q, kwargs):
        def apply(resource):
            thing_log = []
            for thing in resource:  # probably a dataset, but potentially metadata et. al. instead
                print(thing)
                thing_log.append({
                    'filesize': sys.getsizeof(thing['data'].content) / 1024,
                    'dataset': thing['filepath'],
                    'mimetype': thing['mimetype'],
                    'extension': thing['extension']
                })
        return q.put(apply(datafy.get(uri, **kwargs)))

    return limited_process.limited_get(
        uri,
        q, timeout=timeout, callback=_size_up
    )


def get_glossary(domain="data.cityofnewyork.us", use_cache=True,
                 endpoint_type="table", resource_filename=None, glossary_filename=None, timeout=60):
    pass


def write_glossary(domain="data.cityofnewyork.us", use_cache=True,
                   endpoint_type="table", resource_filename=None, glossary_filename=None, timeout=60):
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
    resource_list, glossary = load_glossary_todo(resource_filename, glossary_filename, use_cache)

    # Whether we succeed or fail, we'll want to save the data we have at the end with a try-finally block.
    try:
        # What we do with the data depends on the endpoint type.

        # tables:
        # We take advantage of information provided on the Socrata portal pages to avoid having to work with the
        # datasets directly. The facilities provided by the pager module are used to handle reading in data
        # from the portal web interface, which displays, among other things, row and column counts.
        if endpoint_type == "table":
            # Only import pager if we have to.
            from .pager import page_socrata_for_endpoint_size, DeletedEndpointException, driver

            for resource in tqdm(resource_list):
                # Catch an error where the dataset has been deleted, warn but continue.
                try:
                    rowcol = page_socrata_for_endpoint_size(domain, resource['id']['landing_page'], timeout=10)
                except (DeletedEndpointException):
                    print("WARNING: the '{0}' endpoint was probably removed.".format(
                        resource['id']['landing_page']))
                    resource['flags'].append('removed')
                    continue
                except (TimeoutException):
                    print("WARNING: the '{0}' endpoint amay have been removed.".format(
                        resource['id']['landing_page']))
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
            import limited_process
            q = limited_process.q()

            for i, resource in tqdm(list(enumerate(resource_list))):
                # Get the sizing information.
                sizings = get_sizings(
                    "https://data.cityofnewyork.us/api/views/gezn-7mgk/rows.csv?accessType=DOWNLOAD",
                    q, timeout=timeout
                )

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
        import pdb; pdb.set_trace()
        # If a driver was open, close the driver instance.
        if endpoint_type == "table":
            # noinspection PyUnboundLocalVariable
            driver.quit()  # pager.driver

        # Save output.
        write_resource_file(None, None, resource_list, resource_filename)
        write_glossary_file(None, None, glossary, glossary_filepath)

write_resource_representation.__doc__ = write_glossary_docstring
