"""
This module implements methodologies for glossarizing Socrata endpoints.
"""

import pysocrata
import json
import numpy as np
import pandas as pd
from tqdm import tqdm
from .generic import (preexisting_cache, load_glossary_todo,
                      write_resource_file, write_glossary_file)
from selenium.common.exceptions import TimeoutException


def resourcify(metadata, domain, endpoint_type):
    """
    Given Socrata API metadata about a certain endpoint (as well as the domain of the endpoint
    e.g. "data.cityofnewyork.us", and the type, e.g. "table") return a resource-ified entry for the
    endpoint for inclusion in the resource listing.
    """
    # Conditional pager import (this inits PhantomJS, don't necessarily want to if we don't have to).
    if endpoint_type == "blob" or endpoint_type == "link":
        from .pager import page_socrata_for_resource_link

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

    return {
        'landing_page': landing_page,
        'resource': slug,
        'protocol': 'https',
        'name': name,
        'description': description,
        'sources': sources,
        'created': created,
        'last_updated': last_updated,
        'page_views': page_views,
        'column_names': column_names,
        'topics_provided': topics_provided,
        'keywords_provided': keywords_provided,
        'flags': []
    }


def get_portal_metadata(domain, credentials, endpoint_type):
    """
    Given a domain, Socrata API credentials for that domain, and a type of endpoint of interest, returns the metadata
    provided by the portal (via the pysocrata package).
    """
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

    return roi


def get_resource_representation(domain, credentials, endpoint_type):
    """
    Given a domain, Socrata API credentials for that domain, and a type of endpoint of interest, returns a full
    resource representation (using resourcify) for each resource therein.
    """
    roi = get_portal_metadata(domain, credentials, endpoint_type)

    # Convert the pysocrata output to our data representation using resourcify.
    roi_repr = []
    for metadata in tqdm(roi):
        roi_repr.append(resourcify(metadata, domain, endpoint_type))

    return roi_repr


def write_resource_representation(domain="data.cityofnewyork.us", out="nyc-tables.json", use_cache=True,
                                  credentials="../../../auth/nyc-open-data.json", endpoint_type='table'):
    """
    Fetches a resource representation for a single resource type from a Socrata portal. Simple I/O wrapper around
    get_resource_representation, using some utilities from generic.py.
    """
    # If the file already exists and we specify `use_cache=True`, simply return.
    if preexisting_cache(out, use_cache):
        return

    # Generate to file and exit.
    roi_repr = []
    roi_repr += get_resource_representation(domain, credentials, endpoint_type)
    write_resource_file(roi_repr, out)


def glossarize_table(resource, domain, driver=None, timeout=60):
    """
    Given an individual resource (as would be loaded from the resource list) and a domain, and optionally a
    PhantomJS driver (recommended), creates a glossaries entry for that resource.
    """
    from .pager import page_socrata_for_endpoint_size, DeletedEndpointException

    # If a PhantomJS driver has not been initialized (via import), initialize it now.
    # In this case we will also quit it out at the end of the process.
    # This is inefficient, but useful for testing.
    driver_passed = bool(driver)
    if not driver_passed:
        from .pager import driver

    try:
        rowcol = page_socrata_for_endpoint_size(domain, resource['landing_page'], timeout=timeout)
    except DeletedEndpointException:
        print("WARNING: the '{0}' endpoint was deleted.".format(resource['landing_page']))
        resource['flags'].append('removed')
        return []
    except TimeoutException:
        print("WARNING: the '{0}' endpoint could not be processed.".format(resource['landing_page']))
        resource['flags'].append('removed')
        return []

    # Remove the "processed" flag from the resource going into the glossaries, if one exists.
    glossarized_resource = resource.copy()
    glossarized_resource['flags'] = [flag for flag in glossarized_resource['flags'] if flag != 'processed']

    # Attach sizing information.
    glossarized_resource['rows'] = rowcol['rows']
    glossarized_resource['columns'] = rowcol['columns']

    # Attach format information.
    glossarized_resource['available_formats'] = ['csv', 'json', 'rdf', 'rss', 'tsv', 'xml']
    glossarized_resource['preferred_format'] = 'csv'
    glossarized_resource['preferred_mimetype'] = 'text/csv'

    # If no repairable errors were caught, write in the information.
    # (if a non-repairable error was caught the data gets sent to the outer finally block)
    glossarized_resource['dataset'] = '.'

    if not driver_passed:
        driver.quit()

    return [glossarized_resource]


def get_sizings(uri, q, timeout=60):
    """
    Given a URI and a multiprocessing.Queue, returns a structured dict explaining file size and type if download is
    successful, and None if the download process times out (takes too long).

    This method utilizes limited_process and datafy facilities, these are two small modules written for the purposes of
    this project maintained as separate modules.
    """
    import datafy
    import sys
    from .generic import timeout_process

    @timeout_process(timeout)
    def _size_up(uri):
        resource = datafy.get(uri)
        thing_log = []
        for thing in resource:
            thing_log.append({
                'filesize': sys.getsizeof(thing['data'].content) / 1024,
                'dataset': thing['filepath'],
                'mimetype': thing['mimetype'],
                'extension': thing['extension']
            })
        return thing_log

    return _size_up(uri)


def glossarize_nontable(resource, timeout, q=None):
    """
    Same as `glossarize_table`, but for the non-table resource types.
    """
    import limited_process
    # TODO: Remove limited_process non-dependency.

    import zipfile
    from requests.exceptions import ChunkedEncodingError

    if not bool(q):
        q = limited_process.q()

    try:
        sizings = get_sizings(
            resource['resource'],
            q, timeout=timeout
        )
    except zipfile.BadZipfile:
        # cf. https://github.com/ResidentMario/datafy/issues/2
        print("WARNING: the '{0}' endpoint is either misformatted or contains multiple levels of "
              "archiving which failed to process.".format(resource['landing_page']))
        resource['flags'].append('error')
        return []
    # This error is raised when the process takes too long.
    except ChunkedEncodingError:
        print("WARNING: the '{0}' endpoint took longer than the {1} second timeout to process.".format(
            resource['landing_page'], timeout))
        return []
    except:
        # External links may point anywhere, including HTML pages which don't exist or which raise errors when you
        # try to visit them. During testing this occurred with e.g. https://data.cityofnewyork.us/d/sah3-jw2y. It's
        # impossible to exclude everything; best we can do is raise a warning.
        print("WARNING: an error was raised while processing the '{0}' endpoint.".format(resource['landing_page']))
        resource['flags'].append('error')
        return []

    # If successful, append the result to the glossaries...
    if sizings:
        glossarized_resource = []

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
                # Remove the "processed" flag from the resource going into the glossaries, if one exists.
                glossarized_resource_element = resource.copy()
                glossarized_resource_element['flags'] = [flag for flag in glossarized_resource_element['flags'] if
                                                         flag != 'processed']

                # Attach sizing information.
                glossarized_resource_element['filesize'] = sizing['filesize']

                # Attach format information.
                glossarized_resource_element['preferred_format'] = sizing['extension']
                glossarized_resource_element['preferred_mimetype'] = sizing['mimetype']

                # If no repairable errors were caught, write in the information.
                # (if a non-repairable error was caught the data gets sent to the outer finally block)
                glossarized_resource_element['dataset'] = sizing['dataset']

                glossarized_resource.append(glossarized_resource_element)

                # # Update the resource list to make note of the fact that this job has been processed.
                # resource['flags'].append("processed")

        return glossarized_resource

    # If unsuccessful, append a signal result to the glossaries.
    else:
        glossarized_resource = resource.copy()

        glossarized_resource['flags'] = [flag for flag in glossarized_resource['flags'] if
                                         flag != 'processed']

        glossarized_resource["filesize"] = ">{0}s".format(str(timeout))
        glossarized_resource['dataset'] = "."

        return glossarized_resource

    # Either way, update the resource list to make note of the fact that this job has been processed.
    # if 'processed' not in resource['flags']:
    #     resource["flags"].append("processed")


def get_glossary(resource_list, glossary, domain='opendata.cityofnewyork.us', endpoint_type="table", timeout=60):
    # Whether we succeed or fail, we'll want to save the data we have at the end with a try-finally block.
    try:
        # What we do with the data depends on the endpoint type.

        # tables:
        # We take advantage of information provided on the Socrata portal pages to avoid having to work with the
        # datasets directly. The facilities provided by the pager module are used to handle reading in data
        # from the portal web interface, which displays, among other things, row and column counts.
        if endpoint_type == "table":
            # Only import pager if we have to.

            from .pager import driver

            for resource in tqdm(resource_list):
                glossarized_resource = glossarize_table(resource, domain, driver=driver)
                glossary += glossarized_resource

                # Update the resource list to make note of the fact that this job has been processed.
                resource['flags'].append("processed")

        # geospatial datasets, blobs, links:
        # ...
        else:
            import limited_process
            q = limited_process.q()

            for resource in tqdm(list(resource_list)):
                glossarized_resource = glossarize_nontable(resource, timeout, q=q)
                glossary += glossarized_resource

                # Update the resource list to make note of the fact that this job has been processed.
                if 'processed' not in resource['flags']:
                    resource["flags"].append("processed")

    # Whether we succeeded or got caught on a fatal error, in either case clean up.
    finally:
        # If a driver was open, close the driver instance.
        if endpoint_type == "table":
            # noinspection PyUnboundLocalVariable
            driver.quit()  # pager.driver
    return resource_list, glossary


def write_glossary(domain='opendata.cityofnewyork.us', use_cache=True,
                   endpoint_type="table", resource_filename=None, glossary_filename=None, timeout=60):
    """
    Writes a dataset representation.

    Parameters
    ----------
    domain: str, default "opendata.cityofnewyork.us"
        The open data portal landing page URI.
    use_cache: bool, default True
        If a glossaries already exists, whether to simply exit out or blow it away and create a new one (overwriting the
        old one).
    endpoint_type: str, default "table"
        The resource type to build a glossaries for. Options are "table", "blob", "geospatial dataset", and "link".
    timeout: int, default 60
        The maximum amount of time to spend downloading data before killing the process. This is implemented so that
        occasional very large datasets do not crash the process.
    resource_filename: str
        The name of the resource file to read the jobs from.
    glossary_filename: str
        The name of the glossaries file to write the output to.
    """

    # Begin by loading in the data that we have.
    resource_list, glossary = load_glossary_todo(resource_filename, glossary_filename, use_cache)

    # Generate the glossaries.
    try:
        resource_list, glossary = get_glossary(resource_list, glossary, domain=domain, endpoint_type=endpoint_type,
                                               timeout=timeout)

    # Save output.
    finally:
        write_resource_file(resource_list, resource_filename)
        write_glossary_file(glossary, glossary_filename)
