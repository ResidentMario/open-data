import pandas as pd
from tqdm import tqdm
import requests
import warnings
from .generic import (preexisting_cache, load_glossary_todo,
                      write_resource_file, write_glossary_file, timeout_process)


def write_resource_representation(domain="data.gov.sg", out=None, use_cache=True, protocol='https'):
    import pdb; pdb.set_trace()

    # If the file already exists and we specify `use_cache=True`, simply return.
    if preexisting_cache(out, use_cache):
        return

    package_list_slug = "{0}://{1}/api/3/action/package_list".format(protocol, domain)
    package_list = requests.get(package_list_slug).json()

    if 'success' not in package_list or package_list['success'] != True:
        raise requests.RequestException("The CKAN catalog page did not resolve successfully.")

    resources = package_list['result']

    roi_repr = []

    try:
        for resource in tqdm(resources):
            # package_metadata_show vs. package_show?
            metadata = requests.get("{0}://{1}/api/3/action/package_show?id={2}".format(protocol,
                                                                                        domain,
                                                                                        resource)).json()

            try:
                license = metadata['result']['license']
            except KeyError:
                license = metadata['result']['license_title']
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
                        'landing_page': landing_page,
                        'resource': dataset['url'],
                        'protocol': 'https',
                        'name': "{0} - {1}".format(name, dataset['title']),  # composite name.
                        'description': description,
                        'publisher': publisher,
                        'sources': sources,
                        'last_updated': last_updated,
                        'update_frequency': update_frequency,
                        'tags_provided': keywords,
                        'topics_provided': topics,
                        'available_formats': [dataset['format'].lower()],
                        'preferred_format': dataset['format'].lower(),
                        'license': license,
                        'flags': []
                    })

            else:
                available_formats = [m['format'].lower() for m in metadata['result']['resources']]

                roi_repr.append({
                    'landing_page': landing_page,
                    'resource': slug,
                    'protocol': 'https',
                    'name': name,
                    'description': description,
                    'publisher': publisher,
                    'sources': sources,
                    'last_updated': last_updated,
                    'update_frequency': update_frequency,
                    'tags_provided': keywords,
                    'topics_provided': topics,
                    'available_formats': available_formats,
                    'preferred_format': preferred_format,
                    'license': license,
                    'flags': []
                })

    finally:
        # Write to file and exit.
        write_resource_file(roi_repr, out)


def write_glossary(domain="data.gov.sg", resource_filename=None, glossary_filename=None,
                   use_cache=True, timeout=60):
    # import limited_process
    # q = limited_process.q()

    resource_list, glossary = load_glossary_todo(resource_filename, glossary_filename, use_cache=use_cache)

    @timeout_process(timeout)
    def _size_up(uri):
        import sys
        import datafy

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

    # Whether we succeed or fail, we'll want to save the data we have at the end with a try-finally block.
    try:
        for i, resource in tqdm(list(enumerate(resource_list))):

            glossarized_resource = resource.copy()

            # Get the sizing information.
            # If the resource is its own dataset, this is provided in the content header. Sometimes it is not.
            headers = requests.head(resource['resource']).headers
            glossarized_resource['preferred_mimetype'] = headers['content-type']

            try:
                glossarized_resource['filesize'] = headers['content-length']
                glossarized_resource['dataset'] = '.'
                succeeded = True

            # If we error out, this is a packaged/gzipped file. Do sizing the basic way, with a GET request.
            except KeyError:
                dataset_repr = _size_up(resource['resource'])
                try:
                    glossarized_resource['filesize'] = dataset_repr[0]['filesize']
                    glossarized_resource['dataset'] = dataset_repr[0]['dataset']
                    succeeded = True
                except TypeError:
                    # Transient failure.
                    succeeded = False
                    warnings.warn(
                        "Couldn't parse the URI {0} due to a transient network failure."\
                            .format(resource['resource'])
                    )

            # Update the resource list to make note of the fact that this job has been processed.
            if 'processed' not in resource['flags'] and succeeded:
                resource["flags"].append("processed")

            glossary.append(glossarized_resource)

    # Whether we succeeded or got caught on a fatal error, in either case clean up.
    finally:
        # Save output.
        write_resource_file(resource_list, resource_filename)
        write_glossary_file(glossary, glossary_filename)

