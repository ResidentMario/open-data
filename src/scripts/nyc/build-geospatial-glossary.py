import os
import json
import pysocrata
import numpy as np
from tqdm import tqdm

from pebble import ProcessPool
from concurrent.futures import TimeoutError
import datafy

DOMAIN = "data.cityofnewyork.us"
FILE_SLUG = "nyc"

# First check to see whether or not a geospatial.json file already exists. We won't recreate the file if it already
# exists. This means that:
# 1. To regenerate the information from scratch, the file must first be deleted.
# 2. Manual edits to the glossary will be preserved (this is a behavior that we want) and considered by the second part
#    of this script.
preexisting = os.path.isfile("../../../data/" + FILE_SLUG + "/glossaries/geospatial.json")


# If not, build out an initial list.
if not preexisting:
    # Obtain NYC open data portal credentials.
    with open("../../../auth/nyc-open-data.json", "r") as f:
        nyc_auth = json.load(f)

    # Use pysocrata to fetch portal metadata.
    nyc_datasets = pysocrata.get_datasets(**nyc_auth)
    nyc_datasets = [d for d in nyc_datasets if d['resource']['type'] != 'story']  # stories excluded manually

    # Get geospatial datasets.
    nyc_types = [d['resource']['type'] for d in nyc_datasets]
    volcab_map = {'dataset': 'table', 'href': 'link', 'map': 'geospatial dataset', 'file': 'blob'}
    nyc_types = list(map(lambda d: volcab_map[d], nyc_types))
    nyc_endpoints = [d['resource']['id'] for d in nyc_datasets]
    geospatial_indices = np.nonzero([t == 'geospatial dataset' for t in nyc_types])
    geospatial_endpoints = np.array(nyc_endpoints)[geospatial_indices]
    geospatial_datasets = np.array(nyc_datasets)[geospatial_indices]

    # Build the data representation.
    datasets = []
    for dataset in geospatial_datasets:
        endpoint = dataset['resource']['id']
        slug = "https://" + DOMAIN + "/api/geospatial/" + endpoint + "?method=export&format=GeoJSON"
        datasets.append(
            {
                'endpoint': endpoint,
                'resource': slug,
                'dataset': '.',
                'type': 'geojson',
                'rows': '?',
                'columns': '?',
                'filesize': '?',
                'flags': ''
             }
        )

    # Write to the file.
    with open("../../../data/" + FILE_SLUG + "/glossaries/geospatial.json", "w") as fp:
        json.dump(datasets, fp, indent=4)

    del datasets


# At this point we know that the file exists. But its contents may not contain the row and column size information that
# we need, because if it was just regenerated by the loop above that stuff will have been populated simply with "?" so
# far.

# Begin by loading in the data that we have.
with open("../../../data/" + FILE_SLUG + "/glossaries/geospatial.json", "r") as fp:
    datasets = json.loads(fp.read())

# Build a tuple out of the URI, endpoint, and positional index of each entry.
# We'll use each of these later on, either as input to datify.get or to find where to store what we find.
# Ignore datasets which already have all of their size information defined.
datasets_needing_extraction = [d for d in datasets\
                               if (d['rows'] == "?") or (d['columns'] == "?") or (d['filesize'] == "?")]
indices = [i for i, d in enumerate(datasets)\
           if (d['rows'] == "?") or (d['columns'] == "?") or (d['filesize'] == "?")]
uris = [d['resource'] for d in datasets_needing_extraction]
endpoints = [d['endpoint'] for d in datasets_needing_extraction]
process_tuples = list(zip(uris, endpoints, indices))


# Wrap datafy.get for our purposes.
def get_data(tup):
    # Extract the data from the input tuple (couldn't seem to pass data through the map otherwise?)
    uri, endpoint, i = tup[0], tup[1], tup[2]

    # Get the data points.
    ret = datafy.get(uri)
    assert len(ret) == 1  # should be true; otherwise this is a ZIP of some kind.
    data, data_type = ret[0]

    # return a (<gpd.GeoDataFrame object>, "geojson", <endpoint string>, index) tuple
    return data, data_type, endpoint, i


# Whether we succeeded or got caught on a fatal error, in either case save the output to file before exiting.
try:
    # Run our processing jobs asynchronously.
    with ProcessPool(max_workers=4) as pool:
        # Use tqdm manual counting for keeping track of progress.
        with tqdm(total=5) as pbar:
            iterator = pool.map(get_data, process_tuples, timeout=60)  # cap data downloads at 60 seconds apiece.

            while True:
                try:
                    data, data_type, endpoint, i = next(iterator)
                    pbar.update(1)
                    columns = len(data.columns)
                    rows = len(data)
                    filesize = int(data.memory_usage().sum())  # must cast to int because json will not serialize np.int64
                    ep = datasets[i]
                    ep['rows'] = rows
                    ep['columns'] = columns
                    ep['filesize'] = filesize
                except TimeoutError as error:
                    print("Function took longer than %d seconds. Skipping responsible endpoint..." % error.args[1])
                    pbar.update(1)
                except StopIteration:
                    break
finally:
    # Whether we succeeded or got caught on a fatal error, in either case save the output to file before exiting.
    with open("../../../data/" + FILE_SLUG + "/glossaries/geospatial.json", "w") as fp:
        json.dump(datasets, fp, indent=4)