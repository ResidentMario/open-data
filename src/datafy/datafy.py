import pandas as pd
import geopandas as gpd
import requests
import mimetypes
import io
import os
import random


mime_map = {
    'text/csv': 'csv',
    'application/geo+json': 'geojson',
    'application/vnd.geo+json': 'geojson',  # Obseleted, but used by Socrata as of 1/23/2017
    'application/vnd.google-earth.kmz': 'kmz',
    'application/vnd.google-earth.kml+xml': 'kml',
    'application/zip': 'zip',
    'application/json': 'json',
    'application/xml': 'xml',
    'text/xml': 'xml',
}


def get(uri, sizeout=1000):

    # First send a HEAD request and back out if sizeout is exceeded.
    try:
        # Note: requests uses case-insenitive header names for access purposes. Saves a headache.
        content_length = requests.head(uri, timeout=1).headers['content-length']
        if content_length > sizeout:
            return None
    except KeyError:
        pass

    # Then send a GET request.
    r = requests.get(uri, timeout=7)

    # Get the content type and encoding from the header. e.g. "text/csv; encoding=utf-8" => (csv, utf-8)
    # Note that file-magic and python-magic modules exist for this, but are UNIX dependent because they rely on the
    # native libmagic library. Getting that running on Windows would require a lot of effort. For now, let's see if
    # we can maintain portability. That may ultimately be a mistake.
    #
    # All of the vendors that have to ascertain MIME types (including python-magic) uses a variety of lists to do it,
    # backed up by inspection heuristics for when the hard-coded lists fail.
    #
    # We only want to accept a handful of MIME types (http://www.iana.org/assignments/media-types/media-types.xhtml)
    # from the full list, and we assume that the open data portals hosting these services are competent enough to
    # send their data with the correct content types. I built this into the list above this method signature.
    #
    # If we absolutely must use an oracle, we can drop that in here later.
    split = r.headers['content-type'].split()

    # First try our guess.
    try:
        type_hint = mime_map[split[0]]
    except KeyError:
        type_hint = None
    # Then try to use Python's built-in mimetypes module to classify.
    if not type_hint:
        try:
            type_hint = mimetypes.guess_extension(split[0].rstrip(";"))[1:]
        except TypeError:
            type_hint = None
    # Raise if neither method works.
    if not type_hint:
        raise IOError("Couldn't determine meaning of content-type {0} associated with the URI {1}".format(
            type_hint, uri
        ))

    # Get the encoding hint, if there is one.
    encoding_hint = split[1].replace("charset=", "") if len(split) > 1 else None

    # print(type_hint)
    # print(encoding_hint)

    # Use the hints to load the data.
    if type_hint == "csv":
        return pd.read_csv(io.BytesIO(r.content), encoding=encoding_hint), type_hint
    elif type_hint == "geojson":
        # TODO: Can this be done without saving locally?
        # cf. http://gis.stackexchange.com/questions/225586/reading-raw-data-into-geopandas
        # return gpd.read_file(io.BytesIO(r.content), driver='GeoJSON', encoding=encoding_hint)
        temp_filename = str(random.randint(0, 1000000)) + ".geojson"  # minimize the chance of a collision
        with open(temp_filename, "w") as f:
            f.write(r.text)
        import pdb; pdb.set_trace()
        # Portability problem: fiona crashes trying to read on Windows???
        data = gpd.read_file(temp_filename, driver='GeoJSON', encoding=encoding_hint)
        os.remove(temp_filename)
        return data, type_hint
    elif type_hint == "json":  # Can be either pure JSON or GeoJSON.
        return None
    else:
        return None