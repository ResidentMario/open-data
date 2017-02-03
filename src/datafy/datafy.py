import pandas as pd
import geopandas as gpd
import requests
import mimetypes
import io
import zipfile
import os
import random
import shutil
from requests_file import FileAdapter
import magic
from xlrd.biffh import XLRDError


mime_map = {
    'text/csv': 'csv',
    'application/geo+json': 'geojson',
    'application/vnd.geo+json': 'geojson',  # Obselete, but used by Socrata as of 1/23/2017
    'application/vnd.google-earth.kmz': 'kmz',
    'application/vnd.google-earth.kml+xml': 'kml',
    'application/zip': 'zip',
    'application/json': 'json',
    'application/xml': 'xml',
    'text/xml': 'xml',
}


# Set up requests so that it can be used inline with local files.
requests_session = requests.Session()
requests_session.mount("file://", FileAdapter())


def get(uri, sizeout=100000000000000000, type=None, localized=False):

    # First send a HEAD request and back out if sizeout is exceeded. Don't do this if the file is local.
    if "file://" not in uri:
        try:
            # Note: requests uses case-insenitive header names for access purposes. Saves a headache.
            content_length = int(requests.head(uri, timeout=1).headers['content-length'])
            if content_length > sizeout:
                return None

        except (KeyError, TypeError):
            pass

    # Then send a GET request.
    r = requests_session.get(uri, timeout=7)

    # If a type hint is passed from above, use that.
    if type:
        type_hint = type

    # If no type hint is provided we have to guess the file type ourselves. This is a two-step process.
    else:

        # First, we check the result's mimetype against a map of known mimetypes (`mime_map`, above) in order to catch
        # obvious inputs. We can't account for every possible input, however, so this doesn't catch everything.
        try:
            split = r.headers['content-type'].split()
            type_hint = mime_map[split[0].replace(";", "")]
        except KeyError:
            type_hint = None

        # If we get an object type that we don't get using the procedure above, we'll use an oracle to try to get it.
        # An oracle alone isn't enough, because it would e.g. report a CSV document as text/plain, which is unhelpful.
        # This is why the step above is necessary. However, an oracle (the `magic` library in this case) always
        # generates some kind of guess; the base case in the case of scrambled binary seems to be to guess `.bat`.
        if not type_hint:
            try:
                mime = magic.from_buffer(r.content, mime=True)
                type_hint = mimetypes.guess_extension(mime)[1:]
            except TypeError:
                pass

        # If we still don't have an answer, raise (note: I'm not sure this line will ever execute).
        if not type_hint:
            raise IOError("Couldn't determine meaning of the content-type associated with the URI {0}".format(uri))

    # TODO: It may prove necessary to guess encoding information as well. If so, investigate using chardet.

    # If the URI contains a "file://" in front, we know that this piece of data was read out of an archival file format.
    # In that case, we need to pull in a filepath hint so that we can point to which specific file in the resource is
    # the dataset of interest. Otherwise, the entire resource is itself the dataset of interest, and we denote the path
    # with a ".".
    filepath_hint = uri.replace("file://", "") if "file://" in uri else "."

    # If get is called with the localized flag set to true, we are furthermore operating on a file which has already
    # been saved to disc using a temporary filename. This was done to simplify the code with it comes to inspecting
    # archival format files. Since we don't want that temporary path to be included in the filename that we write to
    # the glossary, we set this flag when that happens in order to remove that component of the path before writing,
    # as per here.
    filepath_hint = "/".join(filepath_hint.split("/")[2:]) if localized else filepath_hint

    # Use the hints to load the data.
    if type_hint == "csv":
        return [(pd.read_csv(io.BytesIO(r.content)), filepath_hint, type_hint)]

    elif type_hint == "geojson":
        data = gpd.GeoDataFrame(r.json())
        return [(data, filepath_hint, type_hint)]

    # We assume that JSON data gets passed with a JSON content-type and GeoJSON with a GeoJSON content-type. This is
    # true of the Socrata open data portal, and *probably* true of other open data portal providers, but a rule that is
    # almost certainly broken by less well-behaved landing pages on the net. For those cases, use the type parameter
    # over-ride.
    elif type_hint == "json":
        data = r.json()
        return [(data, filepath_hint, type_hint)]

    elif type_hint == "xls" or type_hint == "xlsx":

        # Try to read the Excel file, and return it as a component if we are successful.
        try:
            data = pd.read_excel(io.BytesIO(r.content))
            return [(data, filepath_hint, type_hint)]

        # If we are not, return the raw bytes instead.
        except XLRDError:
            return [(r.content, filepath_hint, type_hint)]

    elif type_hint == "zip":

        # In certain cases, it's possible to the contents of an archive virtually. This depends on the contents of the
        # file: shapefiles can't be read because they are split across multiple files, KML and KMZ files can't be read
        # because fiona doesn't support them. But most of the rest of things can be.

        # To keep the API simple, however, we're not going to do the three-way fork required to do this. Instead we're
        # going to take the performance hit of writing to disk in all cases (which is really trivial anyway compared to
        # the cost of downloading), and analyze that in-place.

        # This branch will then recursively call get as a subroutine, using the file driver to pick out the rest of the
        # files in the folder.
        z = zipfile.ZipFile(io.BytesIO(r.content))

        while True:
            temp_foldername = str(random.randint(0, 1000000))  # minimize the chance of a collision
            if not os.path.exists(temp_foldername):
                os.makedirs(temp_foldername)
                break
        z.extractall(path=temp_foldername)

        # Recuse using the file driver to deal with local folders.
        ret = []
        for filename in z.namelist():
            type = filename.split(".")[-1]
            ret += get("file:///{0}/{1}".format(temp_foldername, filename), type=type, localized=True)

        # Delete the folder before returning the data.
        shutil.rmtree(temp_foldername)
        return ret

    elif type_hint == "shp":
        # This will only happen on a file read, because shapefiles can't be a content-type on the web.
        data = gpd.read_file(uri.replace("file:///", ""))
        return [(data, filepath_hint, type_hint)]

    elif type_hint == "html" or type_hint == "htm":
        # This happens when we are passed a landing page instead of a proper resource endpoint. This shouldn't happen,
        # but inevitably will, due to the difficulty of separating external resource links from external landing page
        # links (this might also happen if a webpage is packaged into a zip archive or something).
        return [(None, filepath_hint, type_hint)]

    else:
        # We ignore file formats we don't know how to deal with as well as shapefile support files handled elsewhere.
        return [(None, filepath_hint, type_hint)]