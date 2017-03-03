import requests
import mimetypes
import io
import zipfile
import os
import random
import shutil
from requests_file import FileAdapter
import magic


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
    'application/x-msaccess': 'mdb',
    'application/vnd.ms-office': 'xls'  # Wrong, but useful, cf https://github.com/mime-types/ruby-mime-types/issues/98
}


# Set up requests so that it can be used inline with local files.
requests_session = requests.Session()
requests_session.mount("file://", FileAdapter())


def get(uri, sizeout=100000000000000000, type_hints=(None, None), localized=False):
    """
    Given the download URI for a resource, returns a list of (data, filepath_hint, type_hint) tuples corresponding with
    that resource's dataset contents. Note that in some cases None will substitute for `data` in the above, and that in
    some cases this parameter will size out and simply return None instead of anything at all.

    Parameters
    ----------
    uri: str (required)
        The URI corresponding with the result download link.
    sizeout: int
        Before sending a download request this method will first ask the server for the content-length header of the
        download. If one is provided, and exceeds this parameter in size, this method will simply return None.
    type_hints: (str, str) tuple
        Type hint for the dataset's type. Must be a (mimetype, extension) tuple.
    localized: bool
        Whether or not this file is a local file (e.g. a downloaded file that's sitting on your computer disk). This
        flag should only be used when `get` calls itself `recursively`; it shouldn't be set by the user.

    Returns
    -------
    None or a [{'data': r, 'fp': filepath_hint, 'mime': mime, 'ext': ext}, ...] list.
    """
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
    if type_hints != (None, None):
        mime, ext = type_hints

    # If no type hint is provided we have to guess the file type ourselves. This is a two-step process.
    else:

        # First, we check the result's mimetype against a map of known mimetypes (`mime_map`, above) in order to catch
        # obvious inputs. We can't account for every possible input, however, so this doesn't catch everything.

        # Files lacking a mimetype will generally be sent as an application/octet-stream.
        # import pdb; pdb.set_trace()
        try:
            split = r.headers['content-type'].split()
            mime = split[0].replace(";", "")
            ext = mime_map[mime]
        except KeyError:

            # If we get an object type that we don't get using the procedure above, we'll use an oracle to try to get
            # it. An oracle alone isn't enough, because it would e.g. report a CSV document as text/plain, which is
            # unhelpful. This is why the step above is necessary. However, an oracle (the `magic` library in this case)
            # always generates some kind of guess; the base case in the case of scrambled binary seems to be to guess
            # `.bat`.
            mime = magic.from_buffer(r.content, mime=True)

            try:
                ext = mime_map[mime]
            except KeyError:
                try:
                    ext = mimetypes.guess_extension(mime)[1:]
                except TypeError:
                    # If we still don't have an answer, warn.
                    # This mime type will probably need to be added to our hard-coded list at the top of the file.
                    raise RuntimeWarning(
                        "Couldn't determine meaning of the {0} content-type associated with the URI {1}".format(mime,
                                                                                                                uri)
                    )

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

    if ext == "zip":

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

        # Recurse using the file driver to deal with local folders.
        ret = []
        for filename in [name for name in z.namelist() if os.path.isdir(name)]:
            ext = filename.split(".")[-1]
            mime = magic.from_file('{0}/{1}'.format(temp_foldername, filename), mime=True)
            ret += get("file:///{0}/{1}".format(temp_foldername, filename), type_hints=(mime, ext), localized=True)

        # Delete the folder before returning the data.
        shutil.rmtree(temp_foldername)
        return ret

    else:
        return [{'data': r, 'fp': filepath_hint, 'mime': mime, 'ext': ext}]
