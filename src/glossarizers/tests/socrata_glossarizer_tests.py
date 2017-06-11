"""
Unit tests for socrata_glossarizer module macro-level subcomponents.
"""

import unittest
import pytest
import json

import sys; sys.path.insert(0, '../../')
# noinspection PyUnresolvedReferences
from glossarizers import socrata_glossarizer
import requests


# @slow
class TestGetPortalMetadata(unittest.TestCase):
    def test_get_portal_metadata(self):
        # Test whether or not the metadata returned by our processor still fits the format of the metadata that we
        # expect from Socrata. Note that similar tests are a to-do for the pysocrata test suite.

        tables = socrata_glossarizer.get_portal_metadata("data.cityofnewyork.us",
                                                         "../../../auth/nyc-open-data.json",
                                                         "table")
        assert len(tables) > 1000

        # Take an example dataset. Note that if this example dataset ever gets deleted, this test will fail,
        # so it will need to be updated then.
        toi = next(table for table in tables if table['permalink'].split("/")[-1] == "f4rp-2kvy")

        # Now make sure the schema is still the same.
        assert set(toi.keys()) == {'permalink', 'link', 'metadata', 'resource', 'classification'}
        assert isinstance(toi['permalink'], str)
        assert isinstance(toi['link'], str)
        assert set(toi['metadata'].keys()) == {'license', 'domain'}
        assert set(toi['classification'].keys()) == {'tags', 'categories', 'domain_category', 'domain_tags',
                                                     'domain_metadata'}
        assert set(toi['resource'].keys()) == {'parent_fxf', 'columns_description', 'columns_field_name', 'provenance',
                                               'description', 'view_count', 'name', 'id', 'type', 'updatedAt',
                                               'download_count', 'attribution', 'createdAt', 'page_views',
                                               'columns_name'}


class TestResourcify(unittest.TestCase):
    def test_resourcify_table(self):
        """
        Test that the metadata-to-resource method works and that the resource schema is what we expected it to be.
        """
        with open("data/example_metadata-f4rp-2kvy.json", "r") as fp:
            toi_metadata = json.load(fp)
        resource = socrata_glossarizer.resourcify(toi_metadata, domain="data.cityofnewyork.us", endpoint_type="table")
        assert resource.keys() == {'sources', 'flags', 'topics_provided', 'created', 'name', 'protocol', 'description',
                                   'column_names', 'page_views', 'resource', 'last_updated', 'keywords_provided',
                                   'landing_page'}

table_glossary_keys = {'available_formats', 'resource', 'page_views', 'sources', 'created', 'description', 'protocol',
                       'last_updated', 'dataset', 'column_names', 'rows', 'topics_provided', 'preferred_mimetype',
                       'name', 'preferred_format', 'columns', 'flags', 'keywords_provided', 'landing_page'}


class TestGlossarizeTable(unittest.TestCase):
    def test_glossarize_table(self):
        """
        Test that the resource-to-glossaries method works in isolation and that the glossaries schema is what we
        expected it to be.
        """
        with open("data/example_metadata-f4rp-2kvy.json", "r") as fp:
            resource = socrata_glossarizer.resourcify(json.load(fp), "data.cityofnewyork.us", "table")

        glossarized_resource = socrata_glossarizer.glossarize_table(resource, "opendata.cityofnewyork.us")
        assert glossarized_resource[0].keys() == table_glossary_keys


class TestGetSizings(unittest.TestCase):
    def setUp(self):
        self.zip_test_uri = "https://data.cityofnewyork.us/api/views/q68s-8qxv/files/511dbe78-65f3-470f-9cc8" \
                            "-c1415f75a6e6?filename=AnnAvg1_7_300mSurfaces.zip"
        self.csv_test_uri = "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD"
        self.zip_fail_test_uri = "https://data.cityofnewyork.us/api/views/ft4n-yqee/files/3d0f7600-f88a-4a11-8ad9" \
                                 "-707c785caa08?filename=Broadband%20Data%20Dig%20-%20Datasets.zip"

    def test_get_sizings_success_csv(self):
        sizings = socrata_glossarizer.get_sizings(self.csv_test_uri, None, timeout=20)
        assert sizings and len(sizings) != 0

    def test_get_sizings_success_zip(self):
        # More difficult problem than that of a single CSV file.
        sizings = socrata_glossarizer.get_sizings(self.zip_test_uri, None, timeout=20)
        assert sizings and len(sizings) != 0

    def test_get_sizing_fail(self):
        """
        The error being raises is not something I am comfortable with, but it is what works for the moment and what
        the module depends on. See the notes in timeout_process for more details.
        """
        with pytest.raises(requests.exceptions.ChunkedEncodingError):
            socrata_glossarizer.get_sizings(self.zip_fail_test_uri, None, timeout=1)


nontable_glossary_keys = {'resource', 'column_names', 'created', 'page_views', 'landing_page', 'flags',
                          'keywords_provided', 'name', 'description', 'last_updated', 'filesize', 'dataset',
                          'preferred_format', 'protocol', 'sources', 'preferred_mimetype', 'topics_provided'}


class TestGlossarizeNonTable(unittest.TestCase):
    """
    Test that the resource-to-glossaries method works in isolation and that the glossaries schema is what we
    expected it to be.
    """

    def test_glossarize_nontable_blob(self):
        # TODO: Why does this fail?
        # with open("data/example_metadata-q68s-8qxv.json", "r") as fp:
        #     import pdb; pdb.set_trace()
        #     resource = socrata_glossarizer.resourcify(json.load(fp), "data.cityofnewyork.us", "blob")
        with open("data/example_resource-q68s-8qxv.json", "r") as fp:
            resource = json.load(fp)

        glossarized_resource = socrata_glossarizer.glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == nontable_glossary_keys

    def test_glossarize_nontable_geospatial_dataset(self):
        with open("data/example_metadata-ghq4-ydq4.json", "r") as fp:
            resource = socrata_glossarizer.resourcify(json.load(fp), "data.cityofnewyork.us", "geospatial dataset")

        glossarized_resource = socrata_glossarizer.glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == nontable_glossary_keys

    def test_glossarize_link(self):
        # For generating a new metadata entry for testing:
        # links = socrata_glossarizer.get_portal_metadata("data.cityofnewyork.us",
        #                                                 "../../../auth/nyc-open-data.json",
        #                                                 "links")
        # ...
        with open("data/example_metadata-rnnj-5mmi.json", "r") as fp:
            resource = socrata_glossarizer.resourcify(json.load(fp), "data.cityofnewyork.us", "link")

        glossarized_resource = socrata_glossarizer.glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == nontable_glossary_keys


# TODO: run all of the cases, make sure that everything works as expected.
# TODO: refactor ckan_glossarizer.py.
# TODO: write the tests for ckan_glossarizer.py.
# TODO: rewrite all of the docstrings.
# TODO: write a using-this-api guide.
# TODO: write a developing-with-this-api guide.
