import unittest
import pytest
import json

import sys; sys.path.insert(0, '../../')
# noinspection PyUnresolvedReferences
from glossarizers import socrata_glossarizer


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
        import pdb; pdb.set_trace()
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

glossary_keys = {'available_formats', 'resource', 'page_views', 'sources', 'created', 'description', 'protocol',
                 'last_updated', 'dataset', 'column_names', 'rows', 'topics_provided', 'preferred_mimetype', 'name',
                 'preferred_format', 'columns', 'flags', 'keywords_provided', 'landing_page'}


class TestGlossarizeTable(unittest.TestCase):
    def test_glossarize_table(self):
        """
        Test that the resource-to-glossary method works in isolation and that the glossary schema is what we
        expected it to be.
        """
        with open("data/example_resource-f4rp-2kvy.json", "r") as fp:
            resource = json.load(fp)

        glossarized_resource = socrata_glossarizer.glossarize_table(resource, "opendata.cityofnewyork.us")
        assert glossarized_resource.keys() == glossary_keys


class TestGlossarizeNonTable(unittest.TestCase):
    def test_glossarize_nontable(self):
        # TODO: glossarized_non_table tests for all three cases: links, blobs, and geospatial datasets.
        # TODO: run all of the cases, make sure that everything works as expected.
        # TODO: see if it's possible to fix the table detection problem.
        # TODO: refactor ckan_glossarizer.py.
        # TODO: write the tests for ckan_glossarizer.py.
        # TODO: rewrite all of the docstrings.
        # TODO: write a using-this-api guide.
        # TODO: write a developing-with-this-api guide.
        # TODO: delete all the temp files and generally clean up this repo.
        pass