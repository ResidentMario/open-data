import unittest
import pytest
import json

import sys; sys.path.insert(0, '../../')
# noinspection PyUnresolvedReferences
from glossarizers import socrata_glossarizer


# Helpers.
def read_file(fp):
    """Read a file from the /data folder as string."""
    with open('data/' + fp, 'rb') as f:
        return f.read()

# def ok(results):
#     """Return whether or not all results are status 200 OK."""
#     return all([result['data'].ok for result in results])
#
#
# def match(results, expected):
#     """Return whether or not the results match the expectation. Excludes the response object."""
#     for result in results:
#         result.pop('data')
#     return results == expected


# class TestGetPortalMetadata(unittest.TestCase):
#     def test_get_portal_metadata(self):
#         # Test whether or not the metadata returned by our processor still fits the format of the metadata that we
#         # expect from Socrata. Note that similar tests are a to-do for the pysocrata test suite.
#
#         # This test is VERY SLOW.
#         tables = socrata_glossarizer.get_portal_metadata("data.cityofnewyork.us",
#                                                          "../../../auth/nyc-open-data.json",
#                                                          "table")
#         assert len(tables) > 1000
#
#         # Take an example dataset. Note that if this example dataset ever gets deleted, this test will fail,
#         # so it will need to be updated then.
#         import pdb; pdb.set_trace()
#         toi = next(table for table in tables if table['permalink'].split("/")[-1] == "f4rp-2kvy")
#
#         # Now make sure the schema is still the same.
#         assert set(toi.keys()) == {'permalink', 'link', 'metadata', 'resource', 'classification'}
#         assert isinstance(toi['permalink'], str)
#         assert isinstance(toi['link'], str)
#         assert set(toi['metadata'].keys()) == {'license', 'domain'}
#         assert set(toi['classification'].keys()) == {'tags', 'categories', 'domain_category', 'domain_tags',
#                                                      'domain_metadata'}
#         assert set(toi['resource'].keys()) == {'parent_fxf', 'columns_description', 'columns_field_name', 'provenance',
#                                                'description', 'view_count', 'name', 'id', 'type', 'updatedAt',
#                                                'download_count', 'attribution', 'createdAt', 'page_views',
#                                                'columns_name'}

class TestResourcify(unittest.TestCase):
    def test_resourcify_table(self):
        """Test whether or not..."""
        with open("data/example_metadata-f4rp-2kvy.json", "r") as fp:
            toi_metadata = json.load(fp)
        resource = socrata_glossarizer.resourcify(toi_metadata, domain="data.cityofnewyork.us", endpoint_type="table")
        import pdb; pdb.set_trace()
        1 + 1