import unittest
import pytest

import sys; sys.path.insert(0, '../')
import pager


class TestPageSocrata(unittest.TestCase):
    def setUp(self):
        self.domain = 'opendata.cityofnewyork.us'
        # landing page subdomain is "opendata", but all subpages use just "data".
        # test_page_socrata_failure will fail if this is not properly taken into account.

    def test_page_socrata_failure(self):
        # an endpoint that no longer exists
        uri = "https://data.cityofnewyork.us/d/gkne-dk5s"
        with pytest.raises(pager.DeletedEndpointException):
            pager.page_socrata(self.domain, uri)

    def test_page_socrata_success(self):
        # page_socrata wait-fetches the page load. Success if we finish without errors.
        uri = 'https://data.cityofnewyork.us/d/2rd2-9uwy'
        pager.page_socrata(self.domain, uri)

    def test_page_socrata_for_endpoint_size_success_1(self):
        # a table endpoint that does exist
        uri = "https://data.cityofnewyork.us/d/2rd2-9uwy"
        sizing = pager.page_socrata_for_endpoint_size(self.domain, uri)
        assert set(sizing.keys()) == {'columns', 'rows'}

    def test_page_socrata_for_endpoint_size_success_2(self):
        # a different table endpoint that does exist
        uri = "https://data.cityofnewyork.us/d/wr4r-bue7"
        sizing = pager.page_socrata_for_endpoint_size(self.domain, uri)
        assert set(sizing.keys()) == {'columns', 'rows'}

    def test_page_socrata_for_endpoint_size_success_3(self):
        # a different table endpoint that does exist
        uri = "https://data.cityofnewyork.us/d/97zg-4p9t"
        sizing = pager.page_socrata_for_endpoint_size(self.domain, uri)
        assert set(sizing.keys()) == {'columns', 'rows'}