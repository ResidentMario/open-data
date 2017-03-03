import unittest
import sys; sys.path.insert(0, "../")
import limited_requests


class TestLimitedRequests(unittest.TestCase):

    def test_simple_success(self):
        q = limited_requests.q()
        ret = limited_requests.limited_get(
            "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD", q,
            timeout=10
        )
        assert ret

    def test_simple_failure(self):
        q = limited_requests.q()
        ret = limited_requests.limited_get(
            "https://data.cityofnewyork.us/api/geospatial/pi5s-9p35?method=export&format=Shapefile", q,
            timeout=0.1
        )
        assert (not ret)

if __name__ == '__main__':
    unittest.main()
