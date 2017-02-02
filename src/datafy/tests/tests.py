import unittest
import sys; sys.path.insert(0, '../../')
from datafy import datafy


class TestGet(unittest.TestCase):

    def testCSV(self):
        _, _, type = datafy.get("https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD")[0]
        assert type == "csv"

    def testGeoJSON(self):
        _, _, type = datafy.get("https://data.cityofnewyork.us/api/geospatial/arq3-7z49?method=export&format=GeoJSON")[0]
        assert type == "geojson"

    def testJSON(self):
        _, _, type = datafy.get("https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.json?accessType=DOWNLOAD")[0]
        assert type == "json"

    def testShapefile(self):
        ret = datafy.get("https://data.cityofnewyork.us/api/geospatial/arq3-7z49?method=export&format=Shapefile")
        assert len(list(filter(lambda d: d[2] == "shp", ret))) > 0

    def testZippedExcelFiles(self):
        # Plus a Word document.
        docs = datafy.get("https://data.cityofnewyork.us/download/gua4-p9wg/application%2Fzip")
        filetypes = [t[2] for t in docs]
        assert len(list(filter(lambda f: f == 'xlsx', filetypes))) == 7
        assert len(list(filter(lambda f: f == 'docx', filetypes))) == 1

    def testXLSX(self):
        _, _, type = datafy.get("https://data.cityofnewyork.us/download/vnwz-ihnf/application%2Fzip")[0]
        assert type == "xlsx"
