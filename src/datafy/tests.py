import unittest
import pandas as pd
import numpy as np
import datafy


class TestGet(unittest.TestCase):

    def testCSV(self):
        data, type = datafy.get("https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD")
        assert type == "csv"

    def testGeoJSON(self):
        data, type = datafy.get("https://data.cityofnewyork.us/api/geospatial/arq3-7z49?method=export&format=GeoJSON")
        assert type == "geojson"