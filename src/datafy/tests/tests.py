import unittest

import sys; sys.path.insert(0, '../../')
from datafy import datafy


class TestGet(unittest.TestCase):

    def testCSV(self):
        r = datafy.get("https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD")[0]
        assert r['ext'] == 'csv'

    def testGeoJSON(self):
        r = datafy.get("https://data.cityofnewyork.us/api/geospatial/arq3-7z49?method=export&format=GeoJSON")[0]
        assert r['ext'] == 'geojson'

    def testJSON(self):
        r = datafy.get("https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.json?accessType=DOWNLOAD")[0]
        assert r['ext'] == 'json'

    def testShapefile(self):
        ret = datafy.get("https://data.cityofnewyork.us/api/geospatial/arq3-7z49?method=export&format=Shapefile")
        assert len(list(filter(lambda d: d['ext'] == 'shp', ret))) > 0

    def testXLSX(self):
        r = datafy.get("https://data.cityofnewyork.us/download/a2ju-qb9a/application%2Fvnd.ms-excel")[0]
        assert r['ext'] == 'xlsx'

    def testXLS(self):
        r = datafy.get("http://www.nyc.gov/html/dot/downloads/excel/int_active.xls")[0]
        assert r['ext'] == 'xls'

    def testXLSOctetStream(self):
        # The mimetype is set incorrectly, and the magic mimetype guess is the non-IAME vnd.ms-office. Tricky case.
        # cf. cf https://github.com/mime-types/ruby-mime-types/issues/98
        r = datafy.get("https://data.cityofnewyork.us/download/dv6z-emb2/application%2Fvnd.ms-office")[0]
        assert r['ext'] == 'xls'

    def testXML(self):
        r = datafy.get("https://data.cityofnewyork.us/download/qb3k-n8mm/application%2Fxml")[0]
        assert (r['ext'] == 'xml' or r['ext'] == 'xsl')  # weird Python mimetypes ideosyncracy.

    def testKML(self):
        r = datafy.get("http://www.nyc.gov/html/dot/downloads/misc/bike_shelters.kml")[0]
        assert r['ext'] == 'kml'

    def testZippedXLSX(self):
        r = datafy.get("https://data.cityofnewyork.us/download/vnwz-ihnf/application%2Fzip")[0]
        assert r['ext'] == 'xlsx'

    def testZippedMixed1(self):
        # Seven Excel files plus a Word document.
        docs = datafy.get("https://data.cityofnewyork.us/download/gua4-p9wg/application%2Fzip")
        filetypes = [t['ext'] for t in docs]
        assert len(list(filter(lambda f: f == 'xlsx', filetypes))) == 7
        assert len(list(filter(lambda f: f == 'docx', filetypes))) == 1

if __name__ == '__main__':
    unittest.main()
