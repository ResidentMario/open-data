import pandas as pd
from pandas.parser import CParserError
from pebble import concurrent, ProcessPool
from concurrent.futures import TimeoutError


@concurrent.process(timeout=5)
def get_resource(filepath):
    try:
        return pd.read_csv(filepath)
    except CParserError:
        return None  # Unsuccessful flag.


def dostuff(foo, bar=0):
    return foo + bar

if __name__ == '__main__':
    resources = [
        "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD",
        "https://data.cityofnewyork.us/api/views/8hkx-uppz/rows.csv?accessType=DOWNLOAD"
    ]

    with ProcessPool() as pool:
        # iterator = pool.map(dostuff, elements, timeout=5)
        iterator = pool.map(get_resource, resources, timeout=5)

        while True:
            try:
                results = next(iterator)
                print(results)
            except TimeoutError as error:
                print("Function took longer than %d seconds" % error.args[1])
            except StopIteration:
                break