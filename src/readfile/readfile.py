import numpy as np
import pandas as pd
from pandas.parser import CParserError


# def parse_resource(resource_uri):


# def get_protocol(filepath, hint=""):
#     if hint:
#         ext = hint.lower()
#     else:
#         ext = filepath.split(".")[-1].lower()
#     if ext == "csv":
#         try:
#             readin = pd.read_csv(filepath)
#             return pd.read_csv
#         except CParserError:
#             raise IOError("{0} could not be read by the pandas CSV parser.".format(filepath))
#     else:
#         raise IOError("The file was of an unrecognizable type anc could not be read.")