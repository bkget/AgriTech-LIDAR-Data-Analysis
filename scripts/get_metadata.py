import re
import json
import urllib3
import pandas as pd
from logger import get_logger
from file_handler import FileHandler

DEFAULT_URL = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/"
DEFAULT_FILENAME = "../data/usgs_3dep_regions_name"

class GetMetadata():

    def __init__(self, name: str = DEFAULT_FILENAME, target_url: str = DEFAULT_URL) -> None:
        self.filename = name
        self._http = urllib3.PoolManager() 
        self.url = target_url
        self.file_handler = FileHandler()
        self.logger = get_logger("GetMetadata")


