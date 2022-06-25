import sys
# import pdal
import json
import laspy
import numpy as np
import pandas as pd
import geopandas as gpd
from bounds import Bounds
from shapely.geometry import box, Point, Polygon
from file_handler import FileHandler
from logger import get_logger


class FetchLidarData():

    def __init__(self, polygon: Polygon, epsg: str, region: str = '') -> None:
        try:   
            self._file_handler = FileHandler()
            self.DEFAULT_LOCATION = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/"
            minx, miny, maxx, maxy = self.get_polygon_edges(polygon, epsg)
            self._metadata = self._file_handler.read_csv("usgs_3dep_metadata")
            self.epsg = epsg
            self.boundary = ([minx, miny] , [maxx, maxy])

            if(region != ''):
                self.region = self.check_region(region)
                self.file_location = self.DEFAULT_LOCATION + self.region + "/ept.json"
            else:
                self.file_location, self.region = self.get_region_from_bounds(minx, miny, maxx, maxy)
            
            print(self.region)

            self.load_pipeline(self.region, self.boundary, self.epsg)           

            get_logger.info('Successfully Instantiated DataFetcher Class Object') 
         
        except Exception as e:
            print(f"Fail to initialize FetchLidarData Class. {e}")


