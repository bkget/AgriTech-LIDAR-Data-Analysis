import sys, os
from textwrap import indent
import pdal
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

    def __init__(self, polygon: Polygon, epsg: int, region: str = '') -> None:
        try:   
            self._file_handler = FileHandler()
            self._logger = get_logger("FetchLidarData")
            self.DEFAULT_LOCATION = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/"
            self.polygon = polygon
            self.epsg = epsg            
            
            minx, miny, maxx, maxy = self.get_polygon_edges(self.polygon, self.epsg)
            self._metadata = self._file_handler.read_csv("usgs_3dep_metadata")
            self.boundary = ([minx, miny] , [maxx, maxy])
            
            if(region != ''):
                self.region = self.check_region(region)
                self.file_location = self.DEFAULT_LOCATION + self.region + "/ept.json"
            else:
                self.file_location, self.region = self.get_region_from_bounds(minx, miny, maxx, maxy)
            
            print("The name of the region is: ", self.region)

            self.pipeline = self.get_pipeline(self.region, self.boundary, self.epsg)  

            self._logger.info('Successfully Instantiated DataFetcher Class Object') 
         
        except Exception as e:
            self._logger.exception(f"Fail to initialize FetchLidarData Class. {e}")


    def get_pipeline(self, region: str, bounds: Bounds, epsg:int) -> pdal.Pipeline:
        """Loads Geographic LiDAR Pipeline data 

        Parameters
        ----------
        region (str): this argument inputs the name of the region to extract the pipeline data
        bounds (tuple): these are coordinates in the form of latitude and longitude 
        epsg (int): this argument specifies the input coordinate reference system
       
        returns:
            gpd (geopandas Dataframe): returns a geopandas dataframe

        Returns
        -------
        Pdal pipeline object
        """
        try:
            pipeline = {
                "pipeline": [
                    {
                        "bounds": f"{bounds}",
                        "filename": f"https://s3-us-west-2.amazonaws.com/usgs-lidar-public/{region}/ept.json",
                        "type": "readers.ept",
                        "tag": "readdata"
                    },
                    {
                        "limits": "Classification![7:7]",
                        "type": "filters.range",
                        "tag": "nonoise"
                    },
                    {
                        "assignment": "Classification[:]=0",
                        "tag": "wipeclasses",
                        "type": "filters.assign"
                    },
                    {
                        "out_srs": f"EPSG:{epsg}",
                        "tag": "reprojectUTM",
                        "type": "filters.reprojection"
                    },
                    {
                        "tag": "groundify",
                        "type": "filters.smrf"
                    },
                    {
                        "limits": "Classification[2:2]",
                        "type": "filters.range",
                        "tag": "classify"
                    },
                    {
                        "filename": f"{region}.laz",
                        "inputs": [ "classify" ],
                        "tag": "writerslas",
                        "type": "writers.las"
                    },
                    {
                        "filename": f"{region}.tif",
                        "gdalopts": "tiled=yes,     compress=deflate",
                        "inputs": [ "writerslas" ],
                        "nodata": -9999,
                        "output_type": "idw",
                        "resolution": 1,
                        "type": "writers.gdal",
                        "window_size": 6
                    }
                ]
            }            
            
            pipe = pdal.Pipeline(json.dumps(pipeline))
            
            self._logger.info('Successfully get Pipeline data')

            return pipe

        except Exception as e:
            self._logger.exception('Failed to get Pdal Pipeline')

    
    def get_polygon_edges(self, polygon: Polygon, epsg: int) -> tuple:
        """To extract polygon bounds and assign polygon cropping bounds.

        Parameters
        ----------
        polygon : Polygon
            Polygon object describing the boundary of the location required
        epsg : int
            CRS system on which the polygon is constructed on

        Returns
        -------
        tuple
            Returns bounds of the polygon provided(minx, miny, maxx, maxy)
        """
        try:
            grid = gpd.GeoDataFrame([polygon], columns=["geometry"])
            grid.set_crs(epsg=epsg, inplace=True)

            grid['geometry'] = grid.geometry.to_crs(epsg=3857)

            minx, miny, maxx, maxy = grid.geometry[0].bounds
            # bounds: ([minx, maxx], [miny, maxy])
            self.extraction_bounds = f"({[minx, maxx]},{[miny,maxy]})"
            
            # Cropping Bounds
            self.polygon_cropping = self.get_crop_polygon(grid.geometry[0])

            grid['geometry'] = grid.geometry.to_crs(epsg=epsg)
            self.geo_df = grid
            
            self._logger.info('Successfully Extracted Polygon Edges and Polygon Cropping Bounds')
            
            return minx, miny, maxx, maxy

        except Exception as e:
            self._logger.exception('Failed to Extract Polygon Edges and Polygon Cropping Bounds')

    def get_crop_polygon(self, polygon: Polygon) -> str:
        """Calculates Polygons Cropping string used when building Pdal's crop pipeline.

        Parameters
        ----------
        polygon: Polygon
            Polygon object describing the boundary of the location required

        Returns
        -------
        str
            Cropping string used by Pdal's crop pipeline
        """
        polygon_cords = 'POLYGON(('
        for i in list(polygon.exterior.coords):
            polygon_cords += f'{i[0]} {i[1]},'

        polygon_cords = polygon_cords[:-1] + '))'

        return polygon_cords

        
    def check_region(self, region: str) -> str:
        """Checks if a region provided is within the file name folders in the AWS dataset.
        Parameters
        ----------
        region : str
            Proabable file name of a folder in the AWS dataset
        Returns
        -------
        str
            Returns the same regions folder file name if it was successfully located
        """
        with open('../data/usgs_3dep_regions_name.txt', 'r') as locations:
            locations_list = locations.read().splitlines()

        if(region in locations_list):
            return region
        else:
            self._logger.exception('Region is Not Available')

    def get_region_from_bounds(self, minx: float, miny: float, maxx: float, maxy: float, indx: int = 1) -> str:
        """Searchs for a region which satisfies the polygon defined from the available boundaries in AWS dataset.
        Parameters
        ----------
        minx : float
            Minimum longitude value of the polygon
        miny : float
            Minimum latitude value of the polygon
        maxx : float
            Maximum longitude value of the polygon
        maxy : float
            Maximum latitude value of the polygon
        indx : int, optional
            Bound indexing, to select the first or other access url's of multiple values for a region
        Returns
        -------
        str
            Access url to retrieve the data from the AWS dataset
        --------------------------
        Args:
            bounds (Bounds): Geometry object describing the boundary of interest for fetching point cloud data

        Returns:
            pd.DataFrame: Resource metadata for regions enclosing the given boundary.
        """

        filtered_df = self._metadata.loc[
            (self._metadata['xmin'] <= minx)
            & (self._metadata['xmax'] >= maxx)
            & (self._metadata['ymin'] <= miny)
            & (self._metadata['ymax'] >= maxy)
        ]

        return self.DEFAULT_LOCATION + filtered_df["filename"] + "/ept.json", filtered_df["region"]


    def get_data(self):
        """Retrieves Data from the AWS Dataset, builds the cloud points from it and 
        assignes and stores the original cloud points and original elevation geopandas dataframe.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        try:
            self.data_count = self.pipeline.execute()
            print("Fetching Point Cloud Data Complete!")

            self.create_cloud_points()
            self.original_cloud_points = self.cloud_points
            self.original_elevation_geodf = self.get_elevation_geodf()
            
            self._logger.info("Data is retrieved successfully")

        except Exception as e:
            self._logger.exception("Failed to retrieve the data.")
            sys.exit(1) 


    def create_cloud_points(self):
        """Creates Cloud Points from the retrieved Pipeline Arrays consisting of other unwanted data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        try:
            cloud_points = []
            pipeline_arrays = self.pipeline.arrays
            for row in pipeline_arrays[0]:
                lst = row.tolist()[-3:]
                cloud_points.append(lst)

            cloud_points = np.array(cloud_points)

            self.cloud_points = cloud_points

        except:
            self._logger.exception('Failed to create cloud points')
            sys.exit(1)


    def get_elevation_geodf(self) -> gpd.GeoDataFrame:
        """Calculates and returns a geopandas elevation dataframe from the cloud points generated before.

        Parameters
        ----------
        None

        Returns
        -------
        gpd.GeoDataFrame
            Geopandas Dataframe with Elevation and coordinate points referenced as Geometry points
        """
        elevation = gpd.GeoDataFrame()
        elevations = []
        points = []
        for row in self.cloud_points:
            elevations.append(row[2])
            point = Point(row[0], row[1])
            points.append(point)

        elevation['elevation'] = elevations
        elevation['geometry'] = points
        elevation.set_crs(epsg=self.epsg, inplace=True)

        self.elevation_geodf = elevation

        return self.elevation_geodf

if __name__ == "__main__":
    MINX, MINY, MAXX, MAXY = [-93.756155, 41.918015, -93.747334, 41.921429]
    polygon = Polygon(((MINX, MINY), 
                       (MINX, MAXY),
                       (MAXX, MAXY), 
                       (MAXX, MINY), 
                       (MINX, MINY)) )

    df = FetchLidarData(polygon=polygon, region="IA_FullState", epsg=4326) 

    df.get_data()

    elevation = df.get_elevation_geodf()

    elevation.sample(10)