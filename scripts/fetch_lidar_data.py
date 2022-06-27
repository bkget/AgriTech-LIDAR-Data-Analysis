import sys, os
from textwrap import indent
import pdal
import json
import laspy
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
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
            self.bounds = None
            self.crs_polygon = None

            self.las_path = region+'.las'
            self.tif_path = region+'.tif'
            
            self.pipeline = None        
            self.las_file = None
            self.points = None
            self.elevation = None
            self.geopd_df = None

            minx, miny, maxx, maxy = self.get_polygon_boundarys()
            
            if(region != ''):
                self.region = self.check_region(region)
                self.file_location = self.DEFAULT_LOCATION + self.region + "/ept.json"
            else:
                self.file_location, self.region = self.get_region_from_bounds(minx, miny, maxx, maxy)
            
            print("The name of the region is: ", self.region) 

            self._logger.info('Successfully Instantiated DataFetcher Class Object') 
         
        except Exception as e:
            self._logger.exception(f"Fail to initialize FetchLidarData Class. {e}")


    def get_pipeline(self) -> None:
        """Create Geographic LiDAR Pipeline data 

        Parameters
        ----------
            None
       
        returns:
            gpd (geopandas Dataframe): returns a geopandas dataframe

        Returns
        -------
            Pdal pipeline object
        """
        try: 

            self.get_polygon_edges()

            pipeline =  self._file_handler.read_json("pipeline_template")

            pipeline['pipeline'][0]['bounds'] = self.bounds
            pipeline['pipeline'][0]['filename'] = self.file_location
            
            pipeline['pipeline'][1]['polygon'] = self.crs_polygon
            
            pipeline['pipeline'][4]['out_srs'] = f'EPSG:{self.epsg}'

            pipeline['pipeline'][7]['filename'] = self.las_path
            pipeline['pipeline'][8]['filename'] = self.tif_path


            print("Data Link : " , pipeline['pipeline'][0]['filename'])
            self.pipeline = pipeline
                

            pipe = pdal.Pipeline(json.dumps(pipeline))
            
            self._logger.info('Successfully generate Pipeline data')

            return pipe

        except Exception as e:
            self._logger.exception('Failed to get Pdal Pipeline')


    def get_polygon_boundarys(self) -> tuple:
        """ Return the boudary points

        Parameters:
        ----------
            None

        Returns:
        --------
            tuple
                minx, miny, maxx, maxy
        
        """
        polygon_df = gpd.GeoDataFrame([self.polygon], columns=['geometry'])
        
        polygon_df.set_crs(epsg=self.epsg, inplace=True)
        polygon_df['geometry'] = polygon_df['geometry'].to_crs(epsg=3857)
        minx, miny, maxx, maxy = polygon_df['geometry'][0].bounds

        return minx, miny, maxx, maxy


    def get_polygon_edges(self) -> None:
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
            Returns the minimum longitude (minx), the minimum latitude (miny), the maximum longitude (maxx), 
            and the maximum latitude (maxy) values of the polygon
        """
        try: 
            polygon_df = gpd.GeoDataFrame([self.polygon], columns=['geometry'])
        
            polygon_df.set_crs(epsg=self.epsg, inplace=True)
            polygon_df['geometry'] = polygon_df['geometry'].to_crs(epsg=3857)
            minx, miny, maxx, maxy = polygon_df['geometry'][0].bounds
            
            polygon_input = 'POLYGON(('

            xcord, ycord = polygon_df['geometry'][0].exterior.coords.xy
            for x, y in zip(list(xcord), list(ycord)):
                polygon_input += f'{x} {y}, '
            polygon_input = polygon_input[:-2]
            polygon_input += '))'
            
            
            self.bounds = f"({[minx, maxx]},{[miny,maxy]})"
            self.crs_polygon = polygon_input

            self._logger.info('Successfully Extracted Polygon Edges and Polygon Cropping Bounds')
            
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
        """Retrieves Data from the AWS Dataset, builds the cloud points from it and assignes and 
        stores the original cloud points and original elevation geopandas dataframe.

        Parameters
        ----------
            None

        Returns
        -------
            None
        """
        try:
            print("Pipeline is running ...")
            self.get_pipeline()
            pdal_pipe = pdal.Pipeline(json.dumps(self.pipeline))
            pdal_pipe.execute() 

            self._logger.info("Data is fetched successfully")

        except Exception as e:
            self._logger.exception("Failed to retrieve the data!")
            sys.exit(1)     

        
    def read_laz(self):
        '''
        Read the generated Las file and return a laspy read las file.
        '''
        try:
            print("Reading Las File from :", self.las_path)
            las = laspy.read(self.las_path)
            self.las_file = las
            return las
        
        except FileNotFoundError:
            self._logger.exception("Las file not found!")

    
    def generate_points_elevation(self):
        '''
        Return Points (x, y) and elevation (z)
        '''
        print("Generating Points from las File ...")
        points = [Point(x, y) for x,y in zip(self.las_file.x, self.las_file.y)]
        elevation = np.array(self.las_file.z)
        
        self.points, self.elevation = points, elevation

        self._logger.info("Elevation points are generated successfully") 

        return points, elevation


    def generate_geopandasdf(self)->gpd.GeoDataFrame:
        '''
        Generate Geopandas data frame from elevation and geometic points

        Parameter:
        ----------
            None

        Returns:
        --------
            A Geopandas Dataframe

        '''
        self.read_laz()
        self.generate_points_elevation()
        
        print("Making Geopandas Data Frame...")
        geopanda_df = gpd.GeoDataFrame({"elevation": self.elevation, "geometry":self.points})
        geopanda_df.set_geometry('geometry')
        geopanda_df.set_crs(epsg=self.epsg, inplace=True)
        
        self.geopd_df = geopanda_df 
        self._logger.info("Geopandas Dataframe generated successfully")

        return geopanda_df