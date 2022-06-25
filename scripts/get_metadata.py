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


    def get_name_and_year(self, location: str) -> tuple:
        """ Extracts region name and year from the USGS 3DEP region detail text file.

        Returns:
            tuple: A tuple containing the name of the region and year
        """

        regex = '20[0-9][0-9]+'
        is_found = re.search(regex, location)
        if(is_found):
            return location[:-5], location[-4:]
        else:
            return location[:-5], ""

    def save_metadata(self):
        """ Save metadata for all EPT resources on AWS
        """ 
        filenames = self.file_handler.read_txt(self.filename)
        df = pd.DataFrame(columns=['filename', 'region',
                        'year', 'xmin', 'xmax', 'ymin', 'ymax', 'points'])

        index = 0
        for file in filenames:
            r = self._http.request('GET', self.url + file + "ept.json")
            if r.status == 200:
                j = json.loads(r.data)
                region, year = self.get_name_and_year(file) 
                df = df.append({
                    'region': region,
                    'year': year,
                    'xmin': j['bounds'][0],
                    'xmax': j['bounds'][3],
                    'ymin': j['bounds'][1],
                    'ymax': j['bounds'][4],
                    'points': j['points']}, ignore_index=True)

            if(index % 100 == 0):
                print(f"Reading in progress: {((index / len(filenames)) * 100):.2f}%")
                index += 1
        else:
            self.logger.exception(f"Connection problem at: {file}")
          
        self.file_handler.save_metadata_to_csv(df, "../data/usgs_3dep_metadata")

    
if __name__ == "__main__":
    gm = GetMetadata()
    gm.save_metadata()