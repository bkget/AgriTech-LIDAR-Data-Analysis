# AgriTech LIDAR Data Analysis

**Table of content**
  - [Overview](#overview)
  - [Data](#data)
  - [Expected Package Functionalities](#Expected-Package-Functionalities)
  - [Requirements](#requirements)
  - [How to Install](#how-to-install)


## Overview
The aim of this project is to produce an easy to use, reliable and well designed python module that domain experts and data scientists can use to fetch, visualise, and transform publicly available satellite and LIDAR data. In particular, I will use an interface with USGS 3DEP and fetch data using their API. 


## Data 
The USGS 3D Elevation Program (3DEP) provides access to lidar point cloud data from the 3DEP repository. The adoption of cloud storage and computing by 3DEP allows users to work with massive datasets of lidar point cloud data without having to download them to local machines.
The point cloud data is freely accessible from AWS in EPT format in https://s3-us-west-2.amazonaws.com/usgs-lidar-public/. Entwine Point Tile (EPT) is a simple and flexible octree-based storage format for point cloud data. The organization of an EPT dataset contains JSON metadata portions as well as binary point data. The JSON file is core metadata required to interpret the contents of an EPT dataset.


## Expected Package Functionalities
- Data fetching and loading the LIDAR data and return python dictionary contining all years of geopandas file
- Terrain Visualization of Data
- Data Transformation - adding Topographic wetness index (TWI) column to the geopandas dataframe and taking the elevation points output from the USGS LIDAR tool and standardize them to a grid.


## Requirements
- PDAL
- Geopandas
- Laspy
- Shapely


## How to Install?
```
git clone https://github.com/bkget/AgriTech-LIDAR-Data-Analysis.git
cd AgriTech-LIDAR-Data-Analysis
pip install -r requirements.txt
```