#Ben Dykstra & Seth Goodman

#Reliability test extract

#intersect for every single boundary that intersects with unique ones from geojson
#sum for each intersect.

import os
import sys
import json
from shapely.geometry import Polygon, Point, shape, box
import geopandas as gpd
import pandas as pd
import time


#-----------------------------------------------------------------------------------#

t0 = time.time()

# "/Users/benjamindykstra/Documents/AidData/countries/nepal/shapefiles/ADM2/NPL_adm2.shp"
extract_bound = sys.argv[1] 

# "/Users/benjamindykstra/Documents/AidData/string.geojson" 
mean_surf_bound = sys.argv[2] 

# "/Users/benjamindykstra/Documents/AidData/extract_ADM2.csv" 
#contains sum of dollars for extract boundaries
admin_district = sys.argv[3] 

# "/Users/benjamindykstra/Documents/AidData/reliability_output_ADM2.csv"
output = sys.argv[4]

#dataframe with dollar amounts from unique polygons/points
gdf_a = gpd.GeoDataFrame.from_file(extract_bound)
gdf_a['idx'] = range(len(gdf_a)) #creates unique index to merge on

# dataframe from mean surface
mean_surf_df = gpd.GeoDataFrame.from_file(mean_surf_bound)


# "/Users/benjamindykstra/Documents/AidData/extract_ADM2.csv" 

df = pd.DataFrame.from_csv(admin_district)
df['idx'] = range(len(df)) #index to merge with gdf_a
df['max'] = 0 *len(df)


#iterate over shapefile
for row_raw in gdf_a.iterrows():

    row = row_raw[1]
    geom = row['geometry']
    unique_id = row['idx'] #ID field common to both dfs
    mean_surf_df['intersect'] = mean_surf_df['geometry'].intersects(geom)
    tmp_series = mean_surf_df.groupby(by = 'intersect')['unique_dollars'].sum()
    
    df.loc[df['idx'] == unique_id, 'max'] = tmp_series[True]
    

#reliability statistic
df["reliability"] = df['mean_aid']/df['max']

df.to_csv(output)

t1 = time.time()
print("Elapsed time is %5.3f." % (t1-t0))




