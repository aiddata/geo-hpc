#Ben Dykstra

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

t0 = time.time()

extract_bound = "/Users/benjamindykstra/Documents/AidData/reliability_test/ADM2/NPL_adm2.shp" #sys.argv[1]
mean_surf_bound = "/Users/benjamindykstra/Documents/AidData/string.geojson" #sys.argv[2]
#merge_field_in = sys.argv[3]


gdf_a = gpd.GeoDataFrame.from_file(extract_bound)
mean_surf_df = gpd.GeoDataFrame.from_file(mean_surf_bound)

df = pd.DataFrame.from_csv("/Users/benjamindykstra/Documents/AidData/generic_extract_output.csv")
df['max'] = 0 *len(df)

# #for item in gdf_a:
for row_raw in gdf_a.iterrows():
	row = row_raw[1]
	geom = row['geometry']
	unique_id = row['NAME_2'] #ID field common to both dfs
	mean_surf_df['intersect'] = mean_surf_df['geometry'].intersects(geom)
	tmp_series = mean_surf_df.groupby(by = 'intersect')['unique_dollars'].sum()
	#print(tmp_series)
	df.loc[df['NAME_2'] == unique_id,'max'] = tmp_series[True]
	

# tmp_df = pd.DataFrame.from_csv("/Users/benjamindykstra/Documents/AidData/generic_extract_output.csv")
# tmp_df['max'] = 0*len(tmp_df)


# def geom_intersect(geom, unique_id):
# 	mean_surf_df['intersect'] = mean_surf_df.intersects(geom)
# 	tmp_series = mean_surf_df.groupby(by = 'intersect')['unique_dollars'].sum()
# 	print(unique_id)
# 	tmp_df.loc['NAME_2' == str(unique_id)]['max'] = 1


# gdf_a.apply(lambda x: geom_intersect(x['geometry'], x['NAME_2']), axis = 1)


df["reliability"] = df['mean_aid']/df['max']

df.to_csv("/Users/benjamindykstra/Documents/AidData/reliability_output.csv")

t1 = time.time()
print("Elapsed time is %5.3f." % (t1-t0))




