
import sys
import time 
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'extract'))
import test_rasterstats as rs

# vector = "./data_01/NPL_adm2.shp"
# raster = "./data_01/air_temp_1904_06.tif"

vector = "./data/srtm_polygons.shp"
raster = "./data/SRTM_500m_clip.tif"



# import pandas as pd

# test_extract = rs.zonal_stats(vector, raster, stats='mean', all_touched=False, weights=False, geojson_out=True)

# test_data = [i['properties'] for i in test_extract]

# test_df = pd.DataFrame(test_data)
# test_df.rename(columns = {'mean':'ad_extract'}, inplace=True)
# test_df['ad_extract'].fillna('NA', inplace=True)
# test_df.to_csv("/home/userz/Desktop/test_pd_out.csv", sep=",", encoding="utf-8", index=False)



print '-----'
Ts1 = time.time()

nmean = rs.zonal_stats(vector, raster, weights=False, all_touched=False, geojson_out=True)

T_run1 = time.time() - Ts1

print '-----'
Ts2 = time.time()

wmean = rs.zonal_stats(vector, raster, weights=False, all_touched=True, geojson_out=True)

T_run2 = time.time() - Ts2

print '-----'
Ts3 = time.time()

wmean = rs.zonal_stats(vector, raster, weights=True, all_touched=True, geojson_out=True)

T_run3 = time.time() - Ts3


print 'Normal Center: ' + str(T_run1)

print 'Normal All Touched: ' + str(T_run2)

print 'Weighted All Touched: ' + str(T_run3)

