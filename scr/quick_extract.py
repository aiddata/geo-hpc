


import rasterstats as rs
import pandas as pd



vector = "/home/userz/git/afghanistan_gie/canal_data/canal_point_grid.geojson"
raster = "/sciclone/aiddata10/REU/projects/afghanistan_gie/distance_to_canals/distance_starts.tif"

output = "/sciclone/aiddata10/REU/projects/afghanistan_gie/extract_data/canal_starts_grid_extract.csv"




stats = rs.zonal_stats(vector, raster, stats="mean", geojson_out=True)



# to csv
x = [i['properties'] for i in stats]

out = pd.DataFrame(x)

out.to_csv(output, index=False, encoding='utf-8')



# to geojson



