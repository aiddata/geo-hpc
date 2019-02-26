


import rasterstats as rs
import pandas as pd



vector = "/path/to/some/vector.geojson"
raster = "/sciclone/aiddata10/REU/some/raster.tif"

output = "/path/to/results.csv"



stats = rs.zonal_stats(vector, raster, stats="mean", geojson_out=True)



# to csv
x = [i['properties'] for i in stats]

out = pd.DataFrame(x)

out.to_csv(output, index=False, encoding='utf-8')



# to geojson



