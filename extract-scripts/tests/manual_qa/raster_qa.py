import sys
import os
import time


dir_base = os.path.dirname(os.path.abspath(__file__))



raster = sys.argv[1]

output_dir = sys.argv[2]

vector = dir_base + "/raster_qa_points.geojson"





import rasterstats as rs
from numpy import isnan

python_output = output_dir + "/python_qa_output.csv"

Te_start = int(time.time())
stats = rs.zonal_stats(vector, raster, stats="mean", copy_properties=True)#, all_touched=True)

for i in stats:
    i["ad_extract"] = i.pop("mean")
    try:
        if isnan(i["ad_extract"]):
            i["ad_extract"] = "NA"
    except:
        i["ad_extract"] = "NA"


out = open(python_output, "w")
out.write(rs.utils.stats_to_csv(stats))

Te_run = int(time.time() - Te_start)
print 'python extract ('+ python_output +') completed in '+ str(Te_run) +' seconds'





from rpy2.robjects.packages import importr
from rpy2 import robjects

rpy2_output = output_dir + "/rpy2_qa_output.csv"

# load packages
rlib_rgdal = importr("rgdal")
rlib_raster = importr("raster")

# open vector files
r_vector = rlib_rgdal.readOGR(vector, "OGRGeoJSON")

# open raster
r_raster = rlib_raster.raster(raster)

kwargs = {"fun":robjects.r.mean, "sp":True, "na.rm":True}

Te_start = int(time.time())

robjects.r.assign('r_extract', rlib_raster.extract(r_raster, r_vector, **kwargs))

Te_run = int(time.time() - Te_start)
print 'rpy2 extract ('+ rpy2_output +') completed in '+ str(Te_run) +' seconds'


robjects.r.assign('r_output', rpy2_output)

robjects.r('colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"')
robjects.r('write.table(r_extract@data, r_output, quote=T, row.names=F, sep=",")')




