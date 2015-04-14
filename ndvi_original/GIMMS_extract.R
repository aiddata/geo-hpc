# title         : GIMMS_extract.R
# purpose       : Given standardized mosaics from GIMMS, extract to a unit of interest.
# producer      : D. Runfola (dan@danrunfola.com)
# last update   : In Williamsburg, VA on 3/3/2015
# inputs        : GIMMS MODIS Global Coverages 
# outputs       : Shapefile containing extracted data.
library(GISTools)
library(raster)

shapefile = "C:\\scratch\\terra_indigenaPolygon.shp"

data_dir = "C:\\GIMMS_Sept\\"

out_file = "C:\\scratch\\summary_r4.shp"

#--------------------

summary_shp = readShapePoly(shapefile)

y_files <- list.files(data_dir)
 
for (j in 1:length(y_files))
{
  rLayer = raster(paste(data_dir,y_files[j],sep=""))
  summary_shp <- extract(rLayer, summary_shp, fun='mean', na.rm=TRUE, sp=TRUE, small=TRUE)
}

writePolyShape(summary_shp, out_file)