# title         : MODIS_GIMMS_prep.R
# purpose       : Given mosaics of MODIS GIMMS, rescale them to 0 to 1 NDVI values (truncate below 0), remove NA values.
# producer      : D. Runfola (dan@danrunfola.com)
# last update   : In Williamsburg, VA on 2/4/2015
# inputs        : GIMMS MODIS Global Coverages 
# outputs       : GIMMS MODIS Global Coverages with appropriate NA value removal and scaling.


library(raster)

#------ BEGIN OPTIONS ------
#directory with the AVHRR information.
MODIS_dir = 'C:\\MODIS_out'

#directory to output the results
processing_dir = 'C:\\MODIS_out\\Processing'
output_dir = 'C:\\MODIS_out\\Output'

#-----END OPTIONS --------

setwd(MODIS_dir)

#Loop through each year:
year = start_year

#Load an array of all files in the AVHRR GIMMS repository (must be local)
items <- strsplit(MODIS_dir, "\n")[[1]]
files <- list.files(items, pattern="*tif*")

for(i in 1:length(files)) 
{

  r = raster(files[i])
  #Set water and bad pixels to NA
  r[ r[] < 0] <- NA  
  
  r <- calc(r, fun=function(x){float(r/10000)})
 
  fname_out = paste(output_dir,"\\",year,"_NDVI.tif",sep="")
  wromos <- writeRaster(r, filename=fname_out, format="GTiff", overwrite=TRUE)
  gc()

}