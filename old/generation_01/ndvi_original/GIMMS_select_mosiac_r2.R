# title         : GIMMS_select_mosaic.R
# purpose       : Given a set of MODIS GIMMS files, select a range of them based on a bounding box
#                 and produce a Mosaic to feed into an extraction.
# producer      : D. Runfola (dan@danrunfola.com)
# last update   : In Williamsburg, VA on 2/3/2015
# inputs        : Shape file defining AOI, folder of relevant GIMMS files representing all scenes from a (or multiple) Year/Day combination.
# outputs       : A single TIF which covers the entire AOI from the specified year/date combo.
# notes         : Some errors will appear for broad extents due to the omission of water scenes.
#                 Further, broad scenes will require a high amount of memory to mosaic.
#                 Note, this script also sets NA values to NA, and rescales to NDVI -1 to 1.


library(rgdal)
library(stringr)
library(raster)
library(R.utils)

#------ BEGIN OPTIONS ------
#directory with the MODIS information.
MODIS_dir = 'C:\\MODIS'

#directory to output the results
OUTPUT_dir = 'C:\\MODIS_Test\\Output\\'

#Bounding Box (decimal degrees)
left = -73.725120
top = 5.272106
right = -34.929032
bottom = -31.204757

#Date of Year to use for Extraction
DOY = '257'

#Start and end year to produce Mosaics for (note all raw data must be in target dir for each year)
start_year = 2013
end_year = 2013

#-----END OPTIONS --------

setwd(OUTPUT_dir)
#out_scenes = paste(getwd(),"/",year,"/", sep="")

#Loop through each year:
year = start_year

#Load an array of all files in the MODIS repository (must be local)
items <- strsplit(MODIS_dir, "\n")[[1]]
files <- list.files(items)

NDVI_fun <- function(x) { x * 0.004 }

while(year <= end_year) 
{
  out_scenes = paste(getwd(),"/",year,"/", sep="")
  
  nw_tile_x = floor((180 + left) / 9)
  nw_tile_y = floor((90 - top) / 9)
  
  ne_tile_x = floor(( 180 + right) / 9)
  ne_tile_y = floor(( 90 - top) / 9)

  sw_tile_x = floor(( 180 + left) / 9)
  sw_tile_y = floor(( 90 - bottom) / 9)
  
  se_tile_x = floor(( 180 + right) / 9)
  se_tile_y = floor(( 90 - bottom) / 9)
  
  #copy the appropriate files into a new folder for merging, save these for later analysis if needed
  x_start = nw_tile_x
  x_tile = x_start
  while (x_tile <= ne_tile_x)
  {
    y_start = nw_tile_y
    y_tile = y_start
    while (y_tile <= sw_tile_y)
    {
      #GMOD09Q1.A2014257.08d.latlon.x38y09.5v3.NDVI.tif.gz
      x_form = str_pad(x_tile, 2, pad = "0")
      y_form = str_pad(y_tile, 2, pad = "0")
      file_str = paste("GMOD09Q1.A",year,DOY,".08d.latlon.x",x_form,"y",y_form,".5v3.NDVI.tif.gz",sep="")
      ext_str = paste(MODIS_dir, "\\", file_str, sep="")
      new_str = paste(OUTPUT_dir, year, "\\", gsub('.{3}$', '', file_str),sep="")
      
      #We try here, as some files may not exist due to being in the ocean.
      try(gunzip(ext_str, new_str,overwrite=TRUE, remove=FALSE))
      y_tile = y_tile + 1
    }
    x_tile = x_tile + 1
  }
  
  #Take all extracted tiles for the year and mosaic them.
  dir_year = paste(OUTPUT_dir,year,sep="")
  y_items <- strsplit(dir_year, "\n")[[1]]
  y_files <- list.files(y_items)
  setwd(dir_year)
  
  #Fix each raster to scale between -1 and 1, get rid of clouds / water / junk pixels.  
  for (j in 1:length(y_files))
    {
    r = raster(y_files[j])
    r[ r[] == 251] <- NA
    r[ r[] == 252] <- NA
    r[ r[] == 253] <- NA
    r[ r[] == 254] <- NA
    r[ r[] == 255] <- NA
    
    
    r2 <- calc(r, NDVI_fun)
    t = writeRaster(r2, filename=y_files[j], format="GTiff", overwrite = TRUE)
    gc()
    }
  rm(t)
  rm(r2)
  rm(r)
  #Load all of the rasters into an array for mosaic
  input.rasters <- lapply(y_files, raster)
  input.rasters$fun <- max
  gc()
  mos <- do.call(mosaic, input.rasters)
  
  #Output Mosaic
  out_mos_name = paste(dir_year,".tif",sep="")

  wromos <- writeRaster(mos, filename=out_mos_name, format="GTiff", overwrite=TRUE)
  gc()
  year = year + 1
  
}