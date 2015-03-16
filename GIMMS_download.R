# title         : GIMMS_download.R
# purpose       : Download of MODIS GIMMS NDVI files, given pre-identified date and year combination
# producer      : D. Runfola (dan@danrunfola.com)
# last update   : In Williamsburg, VA on 2/3/2015
# inputs        : FTP addresses, day-of-month, year range
# outputs       : a series of NDVI files from the GIMMS Terra repository (ftp://gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/);
# credits       : This script builds on a general MODIS download script by T. Hengl (2010) - http://spatial-analyst.net/wiki/index.php?title=Download_and_resampling_of_MODIS_images

library(RCurl)

#Begin Options ---------------------

#std - "scientific release"
#GMOD - Terra Sensor
#You can change this directory to point to Aqua or Near-Real-Time datasets, if desired.
GIMMS_Repository <- "ftp://gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"

#Output directory for downloads
workd <- "C:\\MODIS\\"

#Day-of-year to download - currently, this script is limited to one month.
#To-do: extend to all months.
#See available days-of-year by selecting a year here: ftp://gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/
DOY <- '257'

#Year range to download
start_year <- 2003
end_year <- 2013

#End Options ----------------------

setwd(workd)

#Loop through each year:
year = start_year

while(year <= end_year) {
  FTP_dir = paste(GIMMS_Repository,year,"/",DOY,"/",sep="")
  print(FTP_dir)
  year <- year+1;
  # get the list of directories (thanks to Barry Rowlingson, via T. Hengl):
  items <- strsplit(getURL(FTP_dir), "\n")[[1]]
  files <- unlist(lapply(strsplit(items, " "), function(x){x[length(x)]}))
  for (i in 1:length(files))
    {
    dl_str = paste(FTP_dir,files[i],sep="")
    dl_str = gsub('.{1}$', '', dl_str)
    
    dest_str = paste(getwd(),"/", files[i], sep="")
    dest_str = gsub('.{1}$', '', dest_str)
    print(dest_str)
    #download scenes
    download.file(dl_str, destfile=dest_str, mode='wb', method='auto', cacheOK=FALSE)
    
    }
}


