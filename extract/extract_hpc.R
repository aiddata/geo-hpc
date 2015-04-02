# gimms ndvi processed mosaic extract

library("raster")

# library("rgdal")
library("maptools")


readIn <- commandArgs(trailingOnly = TRUE)

year <- readIn[1]
day <- readIn[2]
name <- readIn[3]


timer <- proc.time()


base <- "/sciclone/data20/aiddata/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI"



# myVector <- readOGR('/path/to/shps', 'terra_indigenaPolygon')
myVector <- readShapePoly('~/kfw/extract/shps/terra_indigenaPolygon.shp')


myRaster <- raster(paste(base, year, day, name, sep="/")) 


# myExtract <- extract(disaggregate(myRaster, fact=c(4,4)), myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE)
myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


# myOutput <- myExtract@data
# write.table(myOutput, '/home/userx/Desktop/extests/output.csv', quote=T, row.names=F, sep=",")

out_shp <- paste("~/kfw/extract/output/extract_",year,"_",day,".shp", sep="")
writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
print(timer)
