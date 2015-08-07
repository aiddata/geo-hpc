# generic extract script for sciclone extract jobs


library("rgdal")
library("raster")
library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)


# =========================

vector <- readIn[1]
raster <- readIn[2]
output <- readIn[3]
extract_type <- readIn[4]

# =========================


myVector <- readShapePoly(vector)

# myVector <- readOGR("path", "layer")
# myVector <- readOGR("full", "OGRGeoJSON")

myRaster <- raster(raster) 

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


colnames(myExtract@data)[length(colnames(myExtract@data))] <- "ad_extract"


dir.create(dirname(output), recursive=TRUE)

write.table(myExtract@data, paste(output, ".csv", sep=""), quote=T, row.names=F, sep=",")

# writePolyShape(myExtract, paste(output,".shp", sep=""))


timer <- proc.time() - timer
print(paste("extract completed in", timer[3], "seconds."))
