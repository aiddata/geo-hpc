# extract for data with data/year.ext


library("rgdal")
library("raster")
library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)


# =========================

vector <- readIn[1]
raster <- readIn[2]
output <- readIn[3]

# =========================


myVector <- readShapePoly(vector)

myRaster <- raster(raster) 

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


colnames(myExtract@data)[length(colnames(myExtract@data))] <- "ad_extract"


dir.create(dirname(output), recursive=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste(output, ".csv", sep=""), quote=T, row.names=F, sep=",")

# out_shp <- paste(output,".shp", sep="")
# writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
print(paste("extract completed in", timer[3], "seconds."))
