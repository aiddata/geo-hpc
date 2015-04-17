# gimms ndvi processed mosaic extract


library("rgdal")
library("raster")
library("maptools")

# ======================

# buffer <- ""
buffer <- "_buffer"

# ======================

readIn <- commandArgs(trailingOnly = TRUE)

year <- readIn[1]
day <- readIn[2]
name <- readIn[3]

timer <- proc.time()


in_base <- "/sciclone/data20/aiddata/REU/data/historic_gimms_ndvi"
out_base <- paste("/sciclone/data20/aiddata/REU/projects/kfw/extracts/historic_ndvi",buffer,"/output/",year,"/",day, sep="")


# myVector <- readOGR('/path/to/shps', 'terra_indigenaPolygon')
myVector <- readShapePoly(paste('/sciclone/data20/aiddata/REU/projects/kfw/shps/terra_indigenaPolygon_id',buffer,'.shp',sep=""))

myRaster <- raster(paste(in_base, year, day, name, sep="/")) 

# myExtract <- extract(disaggregate(myRaster, fact=c(4,4)), myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE)
myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


dir.create(out_base, recursive=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste(out_base,"/historic",buffer,"_extract_",year,"_",day,".csv", sep=""), quote=T, row.names=F, sep=",")

out_shp <- paste(out_base,"/historic",buffer,"_extract_",year,"_",day,".shp", sep="")
writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
# print(timer)

print(paste("historic_extract_hpc.R:",year,day,"completed in",timer[3],'seconds. ', sep=" "))
