# gimms ndvi processed mosaic extract


library("rgdal")
library("raster")
library("maptools")


readIn <- commandArgs(trailingOnly = TRUE)

year <- readIn[1]
day <- readIn[2]
name <- readIn[3]

fname <- paste(name, ".tif", sep="")

timer <- proc.time()


in_base <- paste("/sciclone/data20/aiddata/REU/data/ltdr.nascom.nasa.gov/allData/Ver4/ndvi/",year, sep="")
out_base <- paste("/sciclone/data20/aiddata/REU/projects/kfw/extracts/ltdr_ndvi/output/",year,"/",day, sep="")



# myVector <- readOGR('/path/to/shps', 'terra_indigenaPolygon')
myVector <- readShapePoly('/sciclone/data20/aiddata/REU/projects/kfw/shps/terra_indigenaPolygon_id.shp')


myRaster <- raster(paste(in_base, fname, sep="/")) 


# myExtract <- extract(disaggregate(myRaster, fact=c(4,4)), myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE)
myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


x <- match(name, colnames(myExtract@data))
colnames(myExtract@data)[x] <- "ndvi"


dir.create(out_base, recursive=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste(out_base,"/extract_",year,"_",day,".csv", sep=""), quote=T, row.names=F, sep=",")

out_shp <- paste(out_base,"/extract_",year,"_",day,".shp", sep="")
writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
# print(timer)

print(paste("extract_hpc.R:",year,day,"completed in",timer[3],'seconds. ', sep=" "))
