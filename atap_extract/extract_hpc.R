# gimms ndvi processed mosaic extract


library("rgdal")
library("raster")
library("maptools")


readIn <- commandArgs(trailingOnly = TRUE)

year <- readIn[1]
day <- readIn[2]
name <- readIn[3]

atap_type <- readIn[4]

atap_abbr <- "air_temp"
if (atap_type == "terrestrial_precipitation") {
	atap_abbr <- "precip"
}

timer <- proc.time()


in_base <- paste("/sciclone/data20/aiddata/REU/data/",atap_type,sep="")
out_base <- paste("/sciclone/data20/aiddata/REU/projects/kfw/extracts/",atap_type,"/output/",year,"/",day, sep="")



# myVector <- readOGR('/path/to/shps', 'terra_indigenaPolygon')
myVector <- readShapePoly('/sciclone/data20/aiddata/REU/projects/kfw/shps/terra_indigenaPolygon.shp')


myRaster <- raster(paste(in_base, year, day, name, sep="/")) 


# myExtract <- extract(disaggregate(myRaster, fact=c(4,4)), myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE)
myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


x <- match(paste(atap_abbr,"_",year,"_",day,sep=""), colnames(myExtract@data))
colnames(myExtract@data)[x] <- atap_abbr


dir.create(out_base, recursive=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste(out_base,"/",atap_abbr,"_extract_",year,"_",day,".csv", sep=""), quote=T, row.names=F, sep=",")

out_shp <- paste(out_base,"/",atap_abbr,"_extract_",year,"_",day,".shp", sep="")
writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
# print(timer)

print(paste("extract_hpc.R:",year,day,"completed in",timer[3],'seconds. ', sep=" "))
