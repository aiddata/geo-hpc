# extract for data with data/year/day.ext


library("rgdal")
library("raster")
library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)


# =========================

year <- readIn[1]
day <- readIn[2]
file_name <- readIn[3]

project_name <- readIn[3]
shape_name <- readIn[4]
data_path <- readIn[5]
extract_name <- readIn[6]
data_base <- readIn[7]
project_base <- readIn[8]

# =========================


in_base <- paste(data_base,"/data/",data_path,"/",year, sep="")
out_base <- paste(project_base,"/projects/",project_name,"/extracts/",extract_name,"/output/",year,"/",day, sep="")


myVector <- readShapePoly(paste(project_base,"/projects/",project_name,"/shps/",shape_name,".shp", sep=""))

myRaster <- raster(paste(in_base, file_name, sep="/")) 

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


colnames(myExtract@data)[length(colnames(myExtract@data))] <- "ad_extract"

dir.create(out_base, recursive=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste(out_base,"/extract_",year,"_",day,".csv", sep=""), quote=T, row.names=F, sep=",")

out_shp <- paste(out_base,"/extract_",year,"_",day,".shp", sep="")
writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
print(paste("extract_hpc.R:",year,day,"completed in",timer[3],'seconds. ', sep=" "))
