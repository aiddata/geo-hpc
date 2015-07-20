# extract for data with data/year/month.ext


library("rgdal")
library("raster")
library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)


# =========================

year <- readIn[1]
month <- readIn[2]
file_name <- readIn[3]

project_name <- readIn[4]
shape_name <- readIn[5]
data_path <- readIn[6]
extract_name <- readIn[7]
data_base <- readIn[8]
project_base <- readIn[9]

# =========================


myVector <- readShapePoly(paste(project_base,"/projects/",project_name,"/shps/",shape_name, sep=""))

myRaster <- raster(paste(data_base,"/data/",data_path,"/",year,"/",file_name, sep="")) 

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


colnames(myExtract@data)[length(colnames(myExtract@data))] <- "ad_extract"

out_base <- paste(project_base,"/projects/",project_name,"/extracts/",extract_name,"/output/",year,"/",month, sep="")

dir.create(out_base, recursive=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste(out_base,"/extract_",year,"_",month,".csv", sep=""), quote=T, row.names=F, sep=",")

out_shp <- paste(out_base,"/extract_",year,"_",month,".shp", sep="")
writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
print(paste("extract_hpc.R:",year,month,"completed in",timer[3],'seconds. ', sep=" "))
