# gimms ndvi processed mosaic extract


library("rgdal")
library("raster")
library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)


# =========================

year <- readIn[1]
file_name <- readIn[2]

project_name <- readIn[3]
shape_name <- readIn[4]
data_path <- readIn[5]
extract_name <- readIn[6]
extract_field <- readIn[7]

# =========================


in_base <- paste("/sciclone/aiddata10/REU/data/",data_path, sep="")
out_base <- paste("/sciclone/aiddata10/REU/projects/",project_name,"/extracts/",extract_name,"/output/",year, sep="")


myVector <- readShapePoly(paste('/sciclone/aiddata10/REU/projects/',project_name,'/shps/',shape_name,'.shp', sep=""))
# myVector <- readOGR('/path/to/shps', 'terra_indigenaPolygon')

myRaster <- raster(paste(in_base,file_name, sep="/")) 

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


colnames(myExtract@data)[length(colnames(myExtract@data))] <- extract_field


dir.create(out_base, recursive=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste(out_base,"/extract_",year,".csv", sep=""), quote=T, row.names=F, sep=",")

out_shp <- paste(out_base,"/extract_",year,".shp", sep="")
writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
print(paste("yearly_extract_hpc.R:",project_name,data_path,year,"extract completed in",timer[3],'seconds. ', sep=" "))
