# extract for single data


library("rgdal")
library("raster")
library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)


# =========================





project_name <- readIn[1]
shape_name <- readIn[2]
data_path <- readIn[3]
extract_name <- readIn[4]
data_base <- readIn[5]
project_base <- readIn[6]

# =========================


myVector <- readShapePoly(paste(project_base,"/projects/",project_name,"/shps/",shape_name, sep=""))

myRaster <- raster(paste(data_base,"/data/",data_path, sep="")) 

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


colnames(myExtract@data)[length(colnames(myExtract@data))] <- "ad_extract"

out_base <- paste(project_base,"/projects/",project_name,"/extracts/",extract_name, sep = "")

dir.create(out_base, recursive=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste(out_base,"/extract.csv", sep=""), quote=T, row.names=F, sep=",")

# out_shp <- paste(out_base,"/extract.shp", sep="")
# writePolyShape(myExtract, out_shp)


timer <- proc.time() - timer
print(paste("extract.R: (single)",project_name,data_path,"completed in",timer[3],"seconds.", sep=" "))

