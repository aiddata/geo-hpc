# generic extract script for sciclone extract jobs


library("rgdal")
library("raster")
library("maptools")


readIn <- commandArgs(trailingOnly = TRUE)


# =========================

vector <- readIn[1]
raster <- readIn[2]
output <- readIn[3]
extract_type <- readIn[4]

# =========================


r_vector <- readShapePoly(vector)

r_raster <- raster(raster) 


timer <- proc.time()

r_extract <- extract(r_raster, r_vector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)
# r_extract <- extract(r_raster, r_vector, fun=mean, sp=TRUE, na.rm=TRUE)

timer <- proc.time() - timer


colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"

dir.create(dirname(output), recursive=TRUE)

write.table(r_extract@data, paste(output, ".csv", sep=""), quote=T, row.names=F, sep=",")

# writePolyShape(r_extract, paste(output,".shp", sep=""))


print(paste("extract completed in", timer[3], "seconds."))
