# generic extract script for sciclone extract jobs


library("rgdal")
library("raster")
# library("maptools")


readIn <- commandArgs(trailingOnly = TRUE)


# =========================

vector_path <- readIn[1]
vector_layer <- readIn[2]
raster <- readIn[3]
output <- readIn[4]
extract_type <- readIn[5]

# =========================


# r_vector <- readShapePoly(vector)
r_vector <- readOGR(vector_path, vector_layer)

r_raster <- raster(raster) 


timer <- proc.time()

if (extract_type == "mean") {
    r_extract <- extract(r_raster, r_vector, fun=extract_type, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)
} else {
    r_extract <- extract(r_raster, r_vector, fun=extract_type, sp=TRUE, na.rm=TRUE)
}

timer <- proc.time() - timer


colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"

dir.create(dirname(output), recursive=TRUE)

write.table(r_extract@data, paste(output, ".csv", sep=""), quote=T, row.names=F, sep=",")

# writePolyShape(r_extract, paste(output,".shp", sep=""))


print(paste("extract completed in", timer[3], "seconds."))
