# generic extract script for sciclone extract jobs


library("rgdal")
library("raster")
# library("maptools")


# =========================

# vector_path <- "./data_01"
# vector_layer <- "NPL_adm2"
# raster <- "./data_01/air_temp_1904_06.tif"


vector_path <- "./data"
vector_layer <- "srtm_polygons"
raster <- "./data/SRTM_500m_clip.tif"

# =========================


# r_vector <- readShapePoly(vector)
r_vector <- readOGR(vector_path, vector_layer)

r_raster <- raster(raster) 



timer1 <- proc.time()

r_extract1 <- extract(r_raster, r_vector, fun=mean, sp=FALSE, weights=FALSE, small=FALSE, na.rm=TRUE)
print(r_extract1)

timer1 <- proc.time() - timer1



timer2 <- proc.time()

r_extract2 <- extract(r_raster, r_vector, fun=mean, sp=FALSE, weights=FALSE, small=TRUE, na.rm=TRUE)
print(r_extract2)

timer2 <- proc.time() - timer2


# timer3 <- proc.time()

# r_extract3 <- extract(r_raster, r_vector, fun=mean, sp=FALSE, weights=TRUE, small=FALSE, na.rm=TRUE)
# print(r_extract3)

# timer3 <- proc.time() - timer3



timer4 <- proc.time()

# same as weights only
r_extract4 <- extract(r_raster, r_vector, fun=mean, sp=FALSE, weights=TRUE, small=TRUE, na.rm=TRUE)
print(r_extract4)

timer4 <- proc.time() - timer4


print(paste("normal extract completed in", timer1[3], "seconds."))

print(paste("small only extract completed in", timer2[3], "seconds."))

# print(paste("weight only extract completed in", timer3[3], "seconds."))

print(paste("small/weighted extract completed in", timer4[3], "seconds."))
