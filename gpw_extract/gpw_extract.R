# extract template

library("raster")
library("rgdal")
library("maptools")

timer <- proc.time()

myVector <- readOGR('/home/userx/Desktop/kfw/shps', 'terra_indigenaPolygon')
# myVector <- readShapePoly('/home/userx/Desktop/extests/shps/terra_indigenaPolygon.shp')

myRaster <- raster('/home/userx/Desktop/gpw/data/glds00ag.asc') 


myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)

myOutput <- myExtract@data
write.table(myOutput, '/home/userx/Desktop/gpw/output/glds00ag.csv', quote=T, row.names=F, sep=",")

out_shp <- "/home/userx/Desktop/gpw/output/glds00ag.shp"
writePolyShape(myExtract, out_shp)

timer <- proc.time() - timer
print(timer)
