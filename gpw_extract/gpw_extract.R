# extract template

library("raster")
library("rgdal")
library("maptools")

timer <- proc.time()

year <- "2000"
yy <- substr(year,3,4)

myVector <- readOGR('/home/userx/Desktop/kfw/shps', 'terra_indigenaPolygon')
# myVector <- readShapePoly('/home/userx/Desktop/extests/shps/terra_indigenaPolygon.shp')

myRaster <- raster(paste('/home/userx/Desktop/gpw/data/glds',yy,'ag.asc',sep="")) 


myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)

myOutput <- myExtract@data
write.table(myOutput, paste('/home/userx/Desktop/gpw/output/',year,'/glds',yy,'ag.csv',sep=""), quote=T, row.names=F, sep=",")

out_shp <- paste('/home/userx/Desktop/gpw/output/',year,'/glds',yy,'ag.shp',sep="")
writePolyShape(myExtract, out_shp)

timer <- proc.time() - timer
print(timer)
