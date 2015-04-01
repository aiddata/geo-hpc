# gimms ndvi processed mosaic extract

library("raster")

library("rgdal")
# library("maptools")

timer <- proc.time()

myVector <- readOGR('/home/userx/Desktop/extests/shps', 'terra_indigenaPolygon')
# myVector <- readShapePoly('/home/userx/Desktop/extests/shps/terra_indigenaPolygon.shp')

myRaster <- raster('/home/userx/Desktop/extests/rasterx.tif') 


# myExtract <- extract(myRaster, myVector, weights=TRUE, small=TRUE)

# weighted mean
# v <- extract(r, polys, weights=TRUE, fun=mean)
# equivalent to:
# v <- extract(myRaster, myVector, weights=TRUE)
# sapply(v, function(x) if (!is.null(x)) {sum(apply(x, 1, prod)) / sum(x[,2])} else NA)

# myExtract <- extract(myRaster, myVector, fun=mean, df=TRUE, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)

# myExtract <- extract(myRaster, myVector, weights=TRUE, normalizeWeights=FALSE, small=TRUE)
# sapply(myExtract, function(x) if (!is.null(x)) {sum(apply(x, 1, prod)) / sum(x[[,2]])} else NA)

# myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, na.rm=TRUE)
# myExtract <- extract(disaggregate(myRaster, fact=c(4,4)), myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE)


myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)

myOutput <- myExtract@data
write.table(myOutput, '/home/userx/Desktop/extests/output.csv', quote=T, row.names=F, sep=",")

# out_shp <- "/home/userx/Desktop/extests/output.shp"
# writePolyShape(myExtract, out_shp)

timer <- proc.time() - timer
print(timer)


# sp small + weights
# "Guaimbé",".","Concluído","Regularizada","Terra Indígena","S",NA,"716.9316 Ha","Ativo","Principal","14.84100 Ha",NA,181.320383513884

# sp + weights
# "Guaimbé",".","Concluído","Regularizada","Terra Indígena","S",NA,"716.9316 Ha","Ativo","Principal","14.84100 Ha",NA,181.320383513884
