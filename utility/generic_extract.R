# extract data based on user inputs

library("raster")
library("rgdal")
library("maptools")

timer <- proc.time()

# inputs:
#	vector_in - file path of vector used for extraction
#  	raster_in - file path of raster which data is to be extracted from
# 	output_in - name of output file with no extension (used for shapefile and csv output)

readIn <- commandArgs(trailingOnly = TRUE)

vector_in <- readIn[1]
raster_in <- readIn[2]
output_in <- readIn[3]

if ( is.na(vector_in) || is.na(raster_in) || is.na(output_in) ) {
	stop("Must include all input fields.")
}

# check input vector exists
if ( file.exists(vector_in) == FALSE) {
	stop("Could not find vector input file.")
}

# check input raster exists
if ( file.exists(raster_in) == FALSE) {
	stop("Could not find raster input file.")
}

# get output path, check if exists
# try to create if it does not exist
out_dir <- dirname(output_in)
if ( file.exists(out_dir) == FALSE) {
	cdir <- dir.create(out_dir, recursive=TRUE) 
	if (cdir == FALSE) {
		stop("Could not create output directory.")
	}
}

# myVector <- readOGR('/home/userx/Desktop/kfw/shps', 'terra_indigenaPolygon')
myVector <- readShapePoly(vector_in)

myRaster <- raster(raster_in) 

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)

myOutput <- myExtract@data

write.table(myOutput, paste(output_in,'.csv',sep=""), quote=T, row.names=F, sep=",")

writePolyShape(myExtract, paste(output_in,'.shp',sep=""))

timer <- proc.time() - timer
print(timer)
