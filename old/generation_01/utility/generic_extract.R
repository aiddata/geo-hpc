# extract data based on user inputs

# inputs:
#   vector_in - <required> file path of vector used for extraction
#   raster_in - <required> file path of raster which data is to be extracted from (must be tif or asc for now)
#   output_in - <required> name of output file with no extension (used for shapefile and csv output)
#   field_in  - <optional> name of field used for output csv and shapefile (not enforced but should be under 10 characters or shapefile will truncate). uses raster file name by default

library("rgdal")
library("raster")
library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)


vector_in <- readIn[1]
raster_in <- readIn[2]
output_in <- readIn[3]


field_in <- ""
if (length(readIn) == 4) {
    field_in <- readIn[4]
}

if ( is.na(vector_in) || is.na(raster_in) || is.na(output_in) ) {
    stop("Must include all input fields.")
}

# check input vector exists
if (file.exists(vector_in) == FALSE) {
    stop("Could not find vector input file.")
}

# check input raster exists
if (file.exists(raster_in) == FALSE) {
    stop("Could not find raster input file.")
}

# check input raster is valid file type
rext <- substring(basename(raster_in), nchar(basename(raster_in))-3)
if (rext != ".tif" && rext != ".asc") {
    stop("Input raster file is not a valid type.")
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

myVector <- readShapePoly(vector_in)

myRaster <- raster(raster_in) 

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


if (field_in != "") {
    colnames(myExtract@data)[length(colnames(myExtract@data))] <- field_in

} else {
    colnames(myExtract@data)[length(colnames(myExtract@data))] <- "ad_extract"
}


write.table(myExtract@data, paste(output_in,'.csv',sep=""), quote=T, row.names=F, sep=",")

# writePolyShape(myExtract, paste(output_in,'.shp',sep=""))


timer <- proc.time() - timer
print(paste("extract completed in", timer[3], "seconds."))
