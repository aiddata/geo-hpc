# extract template

library("raster")
library("rgdal")
library("maptools")


# --------------------------------------------------

base <- "/home/userz/globus-data"
project_name <- "liberia"
shape_name <- "LBR_adm3"

# --------------------------------------------------

myVector <- readOGR(paste(base,'/projects/',project_name,'/shps', sep=""), shape_name)
# myVector <- readShapePoly(paste(base,'/projects/',project_name,'/shps/',shape_name,'.shp', sep=""))


years <- list.files(paste(base,"/data/gpw_v3", sep=""))

for (year in years) {

    timer <- proc.time()

    print(year)

    # year <- "2000"
    yy <- substr(year,3,4)


    myRaster <- raster(paste(base,'/data/gpw_v3/',year,'/00/glds',yy,'ag.asc', sep="")) 

    myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)


    # get output path, check if exists
    # try to create if it does not exist
    out_base <- paste(base,'/projects/',project_name,'/extracts/gpw_v3/output/',year, sep="")

    if (file.exists(out_base) == FALSE) {
        cdir <- dir.create(out_base, recursive=TRUE) 
        if (cdir == FALSE) {
            stop("Could not create output directory.")
        }
    }


    myOutput <- myExtract@data
    write.table(myOutput, paste(base,'/projects/',project_name,'/extracts/gpw_v3/output/',year,'/glds',yy,'ag.csv',sep=""), quote=T, row.names=F, sep=",")

    out_shp <- paste(base,'/projects/',project_name,'/extracts/gpw_v3/output/',year,'/glds',yy,'ag.shp',sep="")
    writePolyShape(myExtract, out_shp)

    timer <- proc.time() - timer
    print(timer)

}