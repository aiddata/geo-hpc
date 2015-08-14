# merge gpw extract data into single table
# column for each unique year_day extract

library("maptools")

timer <- proc.time()


# --------------------------------------------------

base <- "/home/userz/globus-data"
project_name <- "liberia"

# --------------------------------------------------

base <- paste(base,"/projects/",project_name,"/extracts/gpw_v3", sep="")
out <- "extract_merge.csv"


years <- list.files(paste(base,"/output", sep=""))

c <- 0

# db <- 0

for (y in 1:length(years)) {

	year <- years[y]

	files <- list.files(paste(base,"/output/",year,sep=""))

	for (f in 1:length(files)) {

		file <- files[f]

		if ( substring(file, nchar(file)-3) == ".shp" ) {

			path <- paste(base,"/output/",year,"/",file,sep="")

			field <- substring(file,1,nchar(file)-4)

			# read extract shp 
			v <- readShapePoly(path)

			# extract data
	 		ex <- v@data[[field]]

			# create df if it does not exist
			if (c == 0) {
				# df <- data.frame(id= c(1:length(ex)))
                df <- v@data
				c <- 1
			} else {

    			# add extract to df
    			df[[field]] <- ex
            }

		}
	}
}


table_out <- paste(base,"/",out, sep="")
write.table(df, table_out, quote=T, row.names=F, sep=",")

timer <- proc.time() - timer 
print(timer)
