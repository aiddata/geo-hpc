# merge gpw extract data into single table
# column for each unique year_day extract

library("maptools")

timer <- proc.time()


base <- "~/Desktop/gpw"
out <- "gpw_extract_merge.csv"


years <- list.files(paste(base,"/output",sep=""))

c <- 0

# db <- 0

for (y in 1:length(years)) {

	year <- years[y]

	files <- list.files(paste(base,"/output/",year,sep=""))

	for (f in 1:length(files)) {

		file <- files[f]

		if (substring(file, nchar(file)-3) == ".shp") {

			path <- paste(base,"/output/",year,"/",file,sep="")

			field <- substring(file,1,nchar(file)-4)

			# read extract shp 
			v <- readShapePoly(path)

			# extract data
	 		ex <- v@data[[field]]

			# create df if it does not exist
			if (c == 0) {
				df <- data.frame(id= c(1:length(ex)))
				c <- 1
			}

			# add extract to df
			df[[field]] <- ex

		}
	}
}


table_out <- paste(base,"/",out,sep="")
write.table(df, table_out, quote=F, row.names=F, sep=",")

timer <- proc.time() - timer 
print(timer)
