# merge extract data into single table
# column for each unique year_day extract

library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)

extract_type <- readIn[1]

mod <- "air_temp"

if (extract_type == "terrestrial_precipitation") {
	mod <- "precip"
}

base <- paste("/sciclone/data20/aiddata/REU/projects/kfw/extracts/",extract_type,sep="")
out <- paste(mod,"_extract_merge.csv",sep="")


years <- list.files(paste(base,"/output",sep=""))

c <- 0


for (y in 1:length(years)) {

	year <- years[y]

	days <- list.files(paste(base,"/output/",year,sep=""))

	for (d in 1:length(days)) {

		day<- days[d]

		path <- paste(base,"/output/",year,"/",day,"/",mod,"_extract_",year,"_",day,".shp",sep="")

		# read extract shp 
		v <- readShapePoly(path)

		# extract data
 		ex <- v@data[[mod]]


		# create df if it does not exist
		if (c == 0) {
			df <- data.frame(id= c(1:length(ex)))
			c <- 1
		}

		# add extract to df
		df[[paste(year,day,sep="_")]] <- ex

	}
}


table_out <- paste(base,"/",out,sep="")
write.table(df, table_out, quote=F, row.names=F, sep=",")

timer <- proc.time() - timer 
print(timer)
