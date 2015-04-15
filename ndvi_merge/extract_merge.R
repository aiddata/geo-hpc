# merge extract data into single table
# column for each unique year_day extract

library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)

extract_type <- readIn[1]

ndvi <- "mosaic_GMO"

mod <- ""
if (extract_type == "historic") {
	mod <- "historic_"
	ndvi <- "gimms_ndvi"
}

base <- paste("/sciclone/data20/aiddata/REU/projects/kfw/extracts/",mod,"ndvi",sep="")
out <- paste(mod,"extract_merge.csv",sep="")


years <- list.files(paste(base,"/output",sep=""))

c <- 0

# db <- 0

for (y in 1:length(years)) {

	year <- years[y]

	days <- list.files(paste(base,"/output/",year,sep=""))

	for (d in 1:length(days)) {

		day<- days[d]

		path <- paste(base,"/output/",year,"/",day,"/",mod,"extract_",year,"_",day,".shp",sep="")

		# read extract shp 
		v <- readShapePoly(path)

		# extract data
		if (extract_type == "historic") {
 			ex <- v@data[[ndvi]]
 		} else {
 			ex <- v@data[[ndvi]] / 250
 		}

		# add extract data to table
		# for (x in 1:length(ex)) {
		# 	date <- as.numeric(year) + (as.numeric(day)/365)
		# 	ndvi <- ex[[x]]
		# 	newRow <- c(id=x, year=as.integer(year), day=as.integer(day), date=date, ndvi=ndvi)
		# 	if (db == 0) {
		# 		db <- data.frame(newRow)
		# 	} else {
		# 		db <- rbind(db, newRow)
		# 	}
		# }

		# create df if it does not exist
		if (c == 0) {
			df <- data.frame(id= c(1:length(ex)))
			c <- 1
		}

		# add extract to df
		df[[paste(year,day,sep="_")]] <- ex

	}
}


# db_out <- "~/Desktop/merge_db_out.csv"
# write.table(db, db_out, quote=F, row.names=F, sep=",")

table_out <- paste(base,"/",out,sep="")
write.table(df, table_out, quote=F, row.names=F, sep=",")

timer <- proc.time() - timer 
print(timer)
