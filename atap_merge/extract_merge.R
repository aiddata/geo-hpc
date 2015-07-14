# merge extract data into single table
# column for each unique year_day extract

library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)

extract_type <- readIn[1]

project_name <- readIn[2]


mod <- "air_temp"

if (extract_type == "terrestrial_precipitation") {
	mod <- "precip"
}

base <- paste("/sciclone/aiddata10/REU/projects/",project_name,"/extracts/",extract_type,sep="")
out <- "extract_merge.csv"


years <- list.files(paste(base,"/output",sep=""))

c <- 0


for (y in 1:length(years)) {

	year <- years[y]

	days <- list.files(paste(base,"/output/",year,sep=""))

	for (d in 1:length(days)) {

		day<- days[d]

		path <- paste(base,"/output/",year,"/",day,"/extract_",year,"_",day,".shp",sep="")

		# read extract shp 
		v <- readShapePoly(path)

		# extract data
 		ex <- v@data[[mod]]


		# create df if it does not exist
		if (c == 0) {
			# df <- data.frame(id= c(1:length(ex)))
            df <- v@data

            x <- match(mod, colnames(df))
            colnames(df)[x] <- paste(year,day,sep="_")

			c <- 1
		} else {

    		# add extract to df
    		df[[paste(year,day,sep="_")]] <- ex
        }

	}
}


table_out <- paste(base,"/",out,sep="")
write.table(df, table_out, quote=T, row.names=F, sep=",")

timer <- proc.time() - timer 
print(timer)
