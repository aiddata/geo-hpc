# get max ndvi value for each year period


# local format (old)

# path <- "~/Desktop/ndvi_extract_merge.csv"
# max_out <- "~/Desktop/ndvi_extract_year_max.csv"
# range <- c(2002:2014)

# path <- "~/Desktop/historic_ndvi_extract_merge.csv"
# max_out <- "~/Desktop/historic_ndvi_extract_year_max.csv"
# range <- c(1982:2001)


# sciclone format

# no validation or error checking yet
# be sure to enter valid name and range

# example inputs:
# Rscript ndvi_merge_calc.R ndvi 2001 2014 all
# Rscript ndvi_merge_calc.R historic_ndvi_buffer 1982 2002 all

readIn <- commandArgs(trailingOnly = TRUE)

extract <- readIn[1]
year_start <- readIn[2]
year_end <- readIn[3]
run_type <- readIn[4]
# extract <- "ndvi_buffer"
# year_start <- 2002
# year_end <- 2014


base <- "/sciclone/data20/aiddata/REU/projects/kfw/extracts"
# base <- "/home/userz"

path <- paste(base,"/",extract,"/extract_merge.csv",sep="")
max_out <- paste(base,"/",extract,"/merge_year_max.csv",sep="")
mean_out <- paste(base,"/",extract,"/merge_year_mean.csv",sep="")

range <- c(year_start:year_end)


# get actual field names
h <- read.csv(path, nrow=1, head=FALSE)

# read csv
c <- read.csv(path)

# ids
ids <- c[["id"]]

# num rows
rows <- length(c[[1]])

# build dataframe
df_max <- data.frame(id= c(1:rows))
df_mean <- data.frame(id= c(1:rows))

# iterate years
for (y in range) {
	x<-0

 	year <- toString(y)
	years <- c()

	# check if year is in desired range
	# append valid years to list
	for (i in h) {
		if (year == substr(i,1,4)) {
			years <- append(years, paste("X",toString(i),sep=""))
		}
	}

	m1 <- c()
	m2 <- c()

	# get max and mean value for all periods in current year
	for (row in 1:rows) {
		if (run_type == "all" || run_type == "max") {
			max <- max(c[row,years])
			m1 <- append(m1, max)
		}

		if (run_type == "all" || run_type == "mean") {
			mean <- mean(as.numeric(c[row,years]))
			m2 <- append(m2, mean)
		}
	}

	# add year to dataframe
	if (run_type == "all" || run_type == "max") {
		df_max[[paste("\"",year,"\"",sep="")]] <- m1
	}

	if (run_type == "all" || run_type == "mean") {
		df_mean[[paste("\"",year,"\"",sep="")]] <- m2
	}
}

if (run_type == "all" || run_type == "max") {
	write.table(df_max, max_out, quote=F, row.names=F, sep=",")
}

if (run_type == "all" || run_type == "mean") {
	write.table(df_mean, mean_out, quote=F, row.names=F, sep=",")
}
