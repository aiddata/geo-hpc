# get max ndvi value for each year period

type <- "contemporary"
# type <- "historic"

path <- "~/Desktop/ndvi_extract_merge.csv"
table_out <- "~/Desktop/ndvi_extract_year_max.csv"
range <- c(2002:2014)

if (type == "historic") {
	path <- "~/Desktop/historic_ndvi_extract_merge.csv"
	table_out <- "~/Desktop/historic_ndvi_extract_year_max.csv"
	range <- c(1982:2001)
}

# get actual field names
h <- read.csv(path, nrow=1, head=FALSE)

# read csv
c <- read.csv(path)

# ids
ids <- c[["id"]]

# num rows
rows <- length(c[[1]])

# build dataframe
df <- data.frame(id= c(1:rows))

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

	m <- c()
	# get max value for all periods in current year
	for (row in 1:rows) {
		max <- max(c[row,years])
		m <- append(m, max)
	}

	# add year to dataframe
	df[[paste("\"",year,"\"",sep="")]] <- m

}


write.table(df, table_out, quote=F, row.names=F, sep=",")

