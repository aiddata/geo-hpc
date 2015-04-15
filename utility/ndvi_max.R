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


h <- read.csv(path, nrow=1, head=FALSE)

c <- read.csv(path)

ids <- c[["id"]]

rows <- length(c[[1]])

df <- data.frame(id= c(1:rows))


for (y in range) {
	x<-0

 	year <- toString(y)
	years <- c()
	for (i in h) {
		if (year == substr(i,1,4)) {
			years <- append(years, paste("X",toString(i),sep=""))
		}
	}
	# print(c[1:5,years])
	m <- c()
	for (row in 1:rows) {
		# if (x < 1){
			# print(year)
			max <- max(c[row,years])
			m <- append(m, max)
			# x<-x+1
		# }
	}
	# print(m)
	df[[paste("\"",year,"\"",sep="")]] <- m


}


write.table(df, table_out, quote=F, row.names=F, sep=",")

