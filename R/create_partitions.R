##### Define Data Ingest Functions -------------------------------------------------
# Assumes active Spark session

#' Install a version of spark and return directory where it was installed.
#' @param spark_bool Boolean on whether to install spark
#' @param spark_ver Version of spark to install
#' @import sparklyr
#' @return Spark install directory
#' @export
get_spark_dir <- function(spark_bool = T, spark_ver = "3.3.0") {

  # If T, install Spark else just return directory
  if (spark_bool == T){
    print('Available Spark versions:')
    print(spark_available_versions())
    spark_install(version = spark_ver)
  }

  # If spark installed
  prev <- spark_connect(master = "local")
  spark_path = prev$spark_home
  spark_disconnect(prev)

  # Return spark path
  return(spark_path)
}

#' Read in large LBS data from csv(s) and create N device stratified partitions to run walkingcalculatr
#' with available memory. Depending on size of data/number of partitions, may take several hours to execute.
#' ** Requires an active SparkR session. **
#' @param pathname Directory with LBS data (csv)
#' @param output_dir Directory to save N partitions (csv)
#' @param N Number of partitions
#' @import dplyr
#' @return Directory with N partitions (parquet) created
#' @export
create_partition_from_csv <- function(pathname,
                             output_dir,
                             N = 30) {

  # Read in LBS data from directory of csvs
  file_lst <- list.files(pathname,
                         full.names = T,
                         pattern = ".csv.gz|.csv")

  # Read in each csv and concatenate
  df <- read.df(file_lst[[1]], "csv",
                header = "true",
                inferSchema = "true",
                na.strings = "NA")

  for (i in seq_along(file_lst)){

    if (i > 1) {
      temp <- read.df(file_lst[[i]],
                      "csv",
                      header = "true",
                      inferSchema = "true",
                      na.strings = "NA")

      df <- rbind(df, temp)
    }
  }

  # Lower case
  df$id <- SparkR::lower(df$id)
  printSchema(df)

  # Register earlier df as temp view
  createOrReplaceTempView(df, "df")

  # Get device list
  devices <- SparkR::collect(SparkR::sql("SELECT DISTINCT(id) from df"))

  # Create list of device chunks (N - number of chunks)
  if (length(devices < 2)) {
    device_lst <- devices %>% unlist() %>% as.list()
  } else {
    device_lst <- devices[[1]] %>% unlist() %>% as.list()
  }
  df_lst <- split(device_lst, cut(seq_along(device_lst), N, labels = FALSE))

  # For each split, filter data to device pings
  for (i in seq_along(df_lst)){

    # Filter to data of interest
    sel_devices <- df_lst[[i]]
    createOrReplaceTempView(df, "df")
    temp <-
    # temp <- df %>% SparkR::filter(df$id %in% sel_devices)
    # temp <- SparkR::filter(df, df$id %in% sel_devices)

    # Save each dataset
    fileNameOut <- file.path(output_dir, paste0("lbs_data_",i))

    # Save as parquets
    write.parquet(temp, fileNameOut, mode = "overwrite")
    print(str_glue("Saved partition {i} as {fileNameOut}"))
  }
}

#' Read in large LBS data from parquet(s) and create N device stratified partitions to run walkingcalculatr
#' with available memory. Depending on size of data/number of partitions, may take several hours to execute.
#' Requires an active SparkR session.
#' @param sc Spark session
#' @param pathname Directory with LBS data (csv)
#' @param output_dir Directory to save N partitions (csv)
#' @param N Number of partitions
#' @import dplyr
#' @return Directory with N partitions (parquet) created
#' @export
create_partition_from_parquet <- function(sc,
                                          sq,
                                          pathname,
                                          output_dir,
                                          N = 10) {
  # # Import Spark library
  # library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

  # Cast parquet to spark dataframe
  df <- SparkR::read.df(sc, path = pathname,
                        source = 'parquet', schema = NULL)

  # Update: lower case all character vectors to avoid de-duping
  df$userId <- SparkR::lower(df$userId)

  # Show schema
  SparkR::printSchema(df)

  # Register earlier df as temp view
  SparkR::createOrReplaceTempView(df, "df")

  # Get device list
  devices <- SparkR::collect(SparkR::sql("SELECT DISTINCT(userId) from df"))

  # Create list of device chunks (N - number of chunks)
  if (length(devices < 2)) {
    device_lst <- devices %>% unlist() %>% as.list()
  } else {
    device_lst <- devices[[1]] %>% unlist() %>% as.list()
  }

  df_lst <- split(device_lst, cut(seq_along(device_lst), N, labels = FALSE))

  # For each split, filter data to device pings
  print('Starting loop')
  for (i in seq_along(df_lst)){

    # Filter to data of interest
    sel_devices <- df_lst[[i]]
    # temp <- df %>% SparkR::filter(id %in% sel_devices)
    temp <- df %>% SparkR::filter(df$userId %in% df_lst[[i]])
    # temp <- SparkR::filter(df, df$id %in% df_lst[[i]])

    print(length(sel_devices))

    # Save each dataset
    fileNameOut <- file.path(output_dir, paste0("lbs_data_",i))

    # Save as parquet
    write.parquet(temp, fileNameOut, mode = "overwrite")

    # Save as csv
    print(paste("Saved partition", i," as", fileNameOut))
  }

}

### To Do: Add Additional Functions ---------

# 1) Function to convert geospatial points to lat and lon columns (sf)
PointToLatLon <- function(points_df) {

  library(sf)

  # Given dataframe with geometry type column
  coords_df <- points_df %>% extract(geometry,
                                     c('lon', 'lat'),
                                     '\\((.*), (.*)\\)', convert = TRUE)  %>%
    st_drop_geometry(NULL) %>% select('lat', 'lon')

  return(coords_df)
}

# 2) Filter to weekdays or certain time (SparkR)
AddTimePeriods <- function(df_lbs) {

  # Define Spark home directory
  sc <- spark_connect(master = "local")
  spark_path = sc$spark_home
  spark_disconnect(sc)
  Sys.setenv(SPARK_HOME=spark_path)

  # Read in packages
  library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

  # Parse hour from timestamp column
  df_lbs <- withColumn(df_lbs, "hour", SparkR::hour(df_lbs$timestamp))

  # Parse day of week from timestamp column (1 - Sunday, 2 - Monday, ..., 7 - Saturday)
  df_lbs <- withColumn(df_lbs, "dayofweek", SparkR::dayofweek(df_lbs$timestamp))

  return(df_lbs)
}

# 3) Function to convert utctime to unixtime (SparkR)
# Specify format of timestamp
TimestampToUnix <- function(df_lbs, date_format) {
  df_lbs <- withColumn(df_lbs, "unix_time", SparkR::unix_timestamp(df_lbs$timestamp,
                                                                   date_format))
  return(df_lbs)
}

# 4) Function to convert unix to utctime (SparkR)
# Specify format of timestamp (MM-dd-yyyy HH:mm:ss)
UnixToTimestamp <- function(df_lbs, date_format) {

  # Return time format from unixtime
  df_lbs <- withColumn(df_lbs, "time_from_unix", SparkR::from_unixtime(df_lbs$timestamp,
                                                                       date_format))
  return(df_lbs)
}
