####-------------------------------Define Walk Extraction Functions-----------------------------------
#' Given an lbs data table file, clean and calculate walks, then return valid walks as data table
#' @param raw_lbs_dt Data.table read in by `data.table::fread()`, `arrow::read_parquet()`, or `read_raw()`
#' @param user Name of unique device or user ID column of device record ping in raw LBS data.
#' @param unix_time Name of UNIX timestamp column in raw LBS data.
#' If the timestamp column in the raw LBS data is in another format (e.g., UTC datetime),
#' that timestamp column will need to be converted to UNIX time before running `get_walks`.
#' @param latitude Name of column containing latitude coordinate of device record ping in raw LBS data.
#' @param longitude Name of column containing longitude coordinate of device record ping in raw LBS data.
#' @param accuracy Name of column containing horizontal accuracy of device record ping in raw LBS data.
#' @param homeDist Threshold distance in meters used in utilitarian walk calculation. Default is 100m.
#' @param am_peak_start Hour that before-work commuter peak time starts using 24-hour format (e.g., if morning peak time is at 6a - 10a, `am_peak_start = 6`). Default time is `6`.
#' @param am_peak_end Hour that before-work commuter peak time ends using 24-hour format (e.g., if morning peak time is at 6a - 10a, `am_peak_end = 10`). Default time is `10`.
#' @param pm_peak_start Hour that after-work commuter peak time starts using 24-hour format (e.g., if evening peak time is at 3p - 7p, `pm_peak_start = 15`). Default time is `15`.
#' @param pm_peak_end Hour that after-work commuter peak time starts using 24-hour format (e.g., if evening peak time is at 3p - 7p, `pm_peak_end = 19`). Default time is `19`.
#' @return Data table with extracted walking trips
#' @import dplyr
#' @importFrom data.table data.table
#' @export

get_walks <- function(raw_lbs_dt,
                      user,
                      unix_time,
                      latitude,
                      longitude,
                      accuracy,
                      homeDist = 100,
                      am_peak_start = 6,
                      am_peak_end = 10,
                      pm_peak_start = 15,
                      pm_peak_end = 19
                      ) {

  # keep only the columns in the above to reduce file size
  keep_cols <- c(user, unix_time, accuracy, longitude, latitude)

  raw_lbs_dt <- raw_lbs_dt %>%
    select(keep_cols)

  # validate incoming LBS data to ensure colnames exist and are in correct formats
  valid_dt <- data_validation(raw_df = raw_lbs_dt,
                            user = user,
                            unix_time = unix_time,
                            latitude = latitude,
                            longitude = longitude,
                            accuracy = accuracy)

  # catch any empty dataframes and continue cleaning
  safe_clean <- purrr::possibly(clean_walks,
                                otherwise = data.table::data.table(), quiet = F)

  # catch any empty dataframes and continue calculation
  safe_calculate <- purrr::possibly(calculate_walks,
                                    otherwise = data.table::data.table(), quiet = F)
  # clean validated lbs data
  clean_dt <- safe_clean(validated_df = valid_dt)

  # get walks from validated lbs
  walk_dt <- safe_calculate(clean_df = clean_dt,
                            homeDist = homeDist,
                            am_peak_start = am_peak_start,
                            am_peak_end = am_peak_end,
                            pm_peak_start = pm_peak_start,
                            pm_peak_end = pm_peak_end
                            )

  return(walk_dt)

}

#' Filter out still points, duplicate and jumps in data
#' @param validated_df Data frame or data table containing LBS data already validated using `data_validation()`
#' @return Cleaned data table
#' @import Rcpp
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom Rcpp sourceCpp
#' @keywords internal
clean_walks <- function(validated_df){
  # avoid "no visible binding" warnings
  userId <- time <- accuracy <- lat <- lon <- NULL

  # sort by user and time
  setkey(validated_df, userId,time, accuracy, lat, lon)

  # Remove duplicates
  clean_df <- unique(validated_df, by=c("userId", "time"))

  # eliminate inaccurate points
  clean_df <- clean_df[accuracy < 200,]

  # first pass still points
  clean_df <- clean_df[!(makeLoIsStill(clean_df))]

  # Eliminate jumps in the data with low angle and instant in time (move too quickly for time passage)
  clean_df <- RemoveJumps(clean_df)

  # eliminate new still points
  clean_df <- clean_df[!(makeLoIsStill(clean_df))]

  return(clean_df)
}

#' Run E.Moro logic on cleaned data to extract walks
#' @param clean_df Cleaned data table after using `data_validation()` then `clean_walks()`
#' @param homeDist Threshold distance in meters used in utilitarian walk calculation,
#' @param am_peak_start Hour that before-work commuter peak time starts using 24-hour format (e.g., if morning peak time is at 6a - 10a, `am_peak_start = 6`). Default time is `6`.
#' @param am_peak_end Hour that before-work commuter peak time ends using 24-hour format (e.g., if morning peak time is at 6a - 10a, `am_peak_end = 10`). Default time is `10`.
#' @param pm_peak_start Hour that after-work commuter peak time starts using 24-hour format (e.g., if evening peak time is at 3p - 7p, `pm_peak_start = 15`). Default time is `15`.
#' @param pm_peak_end Hour that after-work commuter peak time starts using 24-hour format (e.g., if evening peak time is at 3p - 7p, `pm_peak_end = 19`). Default time is `19`.
#' @import dplyr
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom stats dist
#' @importFrom lutz tz_lookup_coords
#' @importFrom lubridate with_tz as_datetime
#' @importFrom zoo na.locf
#' @importFrom purrr map2
#' @importFrom Rcpp sourceCpp
#' @return Data table with all walk trips extracted from the raw LBS data
#' @keywords internal
calculate_walks <- function(clean_df,
                            homeDist,
                            am_peak_start,
                            am_peak_end,
                            pm_peak_start,
                            pm_peak_end){
  # avoid "no visible binding" warnings
  lon <- lat <- userId <- vel <- time <- angle <- isNewWalk <- walkNum <- NULL
  isAngleTooSmall <- isDistTooLong <- isInWalk <- total_walks_by_user <- NULL
  timezone <- localtime <- peak_cat <- day_of_week <- walk_peak_cat <- NULL

  # Recalculate dist, velocity and angle columns
  clean_df <- clean_df[, dist := haverDistVector(lon, lat), by=userId
  ][1, dist := 0, by=userId
  ][, vel := dist/(time-data.table::shift(time)), by = userId
  ][, angle := findAngVector(lon, lat, dist), by=userId
  ][1, c('vel', 'angle'):= 0, by=userId # set NA to 0
  ][.N, angle := 0, by=userId]

  # find all walks between min and max speed
  minSpeed = 0.5
  maxSpeed = 5
  clean_df[, isNewWalk := ((vel < minSpeed) | (vel > maxSpeed))]

  # Added a sort to see what would happen to these weird files
  setkey(clean_df, userId, time)

  # Remove walks of length 1 and 2
  clean_df <- clean_df[!(data.table::shift(isNewWalk) & data.table::shift(isNewWalk, type='lead')),
  ][!(isNewWalk & data.table::shift(isNewWalk, type='lead')),]

  # Added a sort to see what would happen to these weird files
  setkey(clean_df, userId, time)

  # Name the walks
  clean_df[(isNewWalk), walkNum := .I
  ][1,walkNum:=0,by=userId
  ][, walkNum := na.locf(walkNum)]

  # Determine if the walks are acceptable
  minAngle = 15/180*pi
  maxDist = 1500
  clean_df[, isAngleTooSmall := angle < minAngle
  ][, isDistTooLong := dist > maxDist & !isNewWalk]

  # Ignores first point of walk's weirdness because that dist isn't part of the walk
  # Also ignores first and last points in walk's angle, as that isn't apart of the walk
  clean_df[isNewWalk | data.table::shift(isNewWalk, type='lead'), isAngleTooSmall := FALSE]

  # # Give an ID for walks and non-walks
  clean_df[, isInWalk := !(any(isDistTooLong) | # if any of the distance too long
                             sum(isAngleTooSmall) > 1 | # if more than 1 angle too small
                             (.N==3 & any(isAngleTooSmall)) | # if userID pings = 3 and any angle is small
                             any(is.na(isAngleTooSmall))), # if any NA in isAngleTooSmall
           by=c("userId", "walkNum")]

  # Remove non-walks
  clean_df <- clean_df[(isInWalk),]

  # Set beginning of walks dist, vel, and angle to NA and end of walks angle to NA
  clean_df[(isNewWalk), c('angle', 'dist', 'vel') := NA
  ][data.table::shift(isNewWalk, type = 'lead'), angle:=NA]
  numRows <- clean_df[, .N]
  clean_df[numRows, angle:=NA]

  # Remove unnecessary columns
  clean_df <- clean_df[, !c("isNewWalk", "isAngleTooSmall", "isDistTooLong",
                            "isInWalk")]

  if(nrow(clean_df) > 0){

    # calculate total walks by user
    # clean_df <- clean_df[, total_walks_by_user := dplyr::n_distinct(userId, walkNum), by = userId
    # ][, c('numDaysActive', 'numPoints') := .(length(unique(lubridate::as_datetime(time))), .N), by = userId
    # ][, ':='(walk_duration = (max(time) - min(time))/60,
    #          walk_distance = sum(dist, na.rm = T)), by = c("userId", "walkNum")]

    clean_df <- clean_df[, total_walks_by_user := dplyr::n_distinct(userId, walkNum), by = userId
    ][, ':=' (numDaysActive = length(unique(lubridate::as_datetime(time))),
              numPoints = .N)
      ][, ':='(walk_duration = (max(time) - min(time))/60,
             walk_distance = sum(dist, na.rm = T)), by = c("userId", "walkNum")]


    # this may trigger a warning again
    clean_df <- clean_df[,timezone := tz_lookup_coords(lat = lat, lon = lon, method = "accurate")
    ][,localtime := map2(.x = as_datetime(time), .y = timezone,
                         .f = function(x, y) {with_tz(x, tzone = y)})] %>%
      unnest(localtime) %>%
      as.data.table()

    # add peaks into dataframe
    clean_df[,peak_cat:= find_peaks(localtime)][,day_of_week := weekdays(localtime)
    ][day_of_week %in% c("Saturday", "Sunday")
      , ':=' (peak_cat = "off_peak", day_of_week = 'weekend')
    ][day_of_week != 'weekend'
      , ':=' (day_of_week = 'weekday')
    ][,walk_peak_cat:= data.table::first(peak_cat), by = c("userId", "walkNum")]

    # add in utilitarian vs recreation calculations
    clean_df <- createUtilSplit(clean_df, homeDist = homeDist)

    # remove more unnecessary columns
    calculate_df <- clean_df %>%
      select(-c("timezone", "localtime", "util_dist", "peak_cat"))

  }else{
    calculate_df <- data.table()
  }

  return(calculate_df)
}

####-------------------------------Define Data Check Functions -----------------------------------
#' Validate the raw data table and transform to ensure it works with the walks funcs
#' @param raw_df Raw LBS data table read using `read_raw()` or other data reading methods like `data.table::fread()` or `read_csv()`.
#' @param user Name of unique device or user ID column of device record ping in raw LBS data. Defaults to `id`.
#' @param unix_time Name of UNIX timestamp column in raw LBS data. Defaults to `unix_timestamp`.
#' @param latitude Name of column containing latitude coordinate of device record ping in raw LBS data. Defaults to `latitude`.
#' @param longitude Name of column containing longitude coordinate of device record ping in raw LBS data. Defaults to `latitude`.
#' @param accuracy Name of column containing horizontal accuracy of device record ping in raw LBS data. Defaults to `horizontal_accuracy`.
#' @return Cleaned data table with standardized column names (userId, time, lat, lon, accuracy)
#' @import dplyr
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom stats na.omit
#' @keywords internal
data_validation <- function(raw_df, user, unix_time, latitude,longitude, accuracy){
  # avoid "no visible binding" warnings
  time <- NULL

  if (!is.data.frame(raw_df) & !is.data.table(raw_df)) {
    stop("Dataset is not a dataframe or data table")
  }else{
    df <- data.table(raw_df)
  }

  # remove all NA from userID, time, longitude, and latitude
  # these variables must always have values in them
  df <- na.omit(df, cols = c(unix_time, user, longitude, latitude))

  if(length(df) == 0){
    stop("Columns in c(unix_time, user, longitude, latitude) cannot contain all NA values.")
  }

  # make userId lowercase because it's not case sensitive
  # df[[user]] <- tolower(df[[user]])

  # test if unix_time is numeric
  df[[unix_time]] <- test_numeric("Unix timestamp",df[[unix_time]])

  # remove instances where the unixtime is less than 10 digits
  unix_str <- toString(unix_time)
  df <- df[df[[unix_str]] >= 1000000000,]

  # apply the unix check
  df[, c('time'):= lapply(.SD, check_unix), .SDcols = unix_str]


  # verify that accuracy is numeric
  df[[accuracy]] <- test_numeric("Accuracy",df[[accuracy]])

  # verify that latitude and longitude are numeric
  df[[latitude]] <- test_numeric("Latitude",df[[latitude]])
  df[[longitude]] <- test_numeric("Longitude",df[[longitude]])

  # remove values that are not valid latitude (< -90 and > 90) and longitude (< -180 and > 180)
  lon_str <- toString(longitude)
  lat_str <- toString(latitude)

  df <- df[(df[[lon_str]] <= 180 & df[[lon_str]] >= -180) &
             (df[[lat_str]] <= 90 & df[[lat_str]] >= -90),]

  # select only columns that we need for the walk calculation
  validated_df <- dplyr::select(df,c(
    "userId" = all_of(user),
    time,
    "lat" = all_of(latitude),
    "lon" = all_of(longitude),
    "accuracy" = all_of(accuracy)
  ))

  if(nrow(df) == 0){
    stop("Your dataset is empty after validation. Please verify that column inputs are correct before continuing.")
  }else{
    message("Your dataset has passed validation.")
  }

  return(validated_df)
}

#' Check whether column is numeric
#' @param col_name Column name (string) to check
#' @param col Selected data table column
#' @return Returns selected data table column if numeric, displays a warning otherwise
#' @keywords internal
test_numeric <- function(col_name, col){

  if(!is.numeric(col)){
    col <- as.numeric(col)

    if(all(is.na(col))){
      stmt = paste0(col_name, " column is not numeric. Please confirm that input column is correct.")
      stop(stmt)
    }
  }

  return(col)
}

#' Read in all files in the file directory path (parquet or csv) and return as a single data.table
#' @param file_dir_path Directory where files are located
#' @param file_pattern File type (either "parquet", "csv", or "csv.gz").
#' @param ... For a parquet file type, can use options found in `arrow::read_parquet()`. For other file types like csv or csv.gz, can use options found in `data.table::fread()`.
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom arrow read_parquet
#' @return Returns raw data table with LBS data
#' @export
read_raw <- function(file_dir_path, file_pattern = c("parquet", "csv", "csv.gz"), ...){

  if(!dir.exists(file_dir_path)){
    stop("
    Directory path does not exist. Please input the correct directory where your data files are located.
    As a reminder, file_dir_path does not take a direct file name, (e.g., 'data/myfile.csv') and only takes directory names (e.g., 'data/').
         ")
  }

  file_lst <- list.files(file_dir_path, full.names = T, pattern = file_pattern)

  if(all(grepl('\\.parquet$', file_lst))){
    dt <- rbindlist(lapply(file_lst, function(x) arrow::read_parquet(x, ...)))
  }else{
    dt <- rbindlist(lapply(file_lst, function(x) fread(x, ...)) )
  }

  if(nrow(dt) == 0){
    stop(paste0("There is no data in ",file_dir_path))
  }
  return(dt)
}

#' Check whether unix timestamp is valid
#' @param unix_time Expression to supress
#' @return If valid timestamp, return
#' @keywords internal
check_unix <- function(unix_time){
  condition10 <- unix_time >= 1000000000 & unix_time <= as.numeric(Sys.time())
  condition13 <- unix_time >= 1000000000000 & unix_time <= (as.numeric(Sys.time()) * 1000)
  if(all(condition10)){
    return(unix_time)
  }else if(all(condition13)){
    return(unix_time/1000)
  }else{
    stop("Some or all unix time values are not valid")
  }
}
