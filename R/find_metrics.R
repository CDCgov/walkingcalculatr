####-------------------------------Define Metric Functions-----------------------------------
#' Delineate utilitarian vs recreational walk trajectories
#' @param extracted_walk_df Data frame or data table containing extracted walks.
#' @param homeDist Threshold distance for utilitarian vs recreation walk split (default 100 m).
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @return Data.table column `IsUtilWalk` designating whether the walk trajectory is utilitarian or recreational.
#' @keywords internal
createUtilSplit <- function(extracted_walk_df, homeDist = 100) {

  # avoid "no visible binding" warnings
  userId <- walkNum <- util_dist <- lon <- lat <- . <- N <- IsUtilWalk <- NULL

  df_walks <- data.table(extracted_walk_df) # convert to data.table if not already

  # Get first and last pings for each unique walk
  utilPings <- df_walks[, .SD[c(1,.N)], by=list(userId, walkNum)
  ][, util_dist := haverDistVector(lon, lat), by = list(userId, walkNum)]

  # Assign recreational if distance is <= homeDist, utilitarian otherwise for the last row in each walk trajectory
  utilPings <- utilPings[utilPings[,.N, by=.(userId, walkNum)][,cumsum(N)],
                         IsUtilWalk := ifelse(util_dist <= homeDist, "recreational", "utilitarian")
  ][!is.na(IsUtilWalk)
  ][,.(userId, walkNum, util_dist, IsUtilWalk)]

  # join into original df
  utilCalc <- df_walks[utilPings, on = c("userId", "walkNum")]

  return(utilCalc)
}

#' Given the hours for each peak time category, return associated category
#' @param am_peak_start Hour that before-work commuter peak time starts using 24-hour format (e.g., if morning peak time is at 6a - 10a, `am_peak_start = 6`)
#' @param am_peak_end Hour that before-work commuter peak time ends using 24-hour format (e.g., if morning peak time is at 6a - 10a, `am_peak_end = 10`)
#' @param pm_peak_start Hour that after-work commuter peak time starts using 24-hour format (e.g., if evening peak time is at 3p - 7p, `pm_peak_start = 15`)
#' @param pm_peak_end Hour that after-work commuter peak time starts using 24-hour format (e.g., if evening peak time is at 3p - 7p, `pm_peak_end = 19`)
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @return Commuter peak times as a character vector
#' @keywords internal
# input the hour that the time of day category starts. This will then be used to output time of day
get_peak_breaks <- function(am_peak_start, am_peak_end, pm_peak_start, pm_peak_end){
  break_start <- c(am_peak_start, am_peak_end, pm_peak_start, pm_peak_end)

  if(all(!is.numeric(break_start))){
    stop('Input vector is not an integer. Please input a vector of integers between 0-23.')
  }

  if(length(unique(break_start)) != length(break_start)){
    stop('No starts can have the same values. Please input unique values.')
  }

  if(length(break_start) < 4){
    stop("There are less than 4 start hours in vector. Please ensure there is a start and end to the AM and PM peaks")
  }

  if(any(break_start < 0 | break_start > 23)){
    stop('No starts can have values < 0 or > 23. Please input values between 0 - 23.')
  }

  # if a value is 0, change to 1 to accommodate for subtraction
  break_start[which(break_start == 0)] <- 1

  starts <- c(am_peak_start, pm_peak_start) - 1
  ends <- c(am_peak_end, pm_peak_end)


  tod_break <- c(-1,sort(c(starts,ends)), 25)

  return(tod_break)
}

#' Calculate commuter peak times for each walk trajectory
#' @param loc_time_col UTC timestamp adjusted to local time zone based on LBS data
#' @param am_peak_start Hour that before-work commuter peak time starts using 24-hour format (e.g., if morning peak time is at 6a - 10a, `am_peak_start = 6`). Default time is `6`.
#' @param am_peak_end Hour that before-work commuter peak time ends using 24-hour format (e.g., if morning peak time is at 6a - 10a, `am_peak_end = 10`). Default time is `10`.
#' @param pm_peak_start Hour that after-work commuter peak time starts using 24-hour format (e.g., if evening peak time is at 3p - 7p, `pm_peak_start = 15`). Default time is `15`.
#' @param pm_peak_end Hour that after-work commuter peak time starts using 24-hour format (e.g., if evening peak time is at 3p - 7p, `pm_peak_end = 19`). Default time is `19`.
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @return Commuter peak time category (`am_peak`, `pm_peak`, or `off_peak`). Weekend times are always `off-peak`.
#' @export
find_peaks <- function(loc_time_col,
                       am_peak_start = 6,
                       am_peak_end = 10,
                       pm_peak_start = 15,
                       pm_peak_end = 19
){
  breaks <- get_peak_breaks(am_peak_start, am_peak_end, pm_peak_start, pm_peak_end)

  # labels for the breaks
  labels <- c("off_peak", "am_peak", "off_peak", "pm_peak", "off_peak")

  # calculate peak times
  peak_times <- cut(x=hour(loc_time_col), breaks = breaks, labels = labels, include.lowest=TRUE)

  return(peak_times)
}

#' Given the hours for each peak time category, return associated category
#' @param walks_in_walkable_df Data frame with extracted walks after using `get_walks()` then mapped using to county tract GEOIDs using `find_walkable()`
#' @import dplyr stringr
#' @return Data frame with GEOIDs in the correct format (11-digit or NA characters)
#' @keywords internal
check_GEOID <- function(walks_in_walkable_df){
  # avoid "no visible binding" warnings
  GEOID <- NULL

  # check if any GEOIDs are != 11 or NA, and if the GEOIDs are not a character data type
  if(!all(nchar(walks_in_walkable_df$GEOID) %in% c(11, 0, NA_character_)) | !is.character(walks_in_walkable_df$GEOID)){
    warning("Some or all census tract GEOIDs are the incorrect length. Adding leading 0s to GEOIDs, but please recheck that the resulting GEOIDs are correct in the final output")

    # Convert GEOID to character data type, and add leading 0s to bring to correct length
    fixed_df <- walks_in_walkable_df %>%
      mutate(GEOID = stringr::str_pad(GEOID, width = 11, side = "left", pad = 0))
  }else{
    fixed_df <- walks_in_walkable_df
  }
  return(fixed_df)
}

#' Generate summary table across all census tracts, with metrics such as walk distance, walk duration,
#' commuter peak times and utilitarian walk counts
#' @param walks_in_walkable_df Data frame with extracted walks after using `get_walks()` then mapped using to county tract GEOIDs using `find_walkable()`
#' @import dplyr
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom rlang has_name
#' @importFrom tidyr spread
#' @importFrom stats median sd
#' @return Summary data frame across all census tracts
#' @export
metric_summary <- function(walks_in_walkable_df){
  # avoid "no visible binding" warnings
  . <- NULL

  # avoid "no visible binding" warnings
  fraction <- time <- userId <- walkNum <- day_of_week <- walk_peak_cat <- NULL
  walk_count <- walk_duration <- walk_distance <- min_dist_dur <- max_dist_dur <- NULL
  med_dist_dur <- mean_dist_dur <- IsUtilWalk <- count_util <- NULL

  valid_walk <- walks_in_walkable_df %>%
    filter(fraction > 0.75)

  # get total number of days with walks in walkable areas
  total_days <- valid_walk %>%
    mutate(date = as.Date(as.POSIXct(time))) %>%
    summarise(total_days_with_walks = n_distinct(date))

  general_metrics <- valid_walk %>%
    reframe(total_walks_in_walkable_areas = n_distinct(userId, walkNum),
            total_distinct_users = n_distinct(userId),
            avg_dist_between_pings = mean(dist, na.rm = T)
    ) %>%
    distinct()

  peak_summary <- valid_walk %>%
    distinct(userId, walkNum, day_of_week, walk_peak_cat) %>%
    mutate(walk_peak_cat = paste(day_of_week, walk_peak_cat, sep = "_")) %>%
    group_by(walk_peak_cat) %>%
    reframe(walk_count = n()) %>%
    spread(walk_peak_cat, walk_count) %>%
    relocate("weekday_pm_peak", .before = "weekday_off_peak")

  # get walk distance and duration summary (min/max/med/mean)
  dist_dur_summary <- valid_walk %>%
    select(userId, walkNum, walk_duration, walk_distance) %>%
    distinct(userId, walkNum, .keep_all = T) %>%
    summarise_at(vars(walk_distance, walk_duration),
                 list(
                   min = ~min(.),
                   max = ~max(.),
                   med = ~median(.),
                   mean = ~mean(.)
                 )) %>%
    mutate(across(.cols = everything()), round(., 2)) %>%
    unite(min_dist_dur, ends_with("_min"), sep = " m, ") %>%
    unite(max_dist_dur, ends_with("_max"), sep = " m, ") %>%
    unite(med_dist_dur, ends_with("_med"), sep = " m, ") %>%
    unite(mean_dist_dur, ends_with("_mean"), sep = " m, ")

  # apply to every column in dist_duration
  dist_dur_summary[] <- lapply(dist_dur_summary, function(x) paste0(x, " minutes"))

  # get count of utilitarian vs recreational walks for walks in walkable areas
  util_summary <- valid_walk %>%
    group_by(IsUtilWalk) %>%
    reframe(count_util = n_distinct(userId,walkNum)) %>%
    spread(IsUtilWalk, count_util)

  summary_df <- cbind(general_metrics,total_days, util_summary, dist_dur_summary, peak_summary)

  return(summary_df)
}

#' Generate census tract-level summary table with metrics such as walk distance, walk duration,
#' commuter peak times and utilitarian walk counts, grouped by GEOID.
#' @param walks_in_walkable_df Data frame with extracted walks after using `get_walks()` then mapped using to county tract GEOIDs using `find_walkable()`
#' @import dplyr
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom rlang has_name
#' @importFrom tidyr spread
#' @importFrom stats median sd
#' @return Summary data frame, grouped by census tract GEOID
#' @export
tract_metric_summary <- function(walks_in_walkable_df){

  # avoid "no visible binding" warnings
  fraction <- GEOID <- time <- userId <- walkNum <- day_of_week <- walk_peak_cat <- NULL
  walk_count <- walk_duration <- walk_distance <- min_dist_dur <- max_dist_dur <- NULL
  med_dist_dur <- mean_dist_dur <- IsUtilWalk <- count_util <- NULL

  # keep only walk trips in walkable areas and confirm that GEOID are in the correct format
  valid_walk <- walks_in_walkable_df %>%
    filter(fraction > 0.75) %>%
    separate_longer_delim(GEOID, delim = ";") %>% # if a ping is in multiple GEOID, split each GEOID into separate row
    check_GEOID()

  # get total number of days with walks in walkable areas
  total_days <- valid_walk %>%
    mutate(date = as.Date(as.POSIXct(time))) %>%
    group_by(GEOID) %>%
    summarise(total_days_with_walks = n_distinct(date))

  # calculate overall metrics for walking trips within census tracts
  # pings not in walkable areas will be removed
  general_metrics <- valid_walk %>%
    group_by(GEOID) %>%
    reframe(
      total_walks_in_walkable_areas = n_distinct(userId, walkNum),
      total_distinct_users = n_distinct(userId),
      avg_dist_between_pings = mean(dist, na.rm = T)
    )

  # get summary of AM/PM/off peak times split by weekdays and weekends
  peak_summary <- valid_walk %>%
    mutate(walk_peak_cat = paste(day_of_week, walk_peak_cat, sep = "_")) %>%
    group_by(GEOID, walk_peak_cat) %>%
    reframe(walk_count = n_distinct(userId, walkNum)) %>%
    spread(walk_peak_cat, walk_count) %>%
    relocate("weekday_pm_peak", .before = "weekday_off_peak")

  # get walk distance and duration summary (min/max/med/mean)
  dist_dur_summary <- valid_walk %>%
    filter(!is.na(GEOID)&GEOID != "") %>%
    distinct(GEOID, userId, walkNum, walk_duration, walk_distance) %>%
    group_by(GEOID) %>%
    summarise_at(vars(walk_distance, walk_duration),
                 list(
                   min = ~min(.),
                   max = ~max(.),
                   med = ~median(.),
                   mean = ~mean(.)
                 )) %>%
    mutate_if(is.numeric, ~round(., 2)) %>%
    unite(min_dist_dur, ends_with("_min"), sep = " m, ") %>%
    unite(max_dist_dur, ends_with("_max"), sep = " m, ") %>%
    unite(med_dist_dur, ends_with("_med"), sep = " m, ") %>%
    unite(mean_dist_dur, ends_with("_mean"), sep = " m, ") %>%
    mutate_at(vars(matches("dist\\_dur")), ~paste(., "minutes"))

  # get count of utilitarian vs recreational walks for walks in walkable areas
  util_summary <- valid_walk %>%
    group_by(GEOID, IsUtilWalk) %>%
    reframe(count_util = n_distinct(userId,walkNum)) %>%
    spread(IsUtilWalk, count_util)

  # combine all metrics into a singular table
  summary_df <- general_metrics %>%
    left_join(total_days, by = "GEOID") %>%
    left_join(util_summary, by = "GEOID") %>%
    left_join(dist_dur_summary, by = "GEOID") %>%
    left_join(peak_summary, by = "GEOID") %>%
    filter(!is.na(GEOID)&GEOID != "")

  # replace all NA with 0
  summary_df[is.na(summary_df)] <- 0
  return(summary_df)
}

#-------------------------------Walking Prevalence------------------------------
#' Read in tract-level population data from ACS. If the specified year is not available, use most recent year (2022).
#' @param county_name NAMELSAD county name (excluding the word 'County', e.g., "Allegheny" instead of "Allegheny County")
#' @param state State of county (e.g., "PA" or "Pennsylvania")
#' @param year Census year of county information to extract from `tigris`
#' @importFrom data.table fread
#' @importFrom utils data
#' @import dplyr stringr
#' @return ACS population estimate by given year for each census tract in a given county
#' @export

get_acs_population_cache <- function(county_name, state, year){

  # avoid "no visible binding" warnings
  US_tract_population2022 <- GEOID <- NAME <- NULL

  # get ACS file from saved area

  if(year == 2022){
    message("Using population estimates from cached year 2022 with TIGRIS shapefiles from ", year)
  }else{
    warning("ACS population count for ", year," is not available. Using population estimates from cached year 2022 with TIGRIS shapefiles from ", year)
  }
  data("US_tract_population2022", envir = environment())

  # get county census tract shapefiles in TIGRIS
  county_tracts <- get_county_tracts(county_name = county_name,
                                     state = state,
                                     year = year)

  tract_pop <- US_tract_population2022 %>%
    mutate_if(is.character, str_squish) %>%
    mutate(GEOID = stringr::str_pad(GEOID, width = 11, side = "left", pad = 0)) %>%
    right_join(county_tracts[,c("GEOID","geometry")], by = "GEOID") %>%
    separate(NAME, into = c("NAMELSAD", "COUNTY", "STATE"), sep = "; ")

  return(tract_pop)
}

#' Read in tract-level population data from ACS using a Census API key to get specific year. If the specified year is not available, use 2022 estimates.
#' @param api_key Census API key generated using the [Census API request form](https://api.census.gov/data/key_signup.html)
#' @param county_name NAMELSAD county name (excluding the word 'County', e.g., "Allegheny" instead of "Allegheny County")
#' @param state State of county (e.g., "PA" or "Pennsylvania")
#' @param year Census year of county information to extract from `tigris`
#' @import dplyr tidycensus
#' @return ACS population estimate by given year for each census tract in a given county
#' @export

get_acs_population_api <- function(api_key, county_name, state, year){
  # avoid "no visible binding" warnings
  NAME <- NULL

  # get county census tract shapefiles in TIGRIS
  county_tracts <- get_county_tracts(county_name = county_name,
                                     state = state,
                                     year = year
  )

  # read in Census API key without installation
  suppressMessages(census_api_key(api_key, overwrite = TRUE, install = FALSE))

  # extract population estimates by census tract for every county in the selected state using the Census API
  # if selected year does not exist or API key is incorrect, then use cached 2022 population data calculated using get_acs_population_cache()
  tract_pop <- tryCatch(expr = {
    suppressMessages(get_acs(geography = "tract",
                             county = county_name,
                             state = state,
                             table = "B01003",
                             year = year)) %>%
      separate(NAME, into = c("NAMELSAD", "COUNTY", "STATE"), sep = "; ") %>%
      mutate_if(is.character, str_squish) %>%
      right_join(county_tracts[,c("GEOID","geometry")], by = "GEOID") %>%
      as.data.frame()
  },
  error = function(e) {
    get_acs_population_cache(county_name = county_name,
                             state = state,
                             year = year)
  }
  )

  return(tract_pop)
}

#' Calculate tract level walk prevalence
#' @param walks_in_walkable_df Data frame with extracted walks using `get_walks()` and then mapped using to county tract GEOIDs using `find_walkable()`
#' @param acs_tract_population_df Data frame created using `get_acs_population_api()` or `get_acs_population_cache()`
#' @import sf dplyr
#' @return Census tract-level walk prevalence table
#' @export
tract_walk_prevalence <- function(walks_in_walkable_df, acs_tract_population_df){

  # avoid "no visible binding" warnings
  buffer <- GEOID <- userId <- walkNum <- fraction <- NULL
  estimate <- count_walkable <- pop_estimate <- geometry <- NAMELSAD <- NULL

  # keep only walks in walkable areas and confirm that GEOIDs are in the correct format
  valid_walk <- walks_in_walkable_df %>%
    filter(fraction > 0.75 & buffer == 1) %>%
    separate_longer_delim(GEOID, delim = ";") %>% # if a ping is in multiple GEOID, split each GEOID into separate row
    check_GEOID()

  # create walk prevalence table
  walk_prevalence <- valid_walk %>%
    group_by(GEOID) %>%
    summarise(count_walkable = n_distinct(userId, walkNum)) %>%
    right_join(acs_tract_population_df[, c("GEOID", "NAMELSAD","estimate", "geometry")], by = "GEOID") %>%
    rename("pop_estimate" = estimate) %>%
    mutate(count_walkable = ifelse(is.na(count_walkable), 0, count_walkable),
           walk_prev = ifelse(pop_estimate !=0, count_walkable/pop_estimate, NA),
           GEOID = ifelse((is.na(GEOID)|GEOID == ""), "No GEOID", GEOID)
    ) %>%
    relocate(geometry,count_walkable, .after = NAMELSAD)

  return(walk_prevalence)
}

#' Calculate walk prevalence for the entire county
#' @param walks_in_walkable_df Data frame with extracted walks found after using `get_walks()` then mapped using to county tract GEOIDs using `find_walkable()`
#' @param acs_tract_population_df Data frame created using `get_acs_population_api()` or `get_acs_population_cache()`
#' @import dplyr
#' @importFrom rlang .data
#' @return County-level walk prevalence summary table
#' @export
county_walk_prevalence <- function(walks_in_walkable_df, acs_tract_population_df){
  # get total county population count
  pop_count <- acs_tract_population_df %>%
    summarise(sum(.data$estimate, na.rm = T)) %>%
    pull()

  # get total walks in walkable areas count
  count_walkable <- walks_in_walkable_df %>%
    filter(.data$fraction > 0.75) %>%
    summarise(tot_walkable = n_distinct(.data$userId, .data$walkNum)) %>%
    pull()

  # combine into a summary table
  prev_summary <- data.frame(count_walkable = count_walkable,
                             pop_estimate = pop_count,
                             walk_prev = count_walkable/pop_count)

  return(prev_summary)

}
#' Calculate census tract-level walk prevalence metrics
#' @param tract_walk_prevalence_df data frame resulting from `tract_walk_prevalence()`
#' @import dplyr tidyr
#' @return Walk prevalence summary statistics table
#' @export
# summary metrics in a nice table for walk prevalence data frames
walk_prevalence_summary <- function(tract_walk_prevalence_df){
  # avoid "no visible binding" warnings
  metric <- value <- variable <- Total <- Mean <- SD <- Median <- Min <- Max <- NULL

  prev_metrics <- tract_walk_prevalence_df %>%
    data.frame() %>%
    summarise(across(
      .cols = where(is.numeric),
      .fns = list(Mean = mean,
                  SD = sd,
                  Median = median,
                  Min = min,
                  Max = max,
                  Total = sum),
      na.rm = TRUE,
      .names = "{col} {fn}"
    )) %>%
    tidyr::gather() %>%
    separate(key, into = c("variable", "metric"), sep = " ") %>%
    tidyr::spread(metric, value) %>%
    mutate(Total = ifelse(variable == "walk_prev", NA, Total)) %>%
    relocate(Mean, SD, Median, Min, Max, Total, .after = variable) %>%
    mutate_if(is.numeric, ~round(., 3))

  return(prev_metrics)

}

# Sidewalk Density --------------------------------------------------------

#' Calculate sidewalk density for the entire county
#' @param county_name NAMELSAD county name (excluding the word 'County', e.g., "Allegheny" instead of "Allegheny County")
#' @param state State of county (e.g., "PA" or "Pennsylvania")
#' @param year Census year of county information to extract from `tigris`
#' @param ped_accessible_highway_keys OSM key values for pedestrian-accessible 'highway' features
#' @param sidewalk_keys OSM key values for sidewalk features
#' @import dplyr sf osmdata
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom tigris counties
#' @importFrom rlang .data
#' @return Sidewalk density table
#' @export
county_sidewalk_density <- function(county_name,
                                     state,
                                     year,
                                     ped_accessible_highway_keys = c("trunk", "primary_link",
                                                                     "living_street", "secondary",
                                                                     "secondary_link", "tertiary",
                                                                     "tertiary_link"),
                                     sidewalk_keys = c("pedestrian", "footway",
                                                       "path", "residential",
                                                       "track")){

  # Read in for given county
  county <- counties(state = state,
                     year = year,
                     keep_zipped_shapefile = T,
                     refresh = T,
                     progress_bar = FALSE
                     ) %>%
    dplyr::filter(grepl(county_name, .data$NAME, ignore.case = T)) %>%
    st_transform(crs=4326)

  # Get sidewalk data (E. Moro values)
  osm_sidewalks <- st_bbox(county) %>%
    opq(timeout=20000, datetime = str_glue("{year}-01-01T12:00:00Z")) %>%  # overpass query
    add_osm_feature(key = "highway", value = sidewalk_keys) %>% osmdata_sf()

  # Get pedestrian accessible highway data
  osm_highways <- st_bbox(county) %>%
    opq(timeout = 20000, datetime = str_glue("{year}-01-01T12:00:00Z")) %>%
    add_osm_feature(key = "highway", value = ped_accessible_highway_keys) %>% osmdata_sf()

  # Get line data
  osm_sidewalks_lines <- osm_sidewalks$osm_lines %>% dplyr::select(.data$osm_id, .data$geometry)
  osm_highway_lines <- osm_highways$osm_lines %>% dplyr::select(.data$osm_id, .data$geometry)

  # Get side walk length from linestring using st_length()
  osm_sidewalks$line_length <- st_length(osm_sidewalks_lines$geometry)
  osm_sidewalks_km <- as.numeric(sum(osm_sidewalks$line_length)/1000)

  osm_highway_lines$line_length <- st_length(osm_highway_lines$geometry)
  osm_highway_km <- as.numeric(sum(osm_highway_lines$line_length/1000))

  # Calculate sidewalk density for given county
  osm_density <- as.numeric(osm_sidewalks_km/osm_highway_km)

  summary_stats <- data.frame("county_name" = county_name,
                              "sidewalk_length_km" = osm_sidewalks_km,
                              "ped_accessible_highway_length_km" = osm_highway_km,
                              "sidewalk_density" = osm_density)
  return (summary_stats)
}

#' Extract sidewalk density features from OSM
#' @param county_tracts Dataframe of all TIGRIS census tract shapefiles for a given county using `get_county_tracts()`
#' @param year Year to extract data from the OSM database as it was up to the specified year. This is usually the year variable used in `get_county_tracts()`
#' @param sidewalk_keys OSM key values for sidewalk features. Default key values are c("pedestrian","footway","path","residential","track")
#' @param ped_accessible_highway_keys OSM key values for pedestrian-accessible 'highway' features. Default key values are c("trunk", "primary_link","living_street", "secondary","secondary_link", "tertiary","tertiary_link")
#' @param quietly FALSE to print out tract progress statements to track completion, and TRUE to run function without progress statements. Defaults to TRUE
#' @import dplyr osmdata
#' @importFrom stringr str_glue
#' @return A list() of sidewalk density OSM features
#' @export
get_density_feat <- function(county_tracts,
                             year,
                             sidewalk_keys = c("pedestrian",
                                               "footway",
                                               "path",
                                               "residential",
                                               "track"),
                             ped_accessible_highway_keys = c("trunk", "primary_link",
                                                             "living_street", "secondary",
                                                             "secondary_link", "tertiary",
                                                             "tertiary_link"),
                             quietly = T){

  # avoid "no visible binding" warnings
  GEOID <- NULL

  # order the county tracts by GEOID
  county_tracts <- county_tracts %>%
    arrange(desc(GEOID))
  start_time <- Sys.time()
  # make list for items
  density_lst <- list()
  for (i in 1:nrow(county_tracts)){
    sel_tract <- county_tracts[i,]
    # find sidewalk features in tracvt
    sidewalk_tract <- st_bbox(sel_tract) %>%
      opq(timeout=2000, datetime = str_glue("{year}-01-01T12:00:00Z")) %>% # overpass query
      add_osm_feature(key = "highway", value = sidewalk_keys) %>% osmdata_sf()

    sidewalk_filter <- sidewalk_tract[c("osm_lines")]

    # find pedestrian accessible highway features in tract
    highway_tract <- st_bbox(sel_tract) %>%
      opq(timeout=2000, datetime = str_glue("{year}-01-01T12:00:00Z")) %>% # overpass query
      add_osm_feature(key = "highway", value = ped_accessible_highway_keys) %>% osmdata_sf()

    highway_filter <- highway_tract[c("osm_lines")]

    # create name for saving into list
    sidewalk_name <- paste0(sel_tract$GEOID, "_sidewalk")
    highway_name <- paste0(sel_tract$GEOID, "_highway")

    if(quietly == F){
      message(paste("Census tract",i,"out of",nrow(county_tracts)))
    }

    density_lst[[sidewalk_name]] <- sidewalk_filter
    density_lst[[highway_name]] <- highway_filter
  }
  end_time <- Sys.time()
  message(end_time - start_time)
  return(density_lst)

}

#' Save sidewalk density list created using `get_density_feat()` into an .RData file for later replication or speed
#' @param density_feat_lst OSM sidewalk density feature list created using `get_density_feat()`
#' @param use_save_directory Input directory path to save OSM features for future use. This must be a directory and not a file path
#' @param save_file_name Character string naming a file to save `density_feat_lst`. Default name is `sidewalk_density_feat`
#' @return Message with the `use_save_directory` path
#' @export
save_density_feat <- function(density_feat_lst, use_save_directory, save_file_name = 'sidewalk_density_feat'){
  # Stop if directory is invalid
  if(!dir.exists(use_save_directory)){
    stop(paste0(use_save_directory,
                " path does not exist. Please confirm that you are inputting the correct path."))
  }
  if(grepl("\\.RData", save_file_name)){
    stop("File name cannot end with .RData. This function will create the .RData file for you.")
  }
  # save entire list as an .RData file
  fileOut <- file.path(use_save_directory, paste0(save_file_name,".RData"))
  saveRDS(density_feat_lst, file=fileOut)

  message(paste0("OSM features saved to ", use_save_directory))

}

#' Load sidewalk density features list  created using `get_density_feat()` and saved as an .RData file using `save_density_feat()`
#' @param save_file_path Specific file path of the .RData file containing OSM sidewalk density features. Save file created using `save_density_feat()`
#' @return OSM sidewalk density feature list
#' @export
load_density_feat <- function(save_file_path){
  if(!file.exists(save_file_path)){
    stop(paste0(save_file_path," path does not exist. Please confirm that you are inputting the correct file location (e.g., '/data/sidewalk_density_feat.RData')."))
  }
  if(!grepl("\\.RData",save_file_path)){
    stop("File input is not an .RData file. Please input the exact path of the .RData file (e.g., '/data/sidewalk_density_feat.RData').")
  }
  density_lst <- readRDS(save_file_path)
  return(density_lst)
}

#' Calculate sidewalk density by tract for a given county
#' @param county_tracts Dataframe of all TIGRIS census tract shapefiles for a given county using `get_county_tracts()`
#' @param sidewalk_density_feature_lst OSM sidewalk density feature list created using `get_density_feat()`
#' @import dplyr sf
#' @importFrom stringr str_pad
#' @importFrom utils stack
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @return Table containing sidewalk density summary values for each census tract in a given county
#' @export
tract_sidewalk_density <- function(county_tracts,
                                    sidewalk_density_feature_lst
) {
  # avoid "no visible binding" warnings
  ind <- values <- sidewalk_length_km <- ped_accessible_highway_length_km <- NULL
  GEOID <- NAMELSAD <- geometry <- sidewalk_density <- NULL

  # Ensure that there are features inside the sidewalk_density_feature_lst
  if(length(sidewalk_density_feature_lst) == 0){
    stop("OSM sidewalk_density_feature_lst is empty. Please try again or rerun get_density_feat")
  }

  # Get sidewalk and highway lengths from each census tract file
  osm_sidewalks_tract_lens <- list()
  osm_highway_tract_lens <- list()

  for (i in 1:nrow(county_tracts)) {
    sel_tract <- county_tracts[i,]

    # get name of features from list
    sidewalk_name <- paste0(sel_tract$GEOID, "_sidewalk")
    highway_name <- paste0(sel_tract$GEOID, "_highway")

    # Select relevant features
    sidewalks <- sidewalk_density_feature_lst[[sidewalk_name]]$osm_lines
    highways <- sidewalk_density_feature_lst[[highway_name]]$osm_lines

    # Get lengths for tracts (in km), return -1 if unable to calculate
    tryCatch(expr = {osm_sidewalk_lens <- as.numeric(sum(st_length(sidewalks$geometry)))/1000},
             error = function(e){ osm_sidewalk_lens <- -1})
    tryCatch(expr = {osm_highway_lens <- as.numeric(sum(st_length(highways$geometry)))/1000},
             error = function(e){ osm_highway_lens <- -1})

    # Append to list
    tract_name <- sel_tract$GEOID

    osm_sidewalks_tract_lens[[tract_name]] <- osm_sidewalk_lens
    osm_highway_tract_lens[[tract_name]] <- osm_highway_lens


  }

  sidewalk_tracts <- as.data.frame(stack(osm_sidewalks_tract_lens)) %>%
    relocate(ind, .before = values) %>%
    rename("sidewalk_length_km" = values,
           "GEOID" = ind
    )

  highway_tracts <- as.data.frame(stack(osm_highway_tract_lens)) %>%
    relocate(ind, .before = values) %>%
    rename("ped_accessible_highway_length_km" = values,
           "GEOID" = ind
    )

  tract_summary_stats <- sidewalk_tracts %>%
    dplyr::full_join(highway_tracts, by = "GEOID") %>%
    mutate(sidewalk_density = sidewalk_length_km/ped_accessible_highway_length_km) %>%
    arrange(desc(sidewalk_density))

  # Join tract names on summary table
  tract_names_stats <- tract_summary_stats %>%
    # added to match county_tracts GEOID format (11-bit char)
    mutate(GEOID = stringr::str_pad(GEOID, width = 11, side = "left", pad = 0)) %>%
    dplyr::left_join(county_tracts[c("GEOID", "NAMELSAD")], by = "GEOID") %>%
    relocate(c(GEOID, NAMELSAD, geometry), .before = sidewalk_length_km)

  return(tract_names_stats)
}
