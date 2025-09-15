####-------------------------------Define Walkable Buffer Functions -------------------------------
#' Suppress annoying warnings that don't effect calculation
#' @param .expr Expression to supress
#' @return None
#' @keywords internal

suppress_warnings <- function(.expr, .f, ...) {
  eval.parent(substitute(
    withCallingHandlers( .expr, warning = function(w) {
      cm <- conditionMessage(w)
      cond <-
        if(is.character(.f)) grepl(.f, cm) else rlang::as_function(.f)(cm,...)
      if (cond) {
        invokeRestart("muffleWarning")
      }
    })
  ))
}

#' Create directory based on county name
#' @param county_name NAMELSAD county name (excluding the word 'County', e.g., "Allegheny" instead of "Allegheny County")
#' @param state State of county (e.g., "PA" or "Pennsylvania")
#' @param data_loc Data location directory. Default is `NA`, which creates a directory within `/tmp` that follows a CountyState format (e.g., `/San FranciscoCA/`). Otherwise, creates the same directory within the location specified.
#' @return Directory path of the new directory created within either `/tmp` or the location specified in `data_loc`.
#' @export

create_directory <- function(county_name, state, data_loc = NA){

  # if data_loc is NA, then create directory in temp directory location, which clears after each session
  if(is.na(data_loc)){
    temp_loc <- tempdir()
    subDir <- file.path(temp_loc, paste0(county_name,state))
  }else{ # else need to add in data location as a string. E.g., "./data/". The directory will be created there
    subDir <- file.path(data_loc, paste0(county_name,state))
  }

  dir.create(subDir, showWarnings = FALSE)

  return(subDir)
}

#' Takes state, county name using abbrev, FIPS, or full name + year.
#' @param county_name NAMELSAD county name (excluding the word 'County', e.g., "Allegheny" instead of "Allegheny County")
#' @param state State of county (e.g., "PA" or "Pennsylvania")
#' @param year Census year of county information to extract from `tigris`
#' @import sf dplyr
#' @importFrom tigris tracts
#' @return Returns shape file census tracts for county
#' @export

get_county_tracts <- function(county_name, state, year){
  # avoid "no visible binding" warnings
  geometry <- NULL

  cts <- tracts(state = state, county = county_name, year = year, progress_bar = FALSE) %>%
    st_transform(crs=4326)

  # Calculate 500 m buffer around each census tract
  county_tracts <- cts %>%
    mutate(buff_geometry = st_set_crs(st_buffer(x = geometry, dist = 500*1e-5), 4326)) %>%
    suppress_warnings( ~grepl("longitude|spatially constant",.))

  return(county_tracts)
}

#' Converts all non-null list items from osm_feat to 4326
#' this no longer requires hard-coded osm list items to convert
#' @param osm_feat A collection of walkable area shapefiles created in `get_osm_feat()`
#' @importFrom sf st_set_crs
#' @return Updated walkable area shape files projected to correct CRS
#' @keywords internal

convert_osm_crs <- function(osm_feat){

  # remove osm_ list items that are NULL/empty
  remove_null <- Filter(Negate(is.null), osm_feat)

  # get the osm list items that start with osm_ (the geometry list items)
  get_osm_names <- grep("^osm_",names(remove_null), value = T)

  # convert all non-null items to 4326
  remove_null[get_osm_names] <- lapply(get_osm_names,
                                       function(x) st_set_crs(remove_null[[x]], 4326))

  return(remove_null)
}

#' Get OpenStreetMap (OSM) features you want to include as walkable areas.
#' Defaults to highway as key and pedestrian-friendly features as values
#' @param county_tracts Dataframe of all TIGRIS census tract shapefiles for a given county using `get_county_tracts()`.
#' @param osm_key A unique key for OpenStreetMap, defaults to `highway`. More information can be found here: https://wiki.openstreetmap.org/wiki/Key:highway
#' @param osm_feat_val A list of features to consider for a given OSM key.
#' @param keep_osm_geometry Defaults to `osm_lines`. Otherwise can specify one of osm_lines, osm_points, osm_polygons, osm_multipolygons.
#' @param ... Other parameters for `osmdata::add_osm_feature()`
#' @import sf osmdata dplyr
#' @return A OSM shapefile created from designated walkable key-value pairs
#' @export

get_osm_feat <- function(county_tracts,
                         osm_key = "highway",
                         osm_feat_val,
                         keep_osm_geometry = "osm_lines", # default include osm_lines. Else can specify one of osm_lines, osm_points, osm_polygons, osm_multipolygons
                         ...
){

  if(missing(osm_feat_val)){
    osm_feat <- st_bbox(county_tracts) %>%
      opq(timeout=20000)%>%
      add_osm_feature(key = osm_key,
                      ...
      ) %>%
      osmdata_sf()

    # keep only the osm_ feature you want. This reduces eventual size of osm feature
    osm_filter <- osm_feat[c("bbox", "overpass_call", "meta", keep_osm_geometry)]

  }else{
    osm_feat <- st_bbox(county_tracts) %>%
      opq(timeout=20000)%>%
      add_osm_feature(key = osm_key,
                      value = osm_feat_val,
                      ...
      ) %>%
      osmdata_sf()

    # keep only the osm_ feature you want. This reduces eventual size of osm feature
    osm_filter <- osm_feat[c("bbox", "overpass_call", "meta", keep_osm_geometry)]
  }

  # reproject all osm data to 4326 crs
  osm_feat_converted <- convert_osm_crs(osm_filter)

  return(osm_feat_converted)
}

#' Grabs default OpenStreetMap (OSM) key-features from E. Moro's study to extract walkable areas
#' @param county_tracts Dataframe of all TIGRIS census tract shapefiles for a given county using `get_county_tracts()`.
#' @import sf
#' @return A OSM shapefile created from default walkable key-value pairs
#' @export

default_walkable_area <- function(county_tracts){
  highway_secondary <- get_osm_feat(county_tracts,
                                    osm_key = "highway",
                                    osm_feat_val = c("secondary", "tertiary",
                                                     "secondary_link","tertiary_link"),
                                    keep_osm_geometry = "osm_lines"
  ) %>%
    suppress_warnings(~grepl("st_crs", .))

  highway_local <- get_osm_feat(county_tracts,
                                osm_key = "highway",
                                osm_feat_val = c("residential", "living_street","cycleway",
                                                 "pedestrian", "footway","track","path"),
                                keep_osm_geometry = "osm_lines") %>%
    suppress_warnings(~grepl("st_crs", .))

  parks <- get_osm_feat(county_tracts,
                        osm_key = "leisure",
                        osm_feat_val = c("park","playground",
                                         "golf_course","nature_reserve"),
                        keep_osm_geometry = "osm_polygons"
  ) %>%
    suppress_warnings(~grepl("st_crs", .))

  osm_lst <- list(highway_secondary = highway_secondary,
                  highway_local = highway_local,
                  parks = parks)

  return(osm_lst)

}

#' Get intersection of OSM features and census tracts, then buffer the feature in tract
#' @param osm_feat Specific walkable area OSM feature extracted using `get_osm_feat()` or `default_walkable_area()`
#' @param cts_tract Single TIGRIS census tract shapefile for a given county using `get_county_tracts()`.
#' @import sf
#' @return Buffered OSM features that are within the given `cts_tract` census tract.
#' @keywords internal

intersect_buffer <- function(osm_feat, cts_tract){
  find_osm_geom <- grep("^osm_", names(osm_feat), value = T)
  if(length(find_osm_geom) > 1){
    stop("There is more than one 'osm_' geometry, please ensure that only one exists in the extracted osm feature")
  }else{
    # intersect osm feature with tract boundaries
    # check if all points are valid. If not, then make valid. If yes, then continue
    if(all(st_is_valid(osm_feat[[find_osm_geom]]))){
      cts_feat <- suppressMessages(st_intersection(osm_feat[[find_osm_geom]], cts_tract$buff_geometry))
    }else{
      cts_feat <- suppressMessages(st_intersection(st_make_valid(osm_feat[[find_osm_geom]]), cts_tract$buff_geometry))
    }

    # get 20 meters buffers around feature in tract
    buffer_feat <- suppressMessages(st_buffer(cts_feat,dist=20*1e-5))

    # keep only certain columns
    buffer_feat <- select(buffer_feat, c(matches("^osm_id$|^geometry$|^name$")))

    return(buffer_feat)
  }
}

#' Find walkable area within a single tract
#' @param cts_tract Single TIGRIS census tract shapefile for a given county using `get_county_tracts()`.
#' @param feat_lst List of OSM features to use as walkable areas. Can use `default_walkable_area()`.
#' @import sf
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom purrr possibly
#' @importFrom nngeo st_remove_holes
#' @return Walkable buffers
#' @keywords internal

area_loop <- function(cts_tract, feat_lst){
  # avoid "no visible binding" warnings
  . <- NULL

  # This will remove any features that do not exist in the tract or that trigger errors
  # buffer_total will then only include non-errored, non-empty OSM features
  safe_intersect <- possibly(intersect_buffer,
                             otherwise = st_sf(osm_id = 1,name = NA, geometry = sf::st_sfc(st_polygon())),
                             quiet = F)

  # intersect streets and parks with tract boundaries, create buffer, and combine into one
  buffer_total <- lapply(feat_lst, function(x) safe_intersect(osm_feat = x, cts_tract = cts_tract) %>%
                           suppress_warnings("spatially|longitude")
  ) %>%
    bind_rows()

  if(nrow(buffer_total) == 0){
    return(buffer_total)
  }

  # if there are multipolygons, convert to polygon. Avoids warning about not keeping all polygons
  buffer_total_convert <- lapply(1:nrow(buffer_total), function(x)
    st_cast(buffer_total[x,], "POLYGON")) %>%
    do.call(what = rbind, args = .) %>%
    suppress_warnings("geometries")

  buffer <- suppressMessages(st_union(buffer_total_convert))

  #remove small holes in the total buffer polygon
  buffer_simplified <- nngeo::st_remove_holes(buffer,max_area=(200*1e-5)^2)

  return(buffer_simplified)
}

#' Get all walkable areas and their buffers by census tract (GEOID)
#' @param county_tracts Dataframe of all TIGRIS census tract shapefiles for a given county using `get_county_tracts()`.
#' @param feat_lst list of OSM features within county census tracts. `Use default_walkable_area()` to use default list of walkable areas.
#' @param quietly Default to `TRUE`. If set to `FALSE`, will print out number of tracts left to extract OSM features.
#' @import sf
#' @return List of walkable areas with a 20m buffer saved into a directory
#' @export

walkable_areas <- function(county_tracts, feat_lst, quietly = TRUE){
  suppressMessages(sf_use_s2(FALSE))
  area_lst <- list()
  start_time <- Sys.time()

  for(i in 1:nrow(county_tracts)){
    tract_selected <- county_tracts[i,]
    if(!quietly){
      message(paste0("Checking for OpenStreetMap areas in census tract ",i, " out of ", nrow(county_tracts)))
    }

    # get buffer for each tract for walkable areas
    buffer_simplified <- area_loop(tract_selected, feat_lst) %>%
      suppress_warnings( ~grepl("longitude|spatially constant|degrees",.)) %>%
      suppressMessages() # remove annoying messages about distance defaulting to degrees

    if(all(st_is_empty(buffer_simplified))) next

    # append tract buffer to list
    area_lst[[tract_selected$GEOID]] <- buffer_simplified
  }

  return(area_lst)

}

#' Get all walkable areas and their buffers by census tract (GEOID)
#' @param walkable_area_lst List of OSM features split out by county census tract GEOID from `walkable_area()`
#' @param use_save_directory input directory path where you want to save OSM features for future use.
#' @return Directory path where the walkable areas were saved into to be used in `find_walkable()`
#' @export

save_walkable_areas <- function(walkable_area_lst, use_save_directory){
  # Stop if directory is invalid
  if(!dir.exists(use_save_directory)){
    stop(paste0(use_save_directory,
                " path does not exist. Please confirm that you are inputting the correct path."))
  }
  # save each tract walkable area in list as an .RData file
  for(n in 1:length(walkable_area_lst)){
    tract_id <- names(walkable_area_lst)[n]
    fileOut <- file.path(use_save_directory, paste0("buffer_",tract_id,".RData"))

    buffer_simplified <- walkable_area_lst[[n]]

    save(buffer_simplified,file=fileOut)
  }
  message(paste0("OSM features saved to ", use_save_directory))

  return(use_save_directory)
}

#' Get all walkable areas and their buffers by census tract (GEOID)
#' @param use_area_directory input directory path where walkable OSM features were saved after `walkable_areas()` and `save_walkable_areas()`for future use.
#' @return Load walkable areas from `walkable_areas()` into a buffer_lst to use in `find_walkable()`
#' @export

load_walkable_areas <- function(use_area_directory){
  if(!dir.exists(use_area_directory)){
    stop("Location of walkable area list does not exist. Please input different value in use_area_directory")
  }

  area_files <- list.files(use_area_directory, pattern = ".RData", full.names = T)

  if(length(area_files) == 0){
    stop(paste0("No valid walkable area features are in directory ",use_area_directory,". Please try a different directory path."))
  }

  walkable_area_lst <- list()
  buffer_simplified <- NULL

  for(f in area_files){
    tract_id <- str_match(f, "buffer_\\s*(.*?)\\s*\\.RData")[2]

    load(f)

    walkable_area_lst[[tract_id]] <- buffer_simplified
  }

  return(walkable_area_lst)
}


####-------------------------------Define Walks in Walkable Area Functions ------------------------------
#' Finds all the walks in walkable areas
#' @param extracted_walk_df Extracted walks dataframe from LBS data after using `get_walks()`
#' @param county_tracts Dataframe of all TIGRIS census tract shapefiles for a given county using `get_county_tracts()`.
#' @param walkable_area_lst A `list()` object of tracts containing walkable areas from `walkable_areas()`.
#' If area list is saved in a directory, use `load_walkable_areas()` to load list first.
#' @param quietly Default to `TRUE`. If set to `FALSE`, will print out progress report of tracts left
#' to map walking trips to, number of tracts without walk trips in walkable areas,
#' number of walk trips in walkable areas, and length of time it took to map walks to walkable areas.
#' @import dplyr sf
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @return Data frame from `extracted_walk_df` with added columns, `buffer`, `fraction`, and `GEOID`.
#' The `buffer` column has values of either `1` or `0`,
#' with `1` indicating the LBS ping is in a walkable area and a `0` if not.
#' The `GEOID` column contains the `GEOID` of the walkable area that the ping is located in.
#' The `fraction` column contains the proportion of pings in a given walk trip that are in walkable areas.
#' A walking trip is considered to be in a walkable area if > 0.75 pings are in walkable areas.
#' @export
find_walkable <- function(extracted_walk_df,
                           county_tracts,
                           walkable_area_lst,
                           quietly = T){

  # avoid "no visible binding" warnings
  buffer_id <- GEOID <- fraction <- buffer <- userId <- walkNum <- NULL


  start_time <- Sys.time()

  # Create coordinates from lat lon columns
  coords_walk <- st_as_sf(extracted_walk_df,coords=c("lon","lat"),crs=4326)

  # this empty list will contain pings in walkable areas grouped by tract GEOID
  valid_pings <- list()

  # Error gets thrown if TRUE
  sf_use_s2(FALSE)

  for(i in 1:nrow(county_tracts)){

    if(!quietly){
      message(paste0("Checking walks in census tract ",i, " out of ", nrow(county_tracts)))
    }

    if(!county_tracts[i,]$GEOID %in% names(walkable_area_lst)) next # if tract not in buffers, then skip
    buffer_simplified <- walkable_area_lst[[county_tracts[i,]$GEOID]]

    # check if walk coordinates are in tract walkable buffer area
    tract_check <- st_contains(buffer_simplified, coords_walk) %>%
      suppress_warnings( ~grepl("longitude|spatially constant",.)) %>%
      suppressMessages()

    # convert row ID and GEOID mapping to data frame
    tract_check <- as.data.frame(tract_check[[1]])

    # Add GEOID as column name
    colnames(tract_check) <- county_tracts[i,]$GEOID

    # cast dataframe to long, resulting in df with GEOID and buffer_id as rownum from original df
    # makes resulting df easy to join to extracted_walk_df
    tract_check <- tract_check %>%
      gather(key = "GEOID", value = "buffer_id")

    # save each census tract dataframe as an item in valid_pings list
    valid_pings[[i]] <- tract_check

  }

  # join all dataframes in valid pings and group by buffer_id/original rownum for extracted_walk_df
  # if there are multiple GEOID for a ping, then list all GEOIDs and separate by semicolon
  # users can then separate each row by semicolon to get a full list of all GEOIDs used
  valid_pings <- rbindlist(valid_pings) %>%
    group_by(buffer_id) %>%
    summarise(GEOID = paste(GEOID, collapse = ";")) # if multiple GEOID, then separate by semicolon

  # create final walkable df with the walkable pings
  walks_in_walkable_df <- extracted_walk_df %>%
    mutate(buffer_id = row_number()) %>% # create buffer_id to join with valid_pings dt
    left_join(valid_pings, by = "buffer_id") %>% # join the list of pings in walkable areas by buffer_id (rownumber)
    mutate(buffer = ifelse(is.na(GEOID)|GEOID == "", 0, 1)) %>% # if the ping is not in a walkable area, then 0 else 1
    select(-buffer_id) %>% # remove buffer_id column (unnecessary)
    as.data.table()

  # calculate fraction of walk pings in walkable areas vs nonwalkable areas by userId and walk trajectory
  walks_in_walkable_df[, fraction:=sum(buffer)/.N, by = c("userId", "walkNum")]

  # get total walking trips in walkable areas
  count_walk <- walks_in_walkable_df %>%
    filter(fraction > 0.75) %>%
    summarise(n_distinct(userId, walkNum)) %>%
    pull()

  end_time <- Sys.time()
  how_long <- end_time-start_time

  if(!quietly){
    print(how_long)

    message("Number of walk trips in walkable areas: ", count_walk
    )
  }
  return(walks_in_walkable_df)
}

