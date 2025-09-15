#' Sample LBS Synthetic Dataset from Ness County, KS
#'
#' Sample LBS data synthetically generated for Ness County, KS for the period between 2021-07-01 00:00:00 and 2021-07-07 00:00:00.
#' Data is intended for testing package functions and experimenting with package output and does not contain any real LBS data.
#'
#' @format A `data.table` with five variables:
#' \describe{
#' \item{\code{id}}{Unique ID associated with each LBS user. This is a column to be used in `get_walks()`.}
#' \item{\code{latitude}}{Latitude coordinates of the GPS ping. This is a column to be used in `get_walks()`.}
#' \item{\code{longitude}}{Longitude coordinates of the GPS ping. This is a column to be used in `get_walks()`.}
#' \item{\code{unix_timestamp}}{13-digit UNIX timestamp of user GPS ping. This is a column to be used in `get_walks()`.}
#' \item{\code{horizontal_accuracy}}{Horizontal accuracy of the user GPS ping, default is 0 meters. This is a column to be used in `get_walks()`.}
#' }
#'@source Synthetically generated LBS dataset for Ness county.
#'
"synthetic_ness_data"

#' Sample LBS Synthetic Dataset from San Francisco County, CA
#'
#' Sample LBS data synthetically generated for San Francisco County, CA for the period between 2022-06-01 00:00:00 and 2022-06-07 00:00:00.
#' Data is intended for testing package functions and experimenting with package output and does not contain any real LBS data.
#'
#' @format A `data.table` with five variables:
#' \describe{
#' \item{\code{id}}{Unique ID associated with each LBS user. This is a column to be used in `get_walks()`.}
#' \item{\code{latitude}}{Latitude coordinates of the GPS ping. This is a column to be used in `get_walks()`.}
#' \item{\code{longitude}}{Longitude coordinates of the GPS ping. This is a column to be used in `get_walks()`.}
#' \item{\code{unix_timestamp}}{13-digit UNIX timestamp of user GPS ping. This is a column to be used in `get_walks()`.}
#' \item{\code{horizontal_accuracy}}{Horizontal accuracy of the user GPS ping, default is 0 meters. This is a column to be used in `get_walks()`.}
#' }
#'@source Synthetically generated LBS dataset for San Francisco county.
#'
"synthetic_san_francisco_data"

#' Extracted Walks from San Francisco County, CA Synthetic Data
#'
#' Walks extracted from synthetically generated for San Francisco County, CA `synthetic_san_francisco_data` and mapped to walkable areas across San Francisco census tracts.
#' Data is from the period between 2022-06-01 00:00:00 and 2022-06-07 00:00:00.
#' Data is intended for testing package functions and experimenting with package output and does not contain any real LBS data.
#'
#' @format A `data.table` with 20 variables:
#' \describe{
#' \item{\code{userId}}{Unique ID associated with each LBS user. This is a column to be used in `get_walks()`.}
#' \item{\code{time}}{10-digit UNIX timestamp of user GPS ping.}
#' \item{\code{lat}}{Latitude coordinates of the GPS ping.}
#' \item{\code{lon}}{Longitude coordinates of the GPS ping.}
#' \item{\code{accuracy}}{Horizontal accuracy of the user GPS ping, default is 0 meters.}
#' \item{\code{dist}}{For a given walk `userId` and `walkNum`, rolling distance `m` between two points over time.}
#' \item{\code{vel}}{For a given walk `userId` and `walkNum`, rolling speed `m/s` between two points over time.}
#' \item{\code{angle}}{Angle between two points used to determine whether points are within walk.}
#' \item{\code{walkNum}}{A walk number for a given device `userId`.}
#' \item{\code{total_walks_by_user}}{The total number of walks for	a given a device `userId`.}
#' \item{\code{numDaysActive}}{The total number of days given device has been active.}
#' \item{\code{numPoints}}{The total number of pings for	a given a walk.}
#' \item{\code{walk_duration}}{Total cumulative duration `sec` for a given walk.}
#' \item{\code{walk_distance}}{Total cumulative distance `m` for a given walk.}
#' \item{\code{day_of_week}}{Category for day of week (weekday, weekend).}
#' \item{\code{walk_peak_cat}}{Category for walk peak hours (am_peak, pm_peak, off_peak)..}
#' \item{\code{IsUtilWalk}}{Categorical variable for whether a walk is utilitarian or recreational given a predetermined start-end buffer distance (default of 100m).}
#' \item{\code{buffer}}{Value (0, 1) designating whether ping is within a walkable area.}
#' \item{\code{GEOID}}{U.S. Census unique identifier from the `tigris` package of the walkable area.
#' the ping is located in. If ping is outside of walkable area, `GEOID` will be `NA`.
#' If a ping is located in multiple walkable areas, then each `GEOID` of the intersected walkable areas will be returned and separated by a semicolon.}
#' \item{\code{fraction}}{Proportion of pings within a given `userId` and `walkNum` walk trip that are within walkable areas.
#' If the number of walk trip pings in walkable areas is > 0.75, the walk trip is considered to be in a walkable area.}
#' }
#'@source Walks extracted from `synthetic_san_francisco_data` and mapped to walkable areas across San Francisco census tracts.
#'
"san_francisco_walks"

#' Census Tract Population Estimates
#'
#' Census tract population estimates provided by the U.S. Census Bureau's 2018-2022 5-year American Community Survey
#'
#' @format A data frame with five variables:
#' \describe{
#' \item{\code{GEOID}}{U.S. Census unique identifier from the `tigris` package. Read more [here](https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html)}
#' \item{\code{NAME}}{Decoded GEOID listing census tract number, county, and state}
#' \item{\code{variable}}{ACS Table ID for Total Population (1-year estimate). Should always be B01003_001.}
#' \item{\code{estimate}}{Population estimate by census tract for a given year}
#' \item{\code{moe}}{Margin of error for population estimates}
#' }
#'
#' @source \url{https://www.census.gov/programs-surveys/acs/data/data-tables.html}
#'
"US_tract_population2022"
