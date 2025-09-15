#' Give names to walks
#' @param walkPtId NA
#' @return walkName
#' @keywords internal
makeWalkName <- function(walkPtId) {
    walkName <- paste0("WALK|", walkPtId)
    return(walkName)
}

#' Eliminate jumps in the data with low angle and instant in time.
#' Currently not just eliminating first too quick point.
#' @param dt Data table with walking records
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom Rcpp sourceCpp
#' @return Boolean array for records that contain a jump
#' @keywords internal
makeLoRandomJump <- function(dt) {

    # avoid "no visible binding" warnings
    lon <- lat <- userId <- vel <- time <- angle <- NULL

    dt[, dist := haverDistVector(lon, lat), by=userId]
    dt[, vel := dist/(time-data.table::shift(time))]

    # Finds first point in series going faster than 200.
    loIsFast <- dt[, (data.table::shift(vel) < 200) & (vel >= 200)]

    dt[, angle := findAngVector(lon, lat, dist)]
    loIsAngle <- dt[, (vel > 5) & (time - data.table::shift(time) < 300) &
                    ((angle < .25*pi)| is.nan(angle))]
    ret <- loIsFast | loIsAngle
    ret[is.na(ret)] <- FALSE
    return(ret)
}

#' Remove jumps from data
#' @param dt Data table with walking records
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom Rcpp sourceCpp
#' @return Cleaned data
#' @keywords internal
RemoveJumps <- function(dt) {

    for(i in c(1:3)) {
        loIsRemoveRow <- makeLoRandomJump(dt)
        dt <- dt[!(loIsRemoveRow),]
    }
    return(dt)
}

#' Remove still points from data
#' @param dt Data table with walking records
#' @return Boolean array for records that are still points
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @importFrom Rcpp sourceCpp
#' @keywords internal
makeLoIsStill <- function(dt) {
    # avoid "no visible binding" warnings
    lon <- lat <- userId <- accuracy <- NULL

    ret1 <- c(rep(FALSE, dt[,.N]))
    ret <- makeLoIsStillHelper(ret1, dt[, accuracy], dt[, lon], dt[, lat],
                               factor(dt[, userId]))
    return(ret)
}

#' Helper function to determine new walks using speed thresholds.
#' Assumes that entries are sorted by user then time, ascending.
#' @param dt Data table with walking records
#' @param minSpeed Minimum speed in m/s threshold for a unique walk
#' @param maxSpeed Maximum speed in m/s threshold for a unique walk
#' @rawNamespace import(data.table, except = c(first, last, between))
#' @return NA
#' @keywords internal
makeLoIsNewWalk <- function(dt, minSpeed, maxSpeed) {
    # avoid "no visible binding" warnings
    vel <- ret <- NULL

    dt[, (vel < minSpeed) | (vel > maxSpeed)]
    return(ret) # double check
}
