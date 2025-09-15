#include <Rcpp.h>
using namespace Rcpp;

//' Haversine function, assumes spherical earth.
//' @param lonA Numerical longitude coordinate for ping A
//' @param latA Numerical latitude coordinate for ping A
//' @param lonB Numerical longitude coordinate for ping B
//' @param latB Numerical latitude coordinate for ping B
//' @param EARTH_RADIUS_METERS Default 6378137 meters
//' @return Haversine distance individual value
//' @keywords internal
// [[Rcpp::export]]
double haverDist(double lonA, double latA, double lonB, double latB,
        int EARTH_RADIUS_METERS = 6378137) {
    lonA = lonA * M_PI/180;
    latA = latA * M_PI/180;
    lonB = lonB * M_PI/180;
    latB = latB * M_PI/180;

    double v = sin((lonA - lonB)/2);
    double u = sin((latA - latB)/2);
    double dist = 2.0 * EARTH_RADIUS_METERS * asin(sqrt(u * u + cos(latA) * cos(latB) * v * v));
    return dist;
}

//' Haversine vector function, assumes spherical earth.
//' @param lon Numerical vector of longitude coordinates for multiple pings
//' @param lat Numerical vector of latitude coordinates for multiple pings
//' @param EARTH_RADIUS_METERS Default 6378137 meters
//' @return Haversine distance vector
//' @keywords internal
// [[Rcpp::export]]
NumericVector haverDistVector(NumericVector lon, NumericVector lat,
        int EARTH_RADIUS_METERS = 6378137) {
    lon = lon * M_PI/180;
    lat = lat * M_PI/180;

    NumericVector ret(lat.size());
    ret[0] = 0;
    int n = ret.size();

    for(int i=1; i < n; i++) {
        double v = sin((lon[i] - lon[i-1])/2);
        double u = sin((lat[i] - lat[i-1])/2);
        ret[i] = 2.0 * EARTH_RADIUS_METERS * asin(sqrt(u * u + cos(lat[i]) * cos(lat[i-1]) * v * v));
    }
    return ret;
}

//' Calculate angle of a set of coordinates.
//' @param lonA Numerical longitude coordinate for ping A
//' @param latA Numerical latitude coordinate for ping A
//' @param lonB Numerical longitude coordinate for ping B
//' @param latB Numerical latitude coordinate for ping B
//' @param lonC Numerical longitude coordinate for ping C
//' @param latC Numerical latitude coordinate for ping C
//' @return angle of latitude and longitude pings
//' @keywords internal
// [[Rcpp::export]]
double findAng(double lonA, double latA, double lonB, double latB, double lonC,
        double latC) {
    double ab = haverDist(lonA, latA, lonB, latB);
    double bc = haverDist(lonB, latB, lonC, latC);
    double ac = haverDist(lonA, latA, lonC, latC);
    double ret = acos((ab*ab + bc*bc - ac*ac)/(2*ab*bc));
    return ret;
}

//' Calculate angle vector of a vector of multiple coordinates.
//' @param lon Numerical vector of longitude coordinates for multiple pings
//' @param lat Numerical vector of latitude coordinates for multiple pings
//' @param dist distance value of the lon and lat vector pairs
//' @return angle vector of latitude and longitude vectors with distance
//' @keywords internal
// [[Rcpp::export]]
NumericVector findAngVector(NumericVector lon, NumericVector lat, NumericVector dist) {
    NumericVector ret(lat.size());
    int n = ret.size();
    ret[0] = NumericVector::get_na();
    ret[n-1] =  NumericVector::get_na();

    // a is i-1, b is i and c is i+1. It computes angle at b.
    for(int i=1; i < n-1; i++) {
        double ab = dist[i];
        double bc = dist[i+1];
        double ac = haverDist(lon[i-1], lat[i-1], lon[i+1], lat[i+1]);
        ret[i] = acos((ab*ab + bc*bc - ac*ac)/(2*ab*bc));
    }
    return ret;
}

//' Find when standing still
//' @param ret Vectorized operation
//' @param accuracy Accuracy value in meters
//' @param lonDeg Numeric vector of longitude in degrees
//' @param latDeg Numeric vector of latitude in degrees
//' @param userFactor Character vector of user IDs
//' @keywords internal
// [[Rcpp::export]]
LogicalVector makeLoIsStillHelper(LogicalVector ret, NumericVector accuracy,
        NumericVector lonDeg, NumericVector latDeg, NumericVector userFactor) {

    //vectorized operations
    int n = ret.size();

    int start = 0;
    //Skip i==0 on purpose
    for(int i=1; i < n; i++) {
        if (userFactor[i] != userFactor[start]) {
            start = i;
            continue;
        }

        double dist = haverDist(latDeg[i], lonDeg[i], latDeg[start], lonDeg[start]);
        if (dist < accuracy[i] + accuracy[start]) {
            ret[i] = true;
        } else {
            start = i;
        }
    }
    return ret;
}

