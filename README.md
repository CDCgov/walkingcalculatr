# walkingcalculatr

R package for extracting walks and calculating walk metrics from Location Based Services (LBS) data. This package implements the [Hunter et al. algorithm](https://www.nature.com/articles/s41467-021-23937-9) as explained within the paper, which extracts walks from mobility data.

> Hunter, Ruth F and Garcia, Leandro and de Sa, Thiago Herick and Zapata-Diomedi, 
> Belen and Millett, Christopher and Woodcock, James and 
> Pentland, Alex ’Sandy’ and Moro, Esteban, Effect of COVID-19 response policies on walking behavior in US cities, Nature communications, Volume 12, Issue 1, 2021,
> Pages 1–9, https://www.nature.com/articles/s41467-021-23937-9

This package also includes examples on how to create visualizations from walking metrics in R and includes example SparkR code for data partitioning (optional).

## Installation

To install a version of `walkingcalculatr`:

```{r, eval = FALSE}
# using the remotes R package
remotes::install_github("https://github.com/cdcgov/walkingcalculatr")
```

Several common R packages should be automatically installed by installing the package; however platform specific (MacOS, Windows, Linux) installation steps may be required to install additional dependencies outlined in [Installation.Rmd](./vignettes/Installation.Rmd). A version of Spark is only required if the data needs to be partitioned due to memory limitations. Currently `walkingcalculatr` functions have been tested for partitions < 200 million rows using a 64 Gb Linux machine. 

## Summary

The `walkingcalculatr` package extracts walking metrics (such as number of walks, average walk duration etc.)  from location based services (LBS) data. The walk extraction is based on techniques which use rule based algorithms derived from spatial metrics such as distance, speed and angle and leverages point-of-interest (POI) data sources (Open Street Map (OSM)) to limit results to walkable areas. Results from `walkingcalculatr` include a table with relevant walk metrics and a subset of records considered to be apart of reported walks. More information on the walk extraction methodology leveraged from [Hunter et al. algorithm](https://www.nature.com/articles/s41467-021-23937-9) can be found at the link.

To start running `walkingcalculatr`, an R installation with a variety of additional packages is required, as is an LBS dataset prepared as outlined in [Quickstart](./vignettes/Quickstart.Rmd).

The rest of this documentation includes:

### Getting started
- [Installation](./vignettes/Installation.Rmd): options for installing `walkingcalculatr` and Spark
- [Quickstart](./vignettes/Quickstart.Rmd): a brief tour of using `walkingcalculatr`

### Advanced topics:
- [Configuration](./vignettes/Configuration.Rmd): examples of generating user defined walkable areas in OSM and configuring different parameters to calculate AM-PM peaks and utilitarian walks.
- [Spark Install & Partitioning large data using Spark](./vignettes/Spark_Overview.Rmd): instructions for installing Spark, initializing a Spark session and partitioning a data set. Should be used if there is in-sufficient memory available to run `walkingcalculatr` functions on raw data. The `walkingcalculatr` functions have been verified to work on LBS data  (< 200 million rows) given a  64 GB Linux machine.
- [Visualizing walking prevalence and sidewalk density](./vignettes/Walk_Prev_Sidewalk_Density.Rmd): example of creating county level visuals for walking prevalence and sidewalk density
- [Troubleshooting/Common Issues](./vignettes/Troubleshooting.Rmd): outlining possible fixes for previously observed common issues
- [Understanding walkingcalculatr output](./vignettes/Output.Rmd): in-depth discussion on output metrics
- [Next Steps](./vignettes/Next_Steps.Rmd): notes on potential enhancements to walking extraction (i.e., Python conversion) and additional walking metrics
