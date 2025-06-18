# walkingcalculatr

## Overview

The walkingcalculatr package extracts walking metrics (such as number of walks, average walk duration etc.) from cellphone-based mobility data. No such data is provided in this repo. This repo only contains code to analyze data provided by users. The walk extraction is based on techniques which use rule-based algorithms derived from spatial metrics such as distance, speed and angle, and leverages point-of-interest (POI) data sources (Open Street Map (OSM)) to limit results to walkable areas. More information on the walk extraction methodology leveraged from Hunter et al. algorithm can be found here: https://github.com/emoro/extract_walks_mobility. 

This package also includes examples on how to create visualizations from walking metrics in R and includes example SparkR code for data partitioning. Please note that users must supply their own data. **Walkingcalculatr does not include any mobility data, only code to analyze it.
**

## Installation

To install a version of `walkingcalculatr`:

```{r, eval = FALSE}
# using the remotes R package
remotes::install_github("https://github.com/cdcgov/walkingcalculatr")
```

Several common R packages should be automatically installed by installing the package; however platform specific (MacOS, Windows, Linux) installation steps may be required to install additional dependencies outlined in [Installation.Rmd](./vignettes/Installation.Rmd). A version of Spark is only required if the data needs to be partitioned due to memory limitations. Currently `walkingcalculatr` functions have been tested for partitions < 200 million rows using a 64 Gb Linux machine. 

## Summary

The `walkingcalculatr` package extracts walking metrics (such as number of walks, average walk duration etc.)  from location based services (LBS) data. The walk extraction is based on techniques which use rule based algorithms derived from spatial metrics such as distance, speed and angle and leverages point-of-interest (POI) data sources (Open Street Map (OSM)) to limit results to walkable areas. Results from `walkingcalculatr` include a table with relevant walk metrics and a subset of records considered to be apart of reported walks. More information on the walk extraction methodology leveraged from [Hunter et al. algorithm](https://www.nature.com/articles/s41467-021-23937-9):

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

  ## Public Domain Standard Notice

  This repository constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105. This repository is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication. All contributions to this repository will be released under the CC0 dedication. By submitting a pull request you are agreeing to comply with this waiver of copyright interest.

  ## License Standard Notice
This work contains source code created by Dr. Esteban Moro, available at https://github.com/emoro/extract_walks_mobility. For information about rights and permissions please see the disclosure below. 

Copyright 2025 emoro

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and information. All material and community participation is covered by the Disclaimer and Code of Conduct. For more information about CDC's privacy policy, please visit http://www.cdc.gov/other/privacy.html.

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by forking and submitting a pull request. (If you are new to GitHub, you might start with a basic tutorial.) 
All comments, messages, pull requests, and other submissions received through CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at http://www.cdc.gov/other/privacy.html.

## Records Management Standard Notice
This repository is not a source of government records, but is a copy to increase collaboration and collaborative potential. Government records are sometimes available through the CDC web site or can otherwise be made available pursuant to a Freedom of Information Act (5 U.S.C. § 552) request. 

## Additional Standard Notices
Please refer to CDC's Template Repository for more information about contributing to this repository, public domain notices and disclaimers, and code of conduct.
