# Walkingcalculatr (Python)

This repository contains the Python code converted from the walkingcalculatr package from December 2023 - August 2024. 

## Files
### Python
- [Quickstart.ipynb](Quickstart.ipynb): Python code to generate walks and walks in walkable areas for synthetic data (Ness County).
- [utils.py](utils.py): Python script containing helper functions used in `Quickstart.ipynb` and `Walk_Prev_Sidewalk_Density.ipynb`.
- [Walk_Prev_Sidewalk_Density.ipynb](Walk_Prev_Sidewalk_Density.ipynb): Python code to calculate walk prevalence and sidewalk density at the county and census-tract level.

### hpc
- [run-stratify.sh](./hpc/run-stratify.sh): Example Slurm script to run a single job in the HPC environment.
- [get-python-walks-array.sh](./hpc/get-python-walks-array.sh): Example Slurm script to run an array of jobs in the HPC environment.

### data_partitioning
- [Stratify_Data.ipynb](./data_partitioning/Stratify_Data.ipynb): Python code to run example for creating N device stratified partitions of synthetic data (Ness County).
- [stratify_functions.py](./data_partitioning/stratify_functions.py): Python script containing helper functions used in `Stratify_Data.ipynb`.

## Setup/Dependencies

To run Walkingcalculatr (Python), a python environment must be created with the required dependencies listed in **requirements.txt** (as of June 2024).

> python -m venv wc_env\
> source wc_env/bin/activate
> pip install -r requirements.txt\
**OR**\
> pip install ipykernel ipython pandas polars numpy path datetime pygris geopandas osmnx matplotlib pyarrow pyspark

## Current Issues and Implementation Differences

### Variance between R and Python OSM data

As of July 2024, a delta between the packages used for OpenStreetMap (OSM) queries in the R ([osmndata](https://cran.r-project.org/web/packages/osmdata/vignettes/osmdata.html)) and Python ([osmnx](https://osmnx.readthedocs.io/en/stable/)) implementation has been identified leading to some variance depending on the geographical area for the counts in the following: 1) walks in walkable areas and 2) sidewalk density. Alternate OSM query methods and packages are being explored in Python to address this issue.
