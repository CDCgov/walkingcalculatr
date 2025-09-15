## Python script to create equally sized partitions
from pathlib import Path

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, window, Column

import glob
import os
import pandas as pd
import time

def create_stratified_sizes(df, max_size: int = 1E9):
    '''
    Create device partitions given a max size rows (max_size). 
    Default limit is 1.0B records.
    '''
    totList = []
    totCount = 0
    devCount = 0
    
    i = 0 # start index
    j = df.shape[0] - 1 # end index
    p = 1 # partition count
    
    ids = df['ID'].to_list()
    sizes = df['NumPings'].to_list()

    # Keep running until all ids have been considered
    while i < j:
        
        currList = []
        curr_size = 0
    
        # Add device to list until max size is reached
        while True and i < j:
            
            # Add larger device, increase index by 1
            curr_size += sizes[i]
            
            if curr_size < max_size:
                currList.append(ids[i])
                i+=1
            else: 
                curr_size -= sizes[i]
                break
            
            # Add smaller device, decrease index by 1
            curr_size += sizes[j]
    
            if curr_size < max_size:
                currList.append(ids[j])
                j-=1
            else: 
                curr_size -= sizes[j]
                break

        # Save to overall list
        # print(f'**** Partition #{p} ****')
        num_device = len(currList)
        # print('Number of devices: ', num_device)
        # print('Number of records: ', curr_size)
        totList.append(currList)
        p+=1
        totCount+=curr_size
        devCount+=num_device

    # print('******'*5)
    # print('Final row count:', totCount)
    # print('Final device count:', devCount)
    return totList
    
def create_partitions_from_parquet(sqlc,
                                   data_path = Path('~/Gitlab/walkingcalculatr/data-raw/synthetic_ness_data.csv.gz'),
                                   strat_path = Path('~/Gitlab/python-and-validation/data_partitioning/partitions'),
                                   county: str = 'ness',
                                   part_size: int = 200E6):

    print("Running stratification for", county)

    # Read in parquet w/o defined schema
    try: 

        df = sqlc.read.parquet(str(data_path))
    except: 

        df = sqlc.read.csv(str(data_path))
        df = df.withColumnRenamed("_c0","ID")\
                .withColumnRenamed("_c1","LATITUDE")\
                .withColumnRenamed("_c2","LONGITUDE")\
                .withColumnRenamed("_c3","TIMESTAMP")\
                .withColumnRenamed("_c4","HORIZONTAL_ACCURACY")\
                .withColumnRenamed("_c5","VERTICAL_ACCURACY")\
                .withColumnRenamed("_c6","SPEED")\
                .withColumnRenamed("_c7","GEOHASH")

    # print(df.printSchema())
    df.createOrReplaceTempView('df')

    ## Clean data ##
    # Lower case ids and clean ids
    try:
        clean_df = sqlc.sql("""
        SELECT TRIM(LOWER(ID)) as ID, 
        LATITUDE, LONGITUDE, TIMESTAMP, 
        HORIZONTAL_ACCURACY,
        VERTICAL_ACCURACY, SPEED, GEOHASH, LOCAL_TIMESTAMP FROM df
        """)

        # Reduced version/columns
    except:
        clean_df = sqlc.sql("""
        SELECT TRIM(LOWER(ID)) as ID, 
        LATITUDE, LONGITUDE, TIMESTAMP, 
        HORIZONTAL_ACCURACY FROM df
        """)

    clean_df.createOrReplaceTempView("clean_df")

    # Create partition number column
    clean_df = clean_df.withColumn("partition_number", lit(0))
    lbs_schema = clean_df.schema
    # print(lbs_schema)

    # Get counts by device if it does not exist
    filePath = strat_path / f'{county}_device_by_num_pings.csv'
    
    if not os.path.isfile(filePath) :

        # Get row counts by id 
        device_counts = sqlc.sql("""
        SELECT ID, COUNT(*) as NumPings FROM clean_df
        GROUP BY ID SORT BY NumPings
        """).toPandas()

        device_counts_df = pd.DataFrame(device_counts).rename(columns = {0: 'ID', 1: 'NumPings'})
        device_counts_df.to_csv(strat_path / f'{county}_device_by_num_pings.csv', index = False)
    
    # Read in row counts by id
    device_counts_df = pd.read_csv(strat_path / f'{county}_device_by_num_pings.csv')

    # Create equal sized - device stratified list for a given max size
    device_eq_lst = create_stratified_sizes(device_counts_df,  max_size = part_size)

    # Update records with partition numbers
    for i, lst in enumerate(device_eq_lst):
        clean_df = clean_df.withColumn("partition_number", 
        when(col("ID").isin(lst), lit(str(i)))\
        .otherwise(col("partition_number")))

    # Confirm valid number of pings by partition
    print('Number of partitions: ', clean_df.select('partition_number').distinct().count())
    print('Number of rows by partition:' , clean_df.groupby('partition_number').count().show())

    return clean_df

