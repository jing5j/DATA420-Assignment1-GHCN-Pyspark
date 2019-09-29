# DATA420 Assignment 1
# Jing Wu 29696576


# start_pyspark_shell -e 4 -c 2 -w 4 -m 4




# Analysis Q4
# Imports

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()


# (a) Count the number of rows in daily

# Define schemas for daily

schema_daily = StructType([
    StructField('ID', StringType()),
    StructField('DATE', StringType()),
    StructField('ELEMENT', StringType()),
    StructField('VALUE', IntegerType()),
    StructField('MEASUREMENT_FLAG', StringType()),
    StructField('QUALITY_FLAG', StringType()),
    StructField('SOURCE_FLAG', StringType()),
    StructField('OBSERVATION_TIME', StringType()),
])


# Load all the daily file (csv format)

all_daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/*.csv.gz")
)

# Count the number of rows in daily
all_daily.count() #2624027105


# (b) Filter daily using the filter command to obtain the subset of observations 
# containing the five core elements described in inventory.
core_element_count = (
    all_daily
    .select(["ID","ELEMENT"])
    .filter(
        (all_daily.ELEMENT == "PRCP") |
        (all_daily.ELEMENT == "SNOW") |
        (all_daily.ELEMENT == "SNWD") |
        (all_daily.ELEMENT == "TMAX") |
        (all_daily.ELEMENT == "TMIN")
    )
    .groupBy("ELEMENT")
    .agg({"ID":"count"})
    .withColumnRenamed("count(ID)","ELEMENT_COUNT")
    .orderBy("ELEMENT_COUNT", ascending = False)
)

core_element_count.show()
#+-------+-------------+
#|ELEMENT|ELEMENT_COUNT|
#+-------+-------------+
#|   PRCP|    918490401|
#|   TMAX|    362528096|
#|   TMIN|    360656728|
#|   SNOW|    322003304|
#|   SNWD|    261455306|
#+-------+-------------+




# (c) Determine how many observations of TMIN do not have a corresponding 
# observation of TMAX.
ELEMENT_count = (
    all_daily
    .select(["ID","DATE","ELEMENT"])
    .filter(
        (all_daily.ELEMENT == "TMIN") |
        (all_daily.ELEMENT == "TMAX")
    )
    .groupBy("ID","DATE")
    .pivot("ELEMENT")
    .agg({"ELEMENT":"count"})
)
ELEMENT_count.cache()



# How many observations of TMIN do not have a corresponding observation of TMAX.
TMIN_without_TMAX = (
    ELEMENT_count
    .filter(
        (ELEMENT_count.TMIN.isNotNull()) &
        (ELEMENT_count.TMAX.isNull())
    )
)
TMIN_without_TMAX.cache()
TMIN_without_TMAX.count() # 7528188


# How many different stations contributed to these observations?
TMIN_stations = (
    TMIN_without_TMAX
    .select("ID")
    .dropDuplicates()
)
TMIN_stations.cache()
TMIN_stations.count() # 26625



# Do any belong to the GSN, HCN, or CRN?

# Load table stations
file_path='hdfs:///user/jwu46/outputs/ghcnd/stations.parquet'
stations = spark.read.parquet(file_path)


# Join the filtered station with GSN_FLAG and HCN_CRN_FLAG
stations_FLAG = (
    TMIN_stations
    .join(
        stations
        .select(["ID","GSN_FLAG","HCN_CRN_FLAG"]),
        on = "ID",
        how = "left"
    )
)
stations_FLAG.cache()
stations_FLAG.show(10,False)

# Filter GSN 
GSN = (
    stations_FLAG
    .filter(stations_FLAG.GSN_FLAG == "GSN")
)
GSN.count() #910

# Filter HCN
HCN = (
    stations_FLAG
    .filter(stations_FLAG.HCN_CRN_FLAG == "HCN")
)
HCN.count() #1181

# Filter CRN
CRN = (
    stations_FLAG
    .filter(stations_FLAG.HCN_CRN_FLAG == "CRN")
)
CRN.count() #23

# (d) Filter daily to obtain all observations of TMIN and TMAX for all stations 
# in New Zealand,and save the result to your output directory.


# Filter ELEMENT of TMIN and TMAX, join with table stations
TMIN_TMAX = (
    all_daily
    .filter(
        (all_daily.ELEMENT == "TMIN") |
        (all_daily.ELEMENT == "TMAX")
    )
    .join(
        stations
        .select(["ID","COUNTRY_CODE","STATION_NAME"]),
        on="ID",
        how="left"
    )
)

# Fileter all New Zealand observations and extract year from date
NZ_TMIN_TMAX = (
    TMIN_TMAX
    .filter(TMIN_TMAX.COUNTRY_CODE == "NZ")
    .withColumn("YEAR", F.trim(F.substring(F.col("DATE"),1,4)))
)
NZ_TMIN_TMAX.show(10,False)
# Save as csv file
NZ_TMIN_TMAX.write.csv('hdfs:///user/jwu46/outputs/ghcnd/NZ_TMIN_TMAX.csv',header=True,mode="overwrite")


NZ_TMIN_TMAX.count() #447017



# Get the earlies year
min_years = (
    NZ_TMIN_TMAX
    .select("YEAR")
    .agg({"YEAR":"min"})
)
min_years.show()
#+---------+
#|min(YEAR)|
#+---------+
#|     1940|
#+---------+



# Get the latest year
max_years = (
    NZ_TMIN_TMAX
    .select("YEAR")
    .agg({"YEAR":"max"})
)
max_years.show()
#+---------+
#|max(YEAR)|
#+---------+
#|     2017|
#+---------+


# (e) Group the precipitation observations by year and country.
# Compute the average rainfall in each year for each country
PRCP_avg = (
    all_daily
    .filter(all_daily.ELEMENT == "PRCP") 
    .join(
        stations
        .select(["ID","COUNTRY_NAME"]),
        on="ID",
        how="left"
    )
    .withColumn("YEAR", F.trim(F.substring(F.col("DATE"),1,4)))
    .groupBy("YEAR","COUNTRY_NAME")
    .agg({"VALUE":"avg"})
    .withColumnRenamed("avg(VALUE)","AVERAGE_RAINFALL")
    .orderBy("AVERAGE_RAINFALL",ascending = False) 
    .join(
        stations
        .select(["COUNTRY_CODE","COUNTRY_NAME"]),
        on="COUNTRY_NAME",
        how="left"
    )       
)

PRCP_avg.show(10,False)
#+------------------+----+----------------+------------+
#|COUNTRY_NAME      |YEAR|AVERAGE_RAINFALL|COUNTRY_CODE|
#+------------------+----+----------------+------------+
#|Equatorial Guinea |2000|4361.0          |EK          |
#|Equatorial Guinea |2000|4361.0          |EK          |
#|Dominican Republic|1975|3414.0          |DR          |
#|Dominican Republic|1975|3414.0          |DR          |
#|Dominican Republic|1975|3414.0          |DR          |
#|Dominican Republic|1975|3414.0          |DR          |
#|Dominican Republic|1975|3414.0          |DR          |
#|Laos              |1974|2480.5          |LA          |
#|Laos              |1974|2480.5          |LA          |
#|Laos              |1974|2480.5          |LA          |
#+------------------+----+----------------+------------+
PRCP_avg.count() # 17257469


PRCP_avg.write.csv('hdfs:///user/jwu46/outputs/ghcnd/PRCP_avg.csv',header=True,mode="overwrite")


  


