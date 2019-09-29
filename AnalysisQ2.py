# DATA420 Assignment 1
# Jing Wu 29696576


# Analysis Q2

from pyspark.sql import SparkSession, functions as F

# Load the stations parquet file in hdfs
file_path='hdfs:///user/jwu46/outputs/ghcnd/stations.parquet'
stations = spark.read.parquet(file_path)


# Filter NZ stations
NZ_stations = (
	stations
	.where(stations.COUNTRY_CODE == "NZ")
	.select(["ID","STATION_NAME","LATITUDE","LONGITUDE"])
)
NZ_stations.show(10,False)
###RESULT
#+-----------+-------------------+--------+---------+
#|ID         |STATION_NAME       |LATITUDE|LONGITUDE|
#+-----------+-------------------+--------+---------+
#|NZ000939450|CAMPBELL ISLAND AWS|-52.55  |169.167  |
#|NZ000933090|NEW PLYMOUTH AWS   |-39.017 |174.183  |
#|NZM00093678|KAIKOURA           |-42.417 |173.7    |
#|NZ000093417|PARAPARAUMU AWS    |-40.9   |174.983  |
#|NZ000093292|GISBORNE AERODROME |-38.65  |177.983  |
#|NZM00093929|ENDERBY ISLAND AWS |-50.483 |166.3    |
#|NZM00093439|WELLINGTON AERO AWS|-41.333 |174.8    |
#|NZ000093012|KAITAIA            |-35.1   |173.267  |
#|NZ000093994|RAOUL ISL/KERMADEC |-29.25  |-177.917 |
#|NZM00093781|CHRISTCHURCH INTL  |-43.489 |172.532  |
#+-----------+-------------------+--------+---------+


# Define the function for computing the distance between two points
from math import sin, cos, sqrt, atan2, radians
def compute_distance(long_1, lat_1, long_2, lat_2):
	long_1, lat_1, long_2, lat_2 = map(radians, [long_1, lat_1, long_2, lat_2])
	
	dist_long = long_2 - long_1
	dist_lat = lat_2 - lat_1
	
	a = sin(dist_lat/2)**2 + cos(lat_1) * cos(lat_2) * sin(dist_long/2)**2
	central_angle = 2 * atan2(sqrt(a), sqrt(1 - a))

	r = 6371
	distance = central_angle * r
	return round(distance, 2)


udf_distance = F.udf(compute_distance)

twin_station_NZ = (
	NZ_stations
	.crossJoin(NZ_stations)
	.toDF("ID_1", "STATION_NAME_1", "LATITUDE_1", "LONGITUDE_1","ID_2", "STATION_NAME_2", "LATITUDE_2", "LONGITUDE_2")
)

twin_station_NZ = (
	twin_station_NZ
	.where(twin_station_NZ.ID_1 > twin_station_NZ.ID_2)
)


station_distance_NZ = (
	twin_station_NZ
	.withColumn("DISTANCE", udf_distance(
		twin_station_NZ.LONGITUDE_1, twin_station_NZ.LATITUDE_1,
		twin_station_NZ.LONGITUDE_2, twin_station_NZ.LATITUDE_2)
	)
	
)

station_distance_NZ = (
	station_distance_NZ
	.withColumn("DISTANCE",station_distance_NZ.DISTANCE.cast(DoubleType()))
)
station_distance_NZ.show(5,False) 
###RESULT
#+-----------+-------------------+----------+-----------+-----------+------------------+----------+-----------+--------+
#|ID_1       |STATION_NAME_1     |LATITUDE_1|LONGITUDE_1|ID_2       |STATION_NAME_2    |LATITUDE_2|LONGITUDE_2|DISTANCE|
#+-----------+-------------------+----------+-----------+-----------+------------------+----------+-----------+--------+
#|NZ000939450|CAMPBELL ISLAND AWS|-52.55    |169.167    |NZ000933090|NEW PLYMOUTH AWS  |-39.017   |174.183    |1553.29 |
#|NZ000939450|CAMPBELL ISLAND AWS|-52.55    |169.167    |NZ000093417|PARAPARAUMU AWS   |-40.9     |174.983    |1368.06 |
#|NZ000939450|CAMPBELL ISLAND AWS|-52.55    |169.167    |NZ000093292|GISBORNE AERODROME|-38.65    |177.983    |1687.99 |
#|NZ000939450|CAMPBELL ISLAND AWS|-52.55    |169.167    |NZ000093012|KAITAIA           |-35.1     |173.267    |1967.22 |
#|NZ000939450|CAMPBELL ISLAND AWS|-52.55    |169.167    |NZ000093994|RAOUL ISL/KERMADEC|-29.25    |-177.917   |2799.18 |
#+-----------+-------------------+----------+-----------+-----------+------------------+----------+-----------+--------+


# Save the distance table to hdfs
station_distance_NZ.write.parquet('hdfs:///user/jwu46/outputs/ghcnd/station_distance_NZ.parquet',mode="overwrite")


# What two stations are the geographically closest in New Zealand?
min_dist = station_distance_NZ.sort("DISTANCE",ascending = True)
min_dist.show(1,False)
###RESULT
#+-----------+-------------------+----------+-----------+-----------+---------------+----------+-----------+--------+
#|ID_1       |STATION_NAME_1     |LATITUDE_1|LONGITUDE_1|ID_2       |STATION_NAME_2 |LATITUDE_2|LONGITUDE_2|DISTANCE|
#+-----------+-------------------+----------+-----------+-----------+---------------+----------+-----------+--------+
#|NZM00093439|WELLINGTON AERO AWS|-41.333   |174.8      |NZ000093417|PARAPARAUMU AWS|-40.9     |174.983    |50.53   |
#+-----------+-------------------+----------+-----------+-----------+---------------+----------+-----------+--------+
