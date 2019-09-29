# DATA420 Assignment 1
# Jing Wu 29696576


# Processing Q2

# Imports

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

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

# Load the first 1000 rows of daily (csv format)

daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
    .limit(1000)
)
daily.cache()
daily.show(10, False)

### RESULT
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
#|ID         |DATE    |ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
#|CA1MB000296|20170101|PRCP   |0    |null            |null        |N          |null            |
#|US1NCBC0113|20170101|PRCP   |5    |null            |null        |N          |null            |
#|ASN00015643|20170101|TMAX   |274  |null            |null        |a          |null            |
#|ASN00015643|20170101|TMIN   |218  |null            |null        |a          |null            |
#|ASN00015643|20170101|PRCP   |2    |null            |null        |a          |null            |
#|US1MTMH0019|20170101|PRCP   |43   |null            |null        |N          |null            |
#|US1MTMH0019|20170101|SNOW   |28   |null            |null        |N          |null            |
#|US1MTMH0019|20170101|SNWD   |178  |null            |null        |N          |null            |
#|ASN00085296|20170101|TMAX   |217  |null            |null        |a          |null            |
#|ASN00085296|20170101|TMIN   |127  |null            |null        |a          |null            |
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+



# Define schemas for stations

schema_stations = StructType([
    StructField('ID', StringType()),
    StructField('LATITUDE', DoubleType()),
    StructField('LONGITUDE', DoubleType()),
    StructField('ELEVATION', DoubleType()),
    StructField('STATE', StringType()),
    StructField('STATION_NAME', StringType()),
    StructField('GSN_FLAG', StringType()),
    StructField('HCN_CRN_FLAG', StringType()),
    StructField('WMO_ID', StringType())
])

# Load each of stations
stations_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/stations")
)
stations_text_only.show(10,False)

stations = stations_text_only.select(
    F.trim(F.substring(F.col("value"),1,11)).alias('ID').cast(schema_stations['ID'].dataType),
    F.trim(F.substring(F.col("value"),13,8)).alias('LATITUDE').cast(schema_stations['LATITUDE'].dataType),
    F.trim(F.substring(F.col("value"),22,9)).alias('LONGITUDE').cast(schema_stations['LONGITUDE'].dataType),
    F.trim(F.substring(F.col("value"),32,6)).alias('ELEVATION').cast(schema_stations['ELEVATION'].dataType),
    F.trim(F.substring(F.col("value"),39,2)).alias('STATE').cast(schema_stations['STATE'].dataType),
    F.trim(F.substring(F.col("value"),42,30)).alias('STATION_NAME').cast(schema_stations['STATION_NAME'].dataType),
    F.trim(F.substring(F.col("value"),73,3)).alias('GSN_FLAG').cast(schema_stations['GSN_FLAG'].dataType),
    F.trim(F.substring(F.col("value"),77,3)).alias('HCN_CRN_FLAG').cast(schema_stations['HCN_CRN_FLAG'].dataType),
    F.trim(F.substring(F.col("value"),81,5)).alias('WMO_ID').cast(schema_stations['WMO_ID'].dataType)
)
stations.cache()
stations.show(10,False)

### RESULT
#+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+
#|ID         |LATIITUDE|LONGITUDE|ELEVATION|STATE|NAME                 |GSN_FLAG|HCN/CRN_FLAG|WMO_ID|
#+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+
#|ACW00011604|17.1167  |-61.7833 |10.1     |     |ST JOHNS COOLIDGE FLD|        |            |      |
#|ACW00011647|17.1333  |-61.7833 |19.2     |     |ST JOHNS             |        |            |      |
#|AE000041196|25.333   |55.517   |34.0     |     |SHARJAH INTER. AIRP  |GSN     |            |41196 |
#|AEM00041194|25.255   |55.364   |10.4     |     |DUBAI INTL           |        |            |41194 |
#|AEM00041217|24.433   |54.651   |26.8     |     |ABU DHABI INTL       |        |            |41217 |
#|AEM00041218|24.262   |55.609   |264.9    |     |AL AIN INTL          |        |            |41218 |
#|AF000040930|35.317   |69.017   |3366.0   |     |NORTH-SALANG         |GSN     |            |40930 |
#|AFM00040938|34.21    |62.228   |977.2    |     |HERAT                |        |            |40938 |
#|AFM00040948|34.566   |69.212   |1791.3   |     |KABUL INTL           |        |            |40948 |
#|AFM00040990|31.5     |65.85    |1010.0   |     |KANDAHAR AIRPORT     |        |            |40990 |
#+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+

stations.count()#103656


# Define schemas for countries

schema_countries = StructType([
    StructField('CODE', StringType()),
    StructField('NAME', StringType()),
])

# Load each of countries

countries_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/countries")
)
countries_text_only.show(10, False)

countries = countries_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(schema_countries['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(schema_countries['NAME'].dataType)
)
countries.cache()
countries.show(10, False)

### RESULT
#+----+------------------------------+
#|CODE|NAME                          |
#+----+------------------------------+
#|AC  |Antigua and Barbuda           |
#|AE  |United Arab Emirates          |
#|AF  |Afghanistan                   |
#|AG  |Algeria                       |
#|AJ  |Azerbaijan                    |
#|AL  |Albania                       |
#|AM  |Armenia                       |
#|AO  |Angola                        |
#|AQ  |American Samoa [United States]|
#|AR  |Argentina                     |
#+----+------------------------------+


countries.count()#218

# Define schemas for states

schema_states = StructType([
    StructField('CODE', StringType()),
    StructField('NAME', StringType()),
])

# Load each of states
states_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/states")
)
states_text_only.show(10,False)

states = states_text_only.select(
    F.trim(F.substring(F.col("value"),1,2)).alias('CODE').cast(schema_states['CODE'].dataType),
    F.trim(F.substring(F.col("value"),4,47)).alias('NAME').cast(schema_states['NAME'].dataType)
)
states.cache()
states.show(10,False)

###RESULT
#+----+----------------+
#|CODE|NAME            |
#+----+----------------+
#|AB  |ALBERTA         |
#|AK  |ALASKA          |
#|AL  |ALABAMA         |
#|AR  |ARKANSAS        |
#|AS  |AMERICAN SAMOA  |
#|AZ  |ARIZONA         |
#|BC  |BRITISH COLUMBIA|
#|CA  |CALIFORNIA      |
#|CO  |COLORADO        |
#|CT  |CONNECTICUT     |
#+----+----------------+


states.count()#74


# Define schemas for inventory

schema_inventory = StructType([
    StructField('ID', StringType()),
    StructField('LATITUDE', DoubleType()),
    StructField('LONGITUDE', DoubleType()),
    StructField('ELEMENT', StringType()),
    StructField('FIRSTYEAR', IntegerType()),
    StructField('LASTYEAR', IntegerType()),
])

# Load each of inventory
inventory_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/inventory")
)
inventory_text_only.show(10,False)

inventory = inventory_text_only.select(
    F.trim(F.substring(F.col("value"),1,11)).alias('ID').cast(schema_inventory['ID'].dataType),
    F.trim(F.substring(F.col("value"),13,8)).alias('LATITUDE').cast(schema_inventory['LATITUDE'].dataType),
    F.trim(F.substring(F.col("value"),22,9)).alias('LONGITUDE').cast(schema_inventory['LONGITUDE'].dataType),
    F.trim(F.substring(F.col("value"),32,4)).alias('ELEMENT').cast(schema_inventory['ELEMENT'].dataType),
    F.trim(F.substring(F.col("value"),37,4)).alias('FIRSTYEAR').cast(schema_inventory['FIRSTYEAR'].dataType),
    F.trim(F.substring(F.col("value"),42,4)).alias('LASTYEAR').cast(schema_inventory['LASTYEAR'].dataType)
)
inventory.cache()
inventory.show(10,False)

###RESULT
#+-----------+--------+---------+-------+---------+--------+
#|ID         |LATITUDE|LONGITUDE|ELEMENT|FIRSTYEAR|LASTYEAR|
#+-----------+--------+---------+-------+---------+--------+
#|ACW00011604|17.1167 |-61.7833 |TMAX   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |TMIN   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |PRCP   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |SNOW   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |SNWD   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |PGTM   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |WDFG   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |WSFG   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |WT03   |1949     |1949    |
#|ACW00011604|17.1167 |-61.7833 |WT08   |1949     |1949    |
#+-----------+--------+---------+-------+---------+--------+


inventory.count()#595699


#How many stations do not have a WMO ID?
stations.filter(stations.WMO_ID == "").count()#95595







# Processing Q3

# (a) Extract country code
stations = (
    stations
    .withColumn("COUNTRY_CODE", stations.ID[0:2])
)
stations.show(10,False)
#RESULT
#+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+------------+
#|ID         |LATITUDE|LONGITUDE|ELEVATION|STATE|STATION_NAME         |GSN_FLAG|HCN_CRN_FLAG|WMO_ID|COUNTRY_CODE|
#+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+------------+
#|ACW00011604|17.1167 |-61.7833 |10.1     |     |ST JOHNS COOLIDGE FLD|        |            |      |AC          |
#|ACW00011647|17.1333 |-61.7833 |19.2     |     |ST JOHNS             |        |            |      |AC          |
#|AE000041196|25.333  |55.517   |34.0     |     |SHARJAH INTER. AIRP  |GSN     |            |41196 |AE          |
#|AEM00041194|25.255  |55.364   |10.4     |     |DUBAI INTL           |        |            |41194 |AE          |
#|AEM00041217|24.433  |54.651   |26.8     |     |ABU DHABI INTL       |        |            |41217 |AE          |
#|AEM00041218|24.262  |55.609   |264.9    |     |AL AIN INTL          |        |            |41218 |AE          |
#|AF000040930|35.317  |69.017   |3366.0   |     |NORTH-SALANG         |GSN     |            |40930 |AF          |
#|AFM00040938|34.21   |62.228   |977.2    |     |HERAT                |        |            |40938 |AF          |
#|AFM00040948|34.566  |69.212   |1791.3   |     |KABUL INTL           |        |            |40948 |AF          |
#|AFM00040990|31.5    |65.85    |1010.0   |     |KANDAHAR AIRPORT     |        |            |40990 |AF          |
#+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+------------+



# (b) Join table countries
stations = (
    stations
    .join(
        countries
        .select(
            F.col("CODE").alias("COUNTRY_CODE"),
            F.col("NAME").alias("COUNTRY_NAME")
        ),
        on="COUNTRY_CODE",
        how="left"
    )
)
stations.show(10,False)
###RESULT
#+------------+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+--------------------+
#|COUNTRY_CODE|ID         |LATITUDE|LONGITUDE|ELEVATION|STATE|STATION_NAME         |GSN_FLAG|HCN_CRN_FLAG|WMO_ID|COUNTRY_NAME        |
#+------------+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+--------------------+
#|AC          |ACW00011604|17.1167 |-61.7833 |10.1     |     |ST JOHNS COOLIDGE FLD|        |            |      |Antigua and Barbuda |
#|AC          |ACW00011647|17.1333 |-61.7833 |19.2     |     |ST JOHNS             |        |            |      |Antigua and Barbuda |
#|AE          |AE000041196|25.333  |55.517   |34.0     |     |SHARJAH INTER. AIRP  |GSN     |            |41196 |United Arab Emirates|
#|AE          |AEM00041194|25.255  |55.364   |10.4     |     |DUBAI INTL           |        |            |41194 |United Arab Emirates|
#|AE          |AEM00041217|24.433  |54.651   |26.8     |     |ABU DHABI INTL       |        |            |41217 |United Arab Emirates|
#|AE          |AEM00041218|24.262  |55.609   |264.9    |     |AL AIN INTL          |        |            |41218 |United Arab Emirates|
#|AF          |AF000040930|35.317  |69.017   |3366.0   |     |NORTH-SALANG         |GSN     |            |40930 |Afghanistan         |
#|AF          |AFM00040938|34.21   |62.228   |977.2    |     |HERAT                |        |            |40938 |Afghanistan         |
#|AF          |AFM00040948|34.566  |69.212   |1791.3   |     |KABUL INTL           |        |            |40948 |Afghanistan         |
#|AF          |AFM00040990|31.5    |65.85    |1010.0   |     |KANDAHAR AIRPORT     |        |            |40990 |Afghanistan         |
#+------------+-----------+--------+---------+---------+-----+---------------------+--------+------------+------+--------------------+


# (c) Join table states
stations = (
    stations
    .join(
        states
        .select(
            F.col("CODE").alias("STATE"),
            F.col("NAME").alias("STATE_NAME")
        ),
        on="STATE",
        how="left"
    )
    .fillna({"STATE_NAME":""})
)
stations.show(5,False)
###RESULT
#+-----+------------+-----------+--------+---------+---------+---------------------+--------+------------+------+--------------------+----------+
#|STATE|COUNTRY_CODE|ID         |LATITUDE|LONGITUDE|ELEVATION|STATION_NAME         |GSN_FLAG|HCN_CRN_FLAG|WMO_ID|COUNTRY_NAME        |STATE_NAME|
#+-----+------------+-----------+--------+---------+---------+---------------------+--------+------------+------+--------------------+----------+
#|     |AC          |ACW00011604|17.1167 |-61.7833 |10.1     |ST JOHNS COOLIDGE FLD|        |            |      |Antigua and Barbuda |          |
#|     |AC          |ACW00011647|17.1333 |-61.7833 |19.2     |ST JOHNS             |        |            |      |Antigua and Barbuda |          |
#|     |AE          |AE000041196|25.333  |55.517   |34.0     |SHARJAH INTER. AIRP  |GSN     |            |41196 |United Arab Emirates|          |
#|     |AE          |AEM00041194|25.255  |55.364   |10.4     |DUBAI INTL           |        |            |41194 |United Arab Emirates|          |
#|     |AE          |AEM00041217|24.433  |54.651   |26.8     |ABU DHABI INTL       |        |            |41217 |United Arab Emirates|          |
#+-----+------------+-----------+--------+---------+---------+---------------------+--------+------------+------+--------------------+----------+


# (d) Basic analysis on inventory

# What was the first and last year that each station was active and
# collected any element at all?
# How many different elements has each station collected overall?
inventory_statistics = (
    inventory
    .select(["ID","ELEMENT","FIRSTYEAR","LASTYEAR"])
    .groupBy("ID")
    .agg({"FIRSTYEAR":"min","LASTYEAR":"max","ELEMENT":"count"})
    .select(
        F.col("ID"),
        F.col("min(FIRSTYEAR)").alias("FIRSTYEAR"),
        F.col("max(LASTYEAR)").alias("LASTYEAR"),
        F.col("count(ELEMENT)").alias("ELEMENT_COUNT"))
)
inventory_statistics.show(10,False)
###RESULT
#+-----------+---------+--------+-------------+
#|ID         |FIRSTYEAR|LASTYEAR|ELEMENT_COUNT|
#+-----------+---------+--------+-------------+
#|AQC00914021|1955     |1957    |10           |
#|AR000000002|1981     |2000    |1            |
#|ASN00003081|1990     |1995    |1            |
#|ASN00004035|1887     |2015    |10           |
#|ASN00007118|1898     |1913    |1            |
#|ASN00008194|1908     |1919    |4            |
#|ASN00008254|1931     |2017    |4            |
#|ASN00009084|1913     |1919    |1            |
#|ASN00009810|1970     |1999    |4            |
#|ASN00010306|1994     |2008    |4            |
#+-----------+---------+--------+-------------+


# Count separately the number of core elements and the number of 
# "other" elements that each station has collected overall.

# Count the core elements
core_element_count = (
    inventory
    .select(["ID","ELEMENT"])
    .where(
        (inventory.ELEMENT == "PRCP") |
        (inventory.ELEMENT == "SNOW") |
        (inventory.ELEMENT == "SNWD") |
        (inventory.ELEMENT == "TMAX") |
        (inventory.ELEMENT == "TMIN")
    )
    .groupBy("ID")
    .pivot("ELEMENT")
    .agg({"ELEMENT":"count"})
)


# Count for other elements
other_element_count= (
    inventory
    .select(["ID","ELEMENT"])
    .where(
        (inventory.ELEMENT != "PRCP") &
        (inventory.ELEMENT != "SNOW") &
        (inventory.ELEMENT != "SNWD") &
        (inventory.ELEMENT != "TMAX") &
        (inventory.ELEMENT != "TMIN")
    )
    .groupBy("ID")
    .agg({"ELEMENT":"count"})
    .select(
        F.col("ID"),
        F.col("count(ELEMENT)").alias("OTHER"))
)


# Join the inventory_statistics, core_element_count and other_element_count
inventory_statistics = (
    inventory_statistics
    .join(
        core_element_count,
        on="ID",
        how="left" 
    )
    .join(
        other_element_count,
        on="ID",
        how="left"
    )    
    .fillna(0)
)

inventory_statistics.show(10,False)
###RESULT
#+-----------+---------+--------+-------------+----+----+----+----+----+-----+
#|ID         |FIRSTYEAR|LASTYEAR|ELEMENT_COUNT|PRCP|SNOW|SNWD|TMAX|TMIN|OTHER|
#+-----------+---------+--------+-------------+----+----+----+----+----+-----+
#|AQC00914021|1955     |1957    |10           |1   |1   |1   |1   |1   |5    |
#|AR000000002|1981     |2000    |1            |1   |0   |0   |0   |0   |0    |
#|ASN00003081|1990     |1995    |1            |1   |0   |0   |0   |0   |0    |
#|ASN00004035|1887     |2015    |10           |1   |0   |0   |1   |1   |7    |
#|ASN00007118|1898     |1913    |1            |1   |0   |0   |0   |0   |0    |
#|ASN00008194|1908     |1919    |4            |1   |0   |0   |0   |0   |3    |
#|ASN00008254|1931     |2017    |4            |1   |0   |0   |0   |0   |3    |
#|ASN00009084|1913     |1919    |1            |1   |0   |0   |0   |0   |0    |
#|ASN00009810|1970     |1999    |4            |1   |0   |0   |0   |0   |3    |
#|ASN00010306|1994     |2008    |4            |1   |0   |0   |0   |0   |3    |
#+-----------+---------+--------+-------------+----+----+----+----+----+-----+


inventory_statistics.count()

# How many stations collect all five core elements?
five_core_element = (
    inventory_statistics
    .select(["ID", "PRCP", "SNOW", "SNWD", "TMAX", "TMIN"])
    .where(
        (inventory_statistics.PRCP != 0) &
        (inventory_statistics.SNOW != 0) &
        (inventory_statistics.SNWD != 0) &
        (inventory_statistics.TMAX != 0) &
        (inventory_statistics.TMIN != 0) 
    )
)
five_core_element.count()#103630



# How many only collection precipitation?
PRCP_element = (
    inventory_statistics
    .where(
        (inventory_statistics.PRCP != 0) &
        (inventory_statistics.ELEMENT_COUNT == inventory_statistics.PRCP)
    )
)
PRCP_element.count()#15970

# Count five core element
five_element_count = (
    inventory
    .select(["ID","ELEMENT"])
    .filter(
        (inventory.ELEMENT == "PRCP") |
        (inventory.ELEMENT == "SNOW") |
        (inventory.ELEMENT == "SNWD") |
        (inventory.ELEMENT == "TMAX") |
        (inventory.ELEMENT == "TMIN")
    )
    .groupBy("ELEMENT")
    .agg({"ID":"count"})
    .withColumnRenamed("count(ID)","ELEMENT_COUNT")
    .orderBy("ELEMENT_COUNT", ascending = False)
)
five_element_count.show()
#+-------+-------------+
#|ELEMENT|ELEMENT_COUNT|
#+-------+-------------+
#|   PRCP|       101614|
#|   SNOW|        61756|
#|   SNWD|        53307|
#|   TMAX|        34109|
#|   TMIN|        34012|
#+-------+-------------+



# (e) JOIN stations and inventory_statistics
stations = (
    stations
    .join(
        inventory_statistics,
        on="ID",
        how="left"
    )
)
stations.show(5,False)
###RESULT
#+-----------+-----+------------+--------+---------+---------+--------------+--------+------------+------+------------------------------+--------------+---------+--------+-------------+----+----+----+----+----+-----+
#|ID         |STATE|COUNTRY_CODE|LATITUDE|LONGITUDE|ELEVATION|STATION_NAME  |GSN_FLAG|HCN_CRN_FLAG|WMO_ID|COUNTRY_NAME                  |STATE_NAME    |FIRSTYEAR|LASTYEAR|ELEMENT_COUNT|PRCP|SNOW|SNWD|TMAX|TMIN|OTHER|
#+-----------+-----+------------+--------+---------+---------+--------------+--------+------------+------+------------------------------+--------------+---------+--------+-------------+----+----+----+----+----+-----+
#|AQC00914021|AS   |AQ          |-14.2667|-170.5833|6.1      |AMOULI TUTUILA|        |            |      |American Samoa [United States]|AMERICAN SAMOA|1955     |1957    |10           |1   |1   |1   |1   |1   |5    |
#|AR000000002|     |AR          |-29.82  |-57.42   |75.0     |BONPLAND      |        |            |      |Argentina                     |              |1981     |2000    |1            |1   |0   |0   |0   |0   |0    |
#|ASN00003081|     |AS          |-17.5778|123.8222 |91.0     |CURTIN        |        |            |      |Australia                     |              |1990     |1995    |1            |1   |0   |0   |0   |0   |0    |
#|ASN00004035|     |AS          |-20.7767|117.1456 |12.0     |ROEBOURNE     |        |            |94309 |Australia                     |              |1887     |2015    |10           |1   |0   |0   |1   |1   |7    |
#|ASN00007118|     |AS          |-26.4   |118.4    |-999.9   |ABBOTTS       |        |            |      |Australia                     |              |1898     |1913    |1            |1   |0   |0   |0   |0   |0    |
#+-----------+-----+------------+--------+---------+---------+--------------+--------+------------+------+------------------------------+--------------+---------+--------+-------------+----+----+----+----+----+-----+


# Save the joint table to hdfs folder
stations.write.parquet('hdfs:///user/jwu46/outputs/ghcnd/stations.parquet',mode="overwrite")



# (f) JOIN daily_2017 and stations
daily = (
    daily
    .join(
        stations,
        on="ID",
        how="left"
    )
)

daily.show(5,False)
###RESULT
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----+------------+--------+---------+---------+-----------------+--------+------------+------+-------------+----------+---------+--------+-------------+----+----+----+----+----+-----+
#|ID         |DATE    |ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|STATE|COUNTRY_CODE|LATITUDE|LONGITUDE|ELEVATION|STATION_NAME     |GSN_FLAG|HCN_CRN_FLAG|WMO_ID|COUNTRY_NAME |STATE_NAME|FIRSTYEAR|LASTYEAR|ELEMENT_COUNT|PRCP|SNOW|SNWD|TMAX|TMIN|OTHER|
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----+------------+--------+---------+---------+-----------------+--------+------------+------+-------------+----------+---------+--------+-------------+----+----+----+----+----+-----+
#|ASN00015005|20170101|PRCP   |24   |null            |null        |a          |null            |     |AS          |-20.0298|137.4906 |205.0    |AVON DOWNS       |        |            |      |Australia    |          |1909     |2017    |6            |1   |0   |0   |1   |1   |3    |
#|FIE00145157|20170101|TMAX   |36   |null            |null        |E          |null            |     |FI          |63.8442 |23.1281  |6.0      |KOKKOLA HOLLIHAKA|        |            |      |Finland      |          |2008     |2017    |3            |1   |0   |0   |1   |1   |0    |
#|FIE00145157|20170101|TMIN   |-58  |null            |null        |E          |null            |     |FI          |63.8442 |23.1281  |6.0      |KOKKOLA HOLLIHAKA|        |            |      |Finland      |          |2008     |2017    |3            |1   |0   |0   |1   |1   |0    |
#|FIE00145157|20170101|PRCP   |23   |null            |null        |E          |null            |     |FI          |63.8442 |23.1281  |6.0      |KOKKOLA HOLLIHAKA|        |            |      |Finland      |          |2008     |2017    |3            |1   |0   |0   |1   |1   |0    |
#|US1ARGN0006|20170101|PRCP   |25   |null            |null        |N          |null            |AR   |US          |36.2086 |-90.5347 |102.7    |LAFE 1.8 W       |        |            |      |United States|ARKANSAS  |1998     |2017    |5            |1   |1   |1   |0   |0   |2    |
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----+------------+--------+---------+---------+-----------------+--------+------------+------+-------------+----------+---------+--------+-------------+----+----+----+----+----+-----+



# Are there any stations in your subset of daily that are not in stations at all?
not_in_station_table = (
    daily
    .select(["ID","STATION_NAME"])
    .where(daily.STATION_NAME.isNull())
)
not_in_station_table.count()#0

# Could you determine if there are any stations in daily that are not in stations 
# without using LEFT JOIN?
not_in_stations = (
    daily
    .select("ID")
    .subtract(stations.select("ID")) 
)
not_in_stations.count()#0



# Sace as csv file to check the file size
stations.write.csv('hdfs:///user/jwu46/outputs/ghcnd/stations.csv',mode="overwrite")