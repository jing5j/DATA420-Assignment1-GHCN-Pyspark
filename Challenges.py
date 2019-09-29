# DATA420 Assignment 1
# Jing Wu 29696576


# start_pyspark_shell -e 4 -c 2 -w 4 -m 4

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


# Load all the daily file (csv format)

all_daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/*.csv.gz")
)



# Challenges Q1
# Investigate the coverage of the other elements
# Both temporally and geographically.


# Temporally investigate the coverage of the other element
 
other_elements = (
    all_daily
    .select(["DATE","ELEMENT"])
    .filter(
        (all_daily.ELEMENT != "PRCP") &
        (all_daily.ELEMENT != "SNOW") &
        (all_daily.ELEMENT != "SNWD") &
        (all_daily.ELEMENT != "TMAX") &
        (all_daily.ELEMENT != "TMIN")
    )
    .withColumn("YEAR", F.trim(F.substring(F.col("DATE"),1,4)))  
)

# Get the start record year for each element
other_elements_year_min = (
    other_elements
    .groupBy("ELEMENT")
    .agg({"YEAR":"min"})   
)


# Get the end record year for each element
other_elements_year_max = (
    other_elements
    .groupBy("ELEMENT")
    .agg({"YEAR":"max"})   
)


# Define a function to compute the years each element was recored
def covered_year(maxyear,minyear):
    return int(maxyear)-int(minyear) + 1

yearcount = F.udf(covered_year)

# Join the start year and end year and compute the covered years
other_elements_year = (
    other_elements_year_min
    .join(
        other_elements_year_max,
        on="ELEMENT",
        how="left"
    )
    .withColumn("COVEREDYEAR",yearcount(F.col("max(YEAR)"),F.col("min(YEAR)")).cast(IntegerType()))

)
other_elements_year.cache()
other_elements_year.count()
other_elements_year.show(10,False)
#+-------+---------+---------+-----------+
#|ELEMENT|min(YEAR)|max(YEAR)|COVEREDYEAR|
#+-------+---------+---------+-----------+
#|PGTM   |1948     |2017     |70         |
#|SN01   |1982     |2009     |28         |
#|SN11   |1982     |2012     |31         |
#|SN57   |2015     |2015     |1          |
#|SX81   |1982     |1990     |9          |
#|WESD   |1952     |2017     |66         |
#|WT08   |1851     |2017     |167        |
#|SN23   |1983     |2010     |28         |
#|SN54   |2002     |2012     |11         |
#|SX11   |1982     |2012     |31         |
#+-------+---------+---------+-----------+



# The 5 longest recorded element
other_elements_year.sort("COVEREDYEAR",ascending = False).show(10,False)
#+-------+---------+---------+-----------+
#|ELEMENT|min(YEAR)|max(YEAR)|COVEREDYEAR|
#+-------+---------+---------+-----------+
#|DWPR   |1832     |2017     |186        |
#|MDPR   |1832     |2017     |186        |
#|DAPR   |1832     |2017     |186        |
#|WT08   |1851     |2017     |167        |
#|WT03   |1851     |2017     |167        |
#|WT11   |1851     |2017     |167        |
#|WT01   |1851     |2017     |167        |
#|WT05   |1851     |2017     |167        |
#|WT04   |1852     |2017     |166        |
#|WT09   |1852     |2017     |166        |
#+-------+---------+---------+-----------+


# The earliest recorded element
other_elements_year.orderBy("min(YEAR)",ascending = True).show(1,False)
#+-------+---------+---------+-----------+
#|ELEMENT|min(YEAR)|max(YEAR)|COVEREDYEAR|
#+-------+---------+---------+-----------+
#|DWPR   |1832     |2017     |186        |
#+-------+---------+---------+-----------+

# Elements stop being recording before 2010
other_elements_year.filter(F.col("max(YEAR)") < 2010).show(5,False)
#+-------+---------+---------+-----------+
#|ELEMENT|min(YEAR)|max(YEAR)|COVEREDYEAR|
#+-------+---------+---------+-----------+
#|SN01   |1982     |2009     |28         |
#|SX81   |1982     |1990     |9          |
#|SX61   |1982     |1996     |15         |
#|SX72   |1982     |1990     |9          |
#|WT12   |1985     |2008     |24         |
#+-------+---------+---------+-----------+




# Load table stations
file_path='hdfs:///user/jwu46/outputs/ghcnd/stations.parquet'
stations = spark.read.parquet(file_path)

#  Geographically investigate the coverage of the other element

# Define the function to group the stations in Southern Hermisphere or Northen Hermisphere
def func(LATITUDE):
    if LATITUDE >= 66 and LATITUDE <=90: 
        return "ARCTIC CIRCLE"
    elif LATITUDE >= 23 and LATITUDE < 66: 
        return "NORTHERN TEMPERATE ZONE"
    elif LATITUDE >= 0  and LATITUDE < 23:
        return "NORTHERN TROPICS"
    elif LATITUDE >= -23 and LATITUDE < 0: 
        return "SOUTHERN TROPICS"
    elif LATITUDE >= -66 and LATITUDE < -23:
        return "SOUTHERN TEMPERATE ZONE"
    else: 
        return "ANTARCTIC CIRCLE"


func_udf = F.udf(func)


# Get the count of the other element in Southern Hermisphere and Northen Hermisphere
geo_other_elements = (
    all_daily
    .select("ID","ELEMENT")
    .filter(
        (all_daily.ELEMENT != "PRCP") &
        (all_daily.ELEMENT != "SNOW") &
        (all_daily.ELEMENT != "SNWD") &
        (all_daily.ELEMENT != "TMAX") &
        (all_daily.ELEMENT != "TMIN")
    )
    .join(
        stations
        .select("ID","LATITUDE"),
        on="ID",
        how="left"
    )
    .withColumn('ZONE',func_udf(F.col('LATITUDE')))
    .groupBy("ELEMENT")
    .pivot("ZONE")
    .agg({"ELEMENT":"count"})
)
geo_other_elements.cache()
geo_other_elements.show(10,False)
#+-----------------------+----------------+
#|ELEMENT|ANTARCTIC CIRCLE|ARCTIC CIRCLE|NORTHERN TEMPERATE ZONE|NORTHERN TROPICS       |SOUTHERN TEMPERATE ZONE|SOUTHERN TROPICS|
#+-------+----------------+-------------+-----------------------+----------------       +-----------------------+----------------+
#|PGTM   |42677           |90410        |8614517                |392748                 |null                   |27300           |
#|WT08   |235             |8193         |2935334                |23670                  |null                   |77              |
#|WESD   |565             |61535        |11668982               |59300                  |null                   |null            |
#|SN57   |null            |null         |31                     |null                   |null                   |null            |
#|SN01   |null            |null         |187266                 |31                     |null                   |null            |
#|SN11   |null            |null         |10197                  |null                   |null                   |null            |
#|SX81   |null            |null         |3075                   |null                   |null                   |null            |
#|SN54   |null            |null         |3900                   |null                   |null                   |null            |
#|WT05   |null            |957          |430902                 |7534                   |null                   |681             |
#|SX72   |null            |null         |3075                   |null                   |null                   |null            |
#+-------+----------------+-------------+-----------------------+----------------       +-----------------------+----------------+



# What elements are the top five in ARCTIC CIRCLE
geo_other_elements.sort("ARCTIC CIRCLE",ascending = False).show(5,False)
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|ELEMENT|ANTARCTIC CIRCLE|ARCTIC CIRCLE|NORTHERN TEMPERATE ZONE|NORTHERN TROPICS|SOUTHERN TEMPERATE ZONE|SOUTHERN TROPICS|
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|TAVG   |428067          |3359084      |66371906               |7113103         |3787687                |3804887         |
#|TOBS   |null            |222598       |165529964              |1983457         |null                   |7887            |
#|PGTM   |42677           |90410        |8614517                |392748          |null                   |27300           |
#|WSFG   |42719           |78616        |4654966                |308202          |null                   |25277           |
#|WDFG   |41627           |78508        |4574344                |304475          |null                   |25237           |
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
geo_other_elements.filter(F.col("ARCTIC CIRCLE").isNotNull()).count() #60



# What elements are the top five in NORTHERN TEMPERATE ZONE
geo_other_elements.sort("NORTHERN TEMPERATE ZONE",ascending = False).show(5,False)
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|ELEMENT|ANTARCTIC CIRCLE|ARCTIC CIRCLE|NORTHERN TEMPERATE ZONE|NORTHERN TROPICS|SOUTHERN TEMPERATE ZONE|SOUTHERN TROPICS|
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|TOBS   |null            |222598       |165529964              |1983457         |null                   |7887            |
#|TAVG   |428067          |3359084      |66371906               |7113103         |3787687                |3804887         |
#|WESD   |565             |61535        |11668982               |59300           |null                   |null            |
#|WT01   |7925            |76947        |8688648                |52053           |null                   |186             |
#|PGTM   |42677           |90410        |8614517                |392748          |null                   |27300           |
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
geo_other_elements.filter(F.col("NORTHERN TEMPERATE ZONE").isNotNull()).count() #127



# What elements are the top five in NORTHERN TROPICS
geo_other_elements.sort("NORTHERN TROPICS",ascending = False).show(5,False)
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|ELEMENT|ANTARCTIC CIRCLE|ARCTIC CIRCLE|NORTHERN TEMPERATE ZONE|NORTHERN TROPICS|SOUTHERN TEMPERATE ZONE|SOUTHERN TROPICS|
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|TAVG   |428067          |3359084      |66371906               |7113103         |3787687                |3804887         |
#|TOBS   |null            |222598       |165529964              |1983457         |null                   |7887            |
#|PGTM   |42677           |90410        |8614517                |392748          |null                   |27300           |
#|WT16   |72              |25704        |4659253                |332451          |null                   |22964           |
#|WSFG   |42719           |78616        |4654966                |308202          |null                   |25277           |
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
geo_other_elements.filter(F.col("NORTHERN TROPICS").isNotNull()).count()#69


# What elements are the top five in SOUTHERN TROPICS
geo_other_elements.sort("SOUTHERN TROPICS",ascending = False).show(5,False)
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|ELEMENT|ANTARCTIC CIRCLE|ARCTIC CIRCLE|NORTHERN TEMPERATE ZONE|NORTHERN TROPICS|SOUTHERN TEMPERATE ZONE|SOUTHERN TROPICS|
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|TAVG   |428067          |3359084      |66371906               |7113103         |3787687                |3804887         |
#|MDPR   |null            |1730         |571921                 |289545          |1043730                |75163           |
#|DAPR   |null            |127          |395155                 |285946          |1031367                |73610           |
#|DWPR   |null            |null         |null                   |null            |1013366                |70501           |
#|PGTM   |42677           |90410        |8614517                |392748          |null                   |27300           |
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
geo_other_elements.filter(F.col("SOUTHERN TROPICS").isNotNull()).count() #49


# What elements are the top five in SOUTHERN TEMPERATE ZONE
geo_other_elements.sort("SOUTHERN TEMPERATE ZONE",ascending = False).show(5,False)
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|ELEMENT|ANTARCTIC CIRCLE|ARCTIC CIRCLE|NORTHERN TEMPERATE ZONE|NORTHERN TROPICS|SOUTHERN TEMPERATE ZONE|SOUTHERN TROPICS|
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|TAVG   |428067          |3359084      |66371906               |7113103         |3787687                |3804887         |
#|MDPR   |null            |1730         |571921                 |289545          |1043730                |75163           |
#|DAPR   |null            |127          |395155                 |285946          |1031367                |73610           |
#|DWPR   |null            |null         |null                   |null            |1013366                |70501           |
#|MDTN   |null            |null         |null                   |null            |65873                  |13667           |
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
geo_other_elements.filter(F.col("SOUTHERN TEMPERATE ZONE").isNotNull()).count() #8
geo_other_elements.filter(F.col("SOUTHERN TEMPERATE ZONE").isNotNull()).show()
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|ELEMENT|ANTARCTIC CIRCLE|ARCTIC CIRCLE|NORTHERN TEMPERATE ZONE|NORTHERN TROPICS|SOUTHERN TEMPERATE ZONE|SOUTHERN TROPICS|
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|   DWPR|            null|         null|                   null|            null|                1013366|           70501|
#|   MDTX|            null|         null|                   null|            null|                  64461|           13441|
#|   DATX|            null|         null|                   null|            null|                  64461|           13441|
#|   DATN|            null|         null|                   null|            null|                  65873|           13667|
#|   MDPR|            null|         1730|                 571921|          289545|                1043730|           75163|
#|   MDTN|            null|         null|                   null|            null|                  65873|           13667|
#|   TAVG|          428067|      3359084|               66371906|         7113103|                3787687|         3804887|
#|   DAPR|            null|          127|                 395155|          285946|                1031367|           73610|
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+



# What elements are the top five in ANTARCTIC CIRCLE
geo_other_elements.sort("ANTARCTIC CIRCLE",ascending = False).show(5,False)
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|ELEMENT|ANTARCTIC CIRCLE|ARCTIC CIRCLE|NORTHERN TEMPERATE ZONE|NORTHERN TROPICS|SOUTHERN TEMPERATE ZONE|SOUTHERN TROPICS|
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
#|TAVG   |428067          |3359084      |66371906               |7113103         |3787687                |3804887         |
#|WSFG   |42719           |78616        |4654966                |308202          |null                   |25277           |
#|PGTM   |42677           |90410        |8614517                |392748          |null                   |27300           |
#|WDFG   |41627           |78508        |4574344                |304475          |null                   |25237           |
#|WT18   |21163           |51498        |1368868                |400             |null                   |27              |
#+-------+----------------+-------------+-----------------------+----------------+-----------------------+----------------+
geo_other_elements.filter(F.col("ANTARCTIC CIRCLE").isNotNull()).count() #29


# Challenges Q2
quality_check = (
    all_daily
    .filter(all_daily.QUALITY_FLAG.isNotNull())
)

quality_check.count() # 7757511




sperate_quality_check = (
    all_daily
    .select(["ID","QUALITY_FLAG"])
    .filter(F.col("QUALITY_FLAG").isNotNull())
    .groupBy("QUALITY_FLAG")
    .agg({"QUALITY_FLAG":"count"})
    .orderBy("count(QUALITY_FLAG)", ascending = False)
)

sperate_quality_check.show()
#+------------+-------------------+
#|QUALITY_FLAG|count(QUALITY_FLAG)|
#+------------+-------------------+
#|           I|            6163942|
#|           D|             687162|
#|           S|             213023|
#|           X|             135086|
#|           G|             111259|
#|           O|             109419|
#|           K|             104899|
#|           L|              96107|
#|           Z|              68817|
#|           M|              27748|
#|           N|              16659|
#|           W|              13917|
#|           R|               8192|
#|           T|               1281|
#+------------+-------------------+


