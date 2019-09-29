# DATA420 Assignment 1
# Jing Wu 29696576


# Analysis Q3
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


# Load the 2010 daily file (csv format)

daily_2010 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2010.csv.gz")
)
daily_2010.cache()
daily_2010.count()# 36946080


# Load the 2017 daily file (csv format)

daily_2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
)
daily_2017.cache()
daily_2017.count()# 21904999



# Load the 2010 to 2015 daily file (csv format)

daily_2010_to_2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/201[0-5].csv.gz")
)

daily_2010_to_2015.count()#207716098
