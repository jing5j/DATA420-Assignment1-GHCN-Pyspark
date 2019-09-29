# DATA420 Assignment 1
# Jing Wu 29696576


# Analysis Q1

# Load the stations parquet file in hdfs
file_path='hdfs:///user/jwu46/outputs/ghcnd/stations.parquet'
stations = spark.read.parquet(file_path)

# How many stations are there in total? 
stations.count()#103656


# How many stations have been active in 2017?
active_stations_2017 = (
	stations
	.where(stations.FIRSTYEAR == 2017)
)
active_stations_2017.count() #459

# (a) How many stations are in each of the GCOS Surface Network (GSN), the US Historical
# Climatology Network (HCN), and the US Climate Reference Network (CRN)? Are there
# any stations that are in more than one of these networks?

# How many stations are in the GCOS Surface Network (GSN)?
GCOS_stations = (
	stations
	.where(stations.GSN_FLAG.isNotNull())
) 

GCOS_stations.count()#991

# How many stations are in the US Historical Climatology Network (HCN)?
# How many stations are in the US Climate Reference Network (CRN)?
hcn_crn_count = (
	stations
	.select("HCN_CRN_FLAG")
	.groupBy("HCN_CRN_FLAG")
	.count()
)
hcn_crn_count.show()
###RESULT
#+------------+------+
#|HCN_CRN_FLAG|count |
#+------------+------+
#|null        |102208|
#|CRN         |230   |
#|HCN         |1218  |
#+------------+------+


# Are there any stations that are in more than one of these networks?
both_network = (
	stations
	.where(
		(stations.GSN_FLAG.isNotNull()) & 
		(stations.HCN_CRN_FLAG.isNotNull())
	)
)
both_network.count()#14


# (b) Count the total number of stations in each country, and store the output in countries using
# the withColumnRenamed command.
# Count stations in each country
countries = (
	stations
        .select(
            F.col("ID"),
            F.col("COUNTRY_CODE").alias("CODE")
        )
        .groupBy("CODE")
        .agg({"ID":"count"})  
        .withColumnRenamed("count(ID)","COUNTRY_STATION_NUM")
        .join(
            countries,
            on="CODE",
            how="left"
        )
)
countries.show(10, False)

###RESULT
#+----+-------------------+---------------------------------+
#|CODE|COUNTRY_STATION_NUM|NAME                             |
#+----+-------------------+---------------------------------+
#|LT  |4                  |Lesotho                          |
#|CI  |21                 |Chile                            |
#|PC  |1                  |Pitcairn Islands [United Kingdom]|
#|FI  |922                |Finland                          |
#|IC  |11                 |Iceland                          |
#|PM  |10                 |Panama                           |
#|PU  |2                  |Guinea-Bissau                    |
#|NS  |1                  |Suriname                         |
#|RO  |30                 |Romania                          |
#|SL  |2                  |Sierra Leone                     |
#+----+-------------------+---------------------------------+


# Save as parquet file to hdfs
countries.write.parquet('hdfs:///user/jwu46/outputs/ghcnd/countries.parquet',mode="overwrite")


# Count stations in each state
states = (
    stations
    .select(
        F.col("ID"),
        F.col("STATE").alias("CODE")
    )
    .groupBy("CODE")
    .agg({"ID":"count"})      
    .withColumnRenamed("count(ID)","STATE_STATION_NUM")
    .join(
        states,
        on="CODE",
        how="left"        
    )
)
states.show(10,False)

###RESULT
#+----+-----------------+-------------------------+
#|CODE|STATE_STATION_NUM|NAME                     |
#+----+-----------------+-------------------------+
#|AZ  |1327             |ARIZONA                  |
#|SC  |965              |SOUTH CAROLINA           |
#|NS  |349              |NOVA SCOTIA              |
#|PI  |1                |PACIFIC ISLANDS          |
#|NL  |329              |NEWFOUNDLAND AND LABRADOR|
#|LA  |667              |LOUISIANA                |
#|MN  |1112             |MINNESOTA                |
#|NJ  |616              |NEW JERSEY               |
#|DC  |14               |DISTRICT OF COLUMBIA     |
#|OR  |1646             |OREGON                   |
#+----+-----------------+-------------------------+


# Save table states as parquet file
states.write.parquet('hdfs:///user/jwu46/outputs/ghcnd/states.parquet',mode="overwrite")


# (c) How many stations are there in the Southern Hemisphere only?
SH_stations = (
	stations
	.where(stations.LATITUDE < 0)
)
SH_stations.count()#25337

# How many stations are there in total in the territories of the United
# States around the world?
USA_stations = (
	stations
	.where(
		stations.COUNTRY_NAME.contains("United States")
	)
)

USA_stations.count()#57227

# Group each territories and count the stations
USA_stations_grouped = (
	USA_stations
	.select("COUNTRY_NAME")
	.groupBy("COUNTRY_NAME")
	.count()
)
USA_stations_grouped.show(20,False)
###RESULT
#+----------------------------------------+-----+
#|COUNTRY_NAME                            |count|
#+----------------------------------------+-----+
#|American Samoa [United States]          |21   |
#|Northern Mariana Islands [United States]|11   |
#|Johnston Atoll [United States]          |4    |
#|Puerto Rico [United States]             |203  |
#|United States                           |56918|
#|Midway Islands [United States}          |2    |
#|Wake Island [United States]             |1    |
#|Guam [United States]                    |21   |
#|Palmyra Atoll [United States]           |3    |
#|Virgin Islands [United States]          |43   |
#+----------------------------------------+-----+
