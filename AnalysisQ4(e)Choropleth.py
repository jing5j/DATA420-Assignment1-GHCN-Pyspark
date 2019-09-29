# DATA420 Assignment1
# Analysis Q4(e)

# Import

import pandas as pd
import glob, os
from pygal_maps_world.maps import COUNTRIES
import pygal_maps_world.maps
import pygal.style


# Load the csv files which are copied from hdfs

path = '/Users/wujing/Desktop/DATA420/Assignment1/A1/PRCP_avg.csv'
file = glob.glob(os.path.join(path,"*.csv"))
dl = []
for f in file:
    dl.append(pd.read_csv(f,index_col=None))
df=pd.concat(dl)


# Select columns and compute the average rainfall of each country

df1 = df[["COUNTRY_CODE","AVERAGE_RAINFALL"]]
df1 = df1.groupby(df1.COUNTRY_CODE).mean()
dict1 = df1.to_dict()


# Classify the country by the average rainfall value

data1={}
data2={}
data3={}
data4={}
for key,value in dict1['AVERAGE_RAINFALL'].items():
    if value <= 30:
        data1[key.lower()] = value
    elif 30 < value <= 100:
        data2[key.lower()] = value
    elif 100 < value <= 200:
        data3[key.lower()] = value        
    else:
        data4[key.lower()] = value


# Plot

worldmap_chart = pygal_maps_world.maps.World()
worldmap_chart.title = "Average Rainfall"
worldmap_chart.add('> 200', data4)
worldmap_chart.add('100 - 200', data3)
worldmap_chart.add('30 - 100', data2)
worldmap_chart.add('< 30', data1)

# Save as .svg
worldmap_chart.render_to_file('AVERAGE_RAINFALL.svg')