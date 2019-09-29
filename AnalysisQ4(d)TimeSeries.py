# DATA420 Assignment1
# Analysis Q4(d)


# Import

import pandas as pd
import glob, os
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
from datetime import datetime


# Load the csv files which are copied from hdfs

path = '/Users/wujing/Desktop/DATA420/Assignment1/A1/NZ_TMIN_TMAX.csv'
file = glob.glob(os.path.join(path,"*.csv"))
dl = []
for f in file:
    dl.append(pd.read_csv(f,index_col=None))
df=pd.concat(dl)


# Select columns 

columns = ["STATION_NAME","DATE","ELEMENT","VALUE"]
NZ = df[columns]


# Convert DATE Format

NZ['DATE']=NZ['DATE'].apply(lambda x:datetime.strptime(str(x),'%Y%m%d'))


# Define function to plot the timeseries for each station  
def plot(i,NAME):
    station_df = NZ.loc[NZ["STATION_NAME"] == NAME]
    station_df = pd.pivot_table(station_df,index='DATE',columns = 'ELEMENT',values='VALUE')
    station_df = station_df.groupby(station_df.index.year).mean()
    ax = fig.add_subplot(4,4,(1+i))
    plt.title(NAME)
    ax.plot(station_df.index,station_df['TMIN'],color='coral')
    ax.plot(station_df.index,station_df['TMAX'],color='#8A2BE2')    


# Define a figure
fig = plt.figure(figsize=(32, 32))
plt.style.use("ggplot") 


# Get the list of STATION NAME

stations = NZ["STATION_NAME"].drop_duplicates().tolist()


# Plot

for i,NAME in enumerate(stations):
    plot(i,NAME)  
    
# Save
plt.tight_layout()
fig.savefig("max_min.png")



# Define function to plot the average time series for the entire country

def avg_plot(i,NAME):
    station_df = NZ.loc[NZ["STATION_NAME"] == NAME]
    station_df = pd.pivot_table(station_df,index='DATE',columns = 'ELEMENT',values='VALUE')
    station_df['AVERAGE'] = station_df.apply(lambda x: (x.TMIN + x.TMAX)/2, axis=1)
    station_df = station_df.groupby(station_df.index.year).mean()
    ax = fig1.add_subplot(4,4,(1+i))
    plt.title(NAME)
    ax.plot(station_df.index,station_df['AVERAGE'],color='coral')


# Define a figure

fig1 = plt.figure(figsize=(32, 32))


# Plot 
for i,NAME in enumerate(stations):
    avg_plot(i,NAME)

# Save
fig1.savefig("average.png")        
