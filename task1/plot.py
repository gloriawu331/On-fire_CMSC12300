#!/usr/bin/env python3
# -*- coding: utf-8 -*-x

import pandas as pd
import plotly.graph_objs as go
import plotly
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

    

def plotdf(file, year):
    '''
    Read results to dataframe to plot
    '''

    df = pd.read_fwf(file,header = None, usecols = [0,1])
    df[0] = df[0].str.strip('[,').str.strip('"')
    df = df[df[0]== str(year)]
    
    # cleaned df
    dff = pd.DataFrame()
#    dff['year'] = df[0].str.strip('[,').str.strip('"')
    dff['year'] = df[0]
    dff['lat'] = df[1].str.split(',',3).str[0].str.strip('["')
    dff['long'] = df[1].str.split(',',3).str[1].str.strip(']"').str.replace('"', '')
#    dff['dist'] = df[1].str.split(',',3).str[2].str.replace(']\tnull','').str.strip(']')
    dff['temp_diff'] = df[1].str.split(',').str[4].str.replace(']\tnull','').str.strip(']')
    
    return dff


'''
Plot mean temperature by year
'''
df = pd.read_csv('merge.txt',header = None, usecols = [0,1,2,5])
df[0] = df[0].str.strip('[,').str.strip('"')

# cleaned df
dff = pd.DataFrame()
#    dff['year'] = df[0].str.strip('[,').str.strip('"')
dff['year'] = df[0]
dff['lat'] = df[1].str.split(',',3).str[0].str.strip('["')
dff['long'] = df[1].str.split(',',3).str[1].str.strip(']"').str.replace('"', '')
#    dff['dist'] = df[1].str.split(',',3).str[2].str.replace(']\tnull','').str.strip(']')
dff['temp_diff'] = df[1].str.split(',',3).str[2].str.replace(']\tnull','').str.strip(']').astype(float)
diffmean = dff.groupby('year').mean()
diffmean.plot(type = 'line')



def mapdf(file):
    '''
    Map plot df
    '''
    df = pd.read_csv(file)
    df = df.replace(['LATITUDE', 'LONGITUDE', 'STATION', 'DATE', 'NAME', 'WND', 'TMP','DEW','id'], np.nan)
    df = df.dropna(axis = 0, how = 'all').rename(columns={'LATITUDE':'lat', 'LONGITUDE':'long'})
#    df.to_csv('cleanedlines.csv')
    return df
    


def interactivepoints(df):
    '''
    plot interactive map points
    usage: interactivepoints(use)
    '''

    mapbox_access_token = 'pk.eyJ1IjoiY2hlbHNlYXBsb3RseSIsImEiOiJjaXFqeXVzdDkwMHFrZnRtOGtlMGtwcGs4In0.SLidkdBMEap9POJGIe1eGw'
    
    plotly.tools.set_credentials_file(username='judylwj', api_key='zv1qhNQa7kApZyz11rCp')
    
    
    data = go.Data([
        go.Scattermapbox(
            lat = df.lat,
            lon = df.long,
            mode='markers',
            marker=go.Marker(
                size=10,
                color='rgb(255, 0, 0)',
                opacity=0.7
            ),
            hoverinfo='text'
        )]
    )
            
    layout = go.Layout(
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            accesstoken=mapbox_access_token,
            bearing=0,
            center=dict(
                lat=45,
                lon=-73
            ),
            pitch=0,
            zoom=5
        ),
    )
    
    return plotly.offline.plot({'data':data, 'layout':layout})

#for y in range(2008, 2017):
#    interactivepoints(plotdf('result1.txt', y))

interactivepoints(plotdf('mergeeu.txt',2016).sort_values(by = 'temp_diff', ascending = False).head(5))
interactivepoints(plotdf('mergeeu.txt',2007).sort('temp_diff', ascending = False).head(5))



  
def slicedf(year):
    '''
    slice df into years
    '''
    df = pd.read_fwf('outcome2.txt', names = ['date', 'num'])
    df.date = df.date.str.strip('"')
#    df['date'] = df['date'].dt.year
    dff = df[df['date'].apply(lambda x: x[:4]) == str(year)]
    dff['date'] = dff['date'].apply(lambda x: x[5:10])
    dff = dff.groupby('date').sum().num.to_frame()
    return dff


'''
Plot trends by year
'''
cur_path = os.path.split(os.path.abspath('__file__'))[0]
output_fldr = "images"
output_dir = os.path.join(cur_path, output_fldr)
if not os.access(output_dir, os.F_OK):    
    os.makedirs(output_dir)
    
linestyles = ["-", "--", "-.", ":"]
plt.figure(figsize=(10, 5))
ax=plt.gca()


for i in range(2006, 2017):
    slicedf(i).plot(ax = ax, label = str(i), linestyle=linestyles[i%4])

ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
output_path = os.path.join(output_dir, 'Q3')
plt.savefig(output_path,bbox_inches='tight')
plt.show()
plt.close()
      
      
distn = pd.read_csv('dfs.csv')
      
      

'''
Bar chart for yearly mean temperature
'''      
sns.set_palette("hls", 4)
sns.set(style='darkgrid')
#
plt.figure()
plt.title('Mean temp by year')
data = [21.0146, 24.7121, 21.2566, 32.172, 25.4016, 30.024, 29.882, 29.282]
sns.barplot(x = ['2016', '2015', '2014','2013', '2012', '2011', '2010', '2009'], y=data, palette="BuPu")
plt.xlabel('year')
plt.ylabel('mean temp')      


      
      
      
    


