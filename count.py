from mrjob.job import MRJob
from datetime import datetime
from datetime import date
from datetime import timedelta
from mrjob.step import MRStep
import numpy as np
from geopy.distance import vincenty

# Use MapReduce to find different stations that experience same trend in the similar time duration, for example, 7 days.
# Analyze the wind direction and latitude, longitude of the station, find whether the airstream affect one station then the other.(or say, whether there are relations between the temp fluctuation)
class same_trend(MRJob):

    def mapper(self, _, line):
    	fields = line.split(',')
    	latitude = (fields[0].strip('[')).strip('"')
    	longitude = (fields[1].strip(']')).replace('"','')
    	longitude = longitude.strip(' ')
    	infos = fields[2].split(']')
    	r_date = datetime.strptime(infos[0].strip('"').strip(' "'),'%Y-%m-%d').date()
    	f_date = (r_date - date(2006,1,1)).days
    	r_date = r_date.isoformat()
    	d_temp_mean = infos[1].strip('[')
    	d_temp_mean = d_temp_mean.strip('\t')
    	wind_dir_mean = fields[3].strip(']').strip(' ')
    	#yield (latitude, longitude, r_date), (d_temp_mean, wind_dir_mean, f_date)
    	#yield f_date, (latitude, longitude, r_date)
    	yield r_date, (latitude, longitude)


    # get the number of big_temp change for each date
    def combiner(self, r_date, values):
        num = 0
        for v in values:
            num += 1
        yield r_date, num



if __name__ == '__main__':
    same_trend.run()
   

#python3 read.py --jobconf mapreduce.job.reduces=1 results.txt > final.txt


