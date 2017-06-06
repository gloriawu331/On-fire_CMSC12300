# CAPP 30123: FINAL PROJECT
# Group: On-Fire
# Author: Xingyun Wu

# This file is used to do the second MapReduce of pairwise comparison. Its
#   purpose is to find linked records.

# Input:
#   a data file containing records with big temperature changes. Could be
#       the file of all records, or the file for a subset of data.

# Run it using:
#   python3 getpair.py --jobconf mapreduce.job.reduces=1 data.csv > results.txt

import os
from mrjob.job import MRJob
from geopy.distance import great_circle

dir_path = os.path.dirname(os.path.realpath(__file__))


class GetPair(MRJob):
    
    
    def mapper(self, _, line):
        fields = line.split(',')
        
        lat = fields[0].strip('[')
        lat = float(lat)
        lon = fields[1].strip()
        lon = float(lon)
        r_date = fields[2].strip(' "')
        f_date = int(float(fields[3].strip()))
        d_temp = float(fields[4].strip())
        wind_dir = fields[5].strip(' ]\t1\n')
        yield (lat, lon, r_date, f_date, d_temp, wind_dir), 1
    
    def combiner(self, key, values):
        lat1 = key[0]
        lon1 = key[1]
#        r_date1 = key[2]
        f_date1 = key[3]
#        d_temp1 = key[4]
#        wind_dir1 = key[5]
        
        with  open(dir_path + "/2006_down_sorted.txt") as file:
            data = file.readlines()
        
        flag = True
        while flag == True:
            for i in range(len(data)):
                infos = data[i].split(',')
                lat2 = infos[0].strip('[')
                lat2 = float(lat2)
                lon2 = infos[1].strip()
                lon2 = float(lon2)
                r_date2 = infos[2].strip(' "')
                f_date2 = int(float(infos[3].strip()))
                d_temp2 = float(infos[4].strip())
                wind_dir2 = infos[5].strip(' ]\t1\n')
            
                if f_date2 > f_date1:
                    if f_date2 <= f_date1 + 3:
                        distance = great_circle((lat2, lon2), (lat1, lon1)).miles
                        if distance <= float(300):
#                            print(key, (lat2, lon2, r_date2, f_date2, d_temp2, wind_dir2, distance))
                            yield key, (lat2, lon2, r_date2, f_date2, d_temp2, wind_dir2, distance)
                    else:
                        flag = False
                
                if i == len(data)-1:
                    flag = False
    
    def reducer(self, key, values):
        yield key, values


if __name__ == '__main__':
    GetPair.run()
