from mrjob.job import MRJob
from datetime import datetime
from datetime import timedelta
from mrjob.step import MRStep
import numpy as np

# MapReduce + temp difference
# Use MapReduce to generate temp diff that larger then 80. 
# Save the results into tex/csvt file
# Use MapReduce to find different stations that experience same trend in the similar time duration, for example, 7 days.
# Analyze the wind direction and latitude, longitude of the station, find whether the airstream affect one station then the other.
#(or say, whether there are relations between the temp fluctuation)

class temp_diff(MRJob):
    
     # Yield twice
    def mapper(self, _, line):
        fields = line.split(',')
        if fields[2] != 'DATE':
            infos = fields[2].replace('"', '').split('T')
            date = datetime.strptime(infos[0],'%Y-%m-%d').date()
            date2 = (date + timedelta(days=1)).isoformat()
            date = date.isoformat() 
            time = infos[1]
            latitude = fields[3].strip('"')
            longitude = fields[4].strip('"')
            location = (latitude, longitude)
            temp = int(fields[7].strip('"'))
            wind_dir = int(fields[11].strip('"'))
            if temp != 9999 and wind_dir != 999:
                yield (location, date, time), (temp, wind_dir, 1)
                yield (location, date2, time), (temp, wind_dir, 0)
    
    # Yield twice(loop over folder)
    # def mapper(self, _, line):
    #     file = line.replace('"', '')
    #     station = open('/mnt/storage/data/cleaned_by_station' + file, 'r+')
    #     for l in station:
    #         fields = l.split(',')
    #         if fields[2] != 'DATE':
    #             infos = fields[2].replace('"', '').split('T')
    #             date = datetime.strptime(infos[0],'%Y-%m-%d').date()
    #             date2 = (date + timedelta(days=1)).isoformat()
    #             date = date.isoformat() 
    #             time = infos[1]
    #             latitude = fields[3].strip('"')
    #             longitude = fields[4].strip('"')
    #             location = (latitude, longitude)
    #             temp = int(fields[7].strip('"'))
    #             wind_dir = int(fields[11].strip('"'))
    #             if temp != 9999 and wind_dir != 999:
    #                 yield (location, date, time), (temp, wind_dir, 1)
    #                 yield (location,, date2, time), (temp, wind_dir, 0)
   

    # Calculate the difference between temperature now and temp 24 hours ago
    # temp is today - yesterday; wind_dir is yesterday
    def combiner(self, keys, values):
        val = list(values)
        if len(val) == 2:
            if val[0][2] == 0:
                d_temp = val[0][0] - val[1][0]
                wind_dir = val[1][1]
            else:
                d_temp = val[1][0] - val[0][0]
                wind_dir = val[0][1]
            yield keys, (d_temp, wind_dir)

    # Find the absolute temperature difference that larger than 80
    def reducer(self, keys, temp_wind):
        for tw in temp_wind:
            if np.abs(tw[0]) >= 80 and tw[1] <= 360:
                yield keys, (tw[0], tw[1])



    # Only need one row data per day
    def mapper2(self, keys, temp_wind):
        location, date, time = keys
        temp, wind_dir = temp_wind  
        yield (location, date), (temp, wind_dir)
    
    # Get temperature difference mean and wind_dir mean
    def combiner2(self, keys, temp_wind):
        sum_1 = 0
        sum_2 = 0
        num = 0
        for tw in temp_wind:
            sum_1 += tw[0]
            sum_2 += tw[1]
            num += 1
        yield keys, (sum_1/num, sum_2/num)
    
    # Generate the temperature difference mean larger than 80, to make sure the 
    # generated data is not because of the temp fluctuation caused by sunset/sunrise
    # change.
    def reducer2(self, keys, means):
        lst = list(means)
        if np.abs(lst[0][0]) >= 80 and lst[0][1] <= 360:
            yield keys, (lst[0][0],lst[0][1])


    def steps(self):
        return [
          MRStep(mapper = self.mapper,
                    combiner = self.combiner,
                    reducer = self.reducer),
          MRStep(mapper = self.mapper2,
                    combiner = self.combiner2,
                    reducer = self.reducer2)]
    


if __name__ == '__main__':
    temp_diff.run()
    

#python3 Mapdiff.py --jobconf mapreduce.job.reduces=1 00702699999.csv
#python3 Mapdiff.py --jobconf mapreduce.job.reduces=1 00702699999.csv > 00702699999.txt
#python3 Mapdiff.py --jobconf mapreduce.job.reduces=1 stations.csv > station_result.txt












