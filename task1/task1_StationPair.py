from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import date
from geopy.distance import vincenty
import numpy as np
import queue


count_pair = 0

class temp_diff(MRJob):


    def mapper(self, _, line):
        '''
        Read a pair station csv, and yield two station's information
        '''
        #create a global count,
        # add it to sort each pair of station in mapper
        global count_pair
        count_pair += 1
        names = line.split(',')
        n1 = names[0].replace('"', '')
        n2 = names[1].replace('"', '') 

        # line is a pair of two station csv file
        # read in two station and yield seperately      
        l1 = open('/mnt/storage/data/cleaned_by_station/' + n1, 'r+')
        l2 = open('/mnt/storage/data/cleaned_by_station/' + n2, 'r+')

        for line1 in l1:
            l = line1.split(',')

            if l[7]!='DEW':
                    
                temp = int(l[7].strip('"'))

                if temp != 9999:
                    lat = l[3].strip('"')
                    long_ = l[4].strip('"')
                    location = (lat, long_)
                    date = (l[2].replace('"', '').split('T'))[0]
                    #use count_pair to yield pair to avoid being reduced
                    #in reducer
                    yield (location, date, count_pair), temp

        for line2 in l2:
            l = line2.split(',')
            if l[7]!='DEW':

                temp = int(l[7].strip('"'))

                if temp != 9999:
                    lat = l[3].strip('"')
                    long_ = l[4].strip('"')
                    location = (lat, long_)
                    date = (l[2].replace('"', '').split('T'))[0]
                    #use count_pair to yield pair to avoid being reduced
                    #in reducer
                    yield (location, date, count_pair), temp


    def combiner(self, locationdate, temp):
        '''
        get mean of station temperature by date
        '''
        location, date, count = locationdate
        s = 0
        num = 0
        for t in temp:
            s += t
            num += 1
        mean = s/ num
        yield (date,count), (location, mean)
        
     
    def reducer(self, date, locmean):
        '''
        generate station pair temperature difference 
        and use geopy package to get the distance between them
        '''
        d, c = date
        l = list(locmean)
        if len(l) == 2:
            loc1 = l[0][0]
            loc2 = l[1][0] 
            dist = vincenty(loc1, loc2).miles
            diff = np.abs(l[0][1] - l[1][1])

            yield (d,loc1,loc2, diff), None


    def reducer2(self, key, _):
        '''
        use PriorityQueue to keep track of nearest_5 neigbor stations
        '''
        date,location,dist, diff = key
        location = tuple(location)
        info = (location, date)
        #print(self.dic)

        if info in self.dic:
            if self.dic[info].full():
                if (-dist) > min(self.dic[info].queue)[0]:
                    self.dic[info].get()
                    self.dic[info].put((-dist, key))
            else:
                self.dic[info].put((-dist,key))

        else:
            nearest_10 = queue.PriorityQueue(maxsize=5)
            nearest_10.put((-dist,key))
            self.dic[info] = nearest_10


    def reducer2_final(self):
        '''
        yield station information in queue
        '''
        #sort
        for item in self.dic:
            #Queue.sort(reverse=True)
            Queue = self.dic[item].queue
            for i in Queue:
                yield i[1], None

    def mapper3(self, key, _):
        '''
        map the nearest_5 stations using year instead of date
        '''
        date, loc, diff = key
        year = date[:4]
        yield (year, loc), diff
        
    def combiner3(self, key, diff):
        '''
        get mean of temperature difference of each station by year
        '''
        year, loc = key
        s = 0
        num = 0
        for d in diff:
            s += d
            num += 1
        yearmean = s/ num
        yield (year, loc), yearmean

              
    def reducer_clean(self, key, yearmean):
        year, loc = key

        s = 0
        num = 0
        for y in yearmean:
            s += y
            num += 1
        m = s/ num
        yield (year, loc, m), None
        
        
    def reducer3_init(self):
        self.topyear = {}
        
    def reducer3(self, key, _):
        '''
        Keep track the top 5 temperature difference station by year
        '''
        year, loc, m = key
        loc = tuple(loc)
        #info = (loc, year)
        if year in self.topyear:
            if self.topyear[year].full():
                if (m) > min(self.topyear[year].queue)[0]:
                    self.topyear[year].get()
                    self.topyear[year].put((m, key))
            else:
                self.topyear[year].put((m, key))

        else:
            top3 = queue.PriorityQueue(maxsize=5)
            top3.put((m, key))
            self.topyear[year] = top3
                        
    def reducer3_final(self):
        for item in self.topyear:
            Queue = self.topyear[item].queue
            for i in Queue:
                yield i[1], None
 

    def steps(self):
        return [
          MRStep(mapper=self.mapper,
                    combiner=self.combiner,
                    reducer=self.reducer),
          MRStep(reducer_init = self.reducer2_init,
                    reducer = self.reducer2,
                    reducer_final = self.reducer2_final),
          MRStep(mapper = self.mapper3,
                  combiner = self.combiner3,
                  reducer = self.reducer_clean),
          MRStep(reducer_init = self.reducer3_init,
                 reducer = self.reducer3,
                 reducer_final = self.reducer3_final)]



if __name__ == '__main__':

        temp_diff.run()

