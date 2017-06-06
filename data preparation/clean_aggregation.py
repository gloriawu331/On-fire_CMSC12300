# CAPP 30123: FINAL PROJECT
# Group: On-Fire
# Author: Xingyun Wu

# This file is used to aggregate files of each year the same station.

# We downloaded the data files using "wget" in shell script, with a csv file 
#   containing all the urls of data.

# Each station has a unique number as identifier. If one station has record of 
#   several years, records of each year would be stored in seperated files,
#   which is designed by the website providing the data. The first file would
#   be stored in id.csv, the the following files would be stored in id.csv.1,
#   id.csv.2, ...

#To run this in shell, put this .py file in the folder storing all the data
#   files, use:
#   for file in `ls *.csv`; do
#       LST=`ls ${file}*`
#       python3 clean_aggregation.py "${LST}"
#   done;

import sys
#print(sys.argv)
import pandas as pd

def join_data_file(filenames):
    '''
    filenames: a list of strings
    '''
    frames = []
#    argv_lst = str(filenames)
#    print(filenames)
    file_names = filenames[0].splitlines()
#    print(file_names)
    
    for i in range(len(file_names)):
#        print(i+1)
        temp_df = pd.read_csv(file_names[i])
        temp_df = temp_df[['STATION', 'DATE', 'LATITUDE', 'LONGITUDE', 'NAME', 'TMP', 'DEW', 'WND']]
        frames.append(temp_df)

    df = pd.concat(frames)   
    df.to_csv(path_or_buf="../../../joined/data/"+file_names[0], index_label='id')
    
    return

if __name__ == '__main__':
    join_data_file(sys.argv[1:])
