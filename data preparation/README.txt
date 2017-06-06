CAPP 30123: Final Project


This is the first step of this project: Data Preparation


Substeps taken:
    1. Collect urls for all the targeted data files.
    2. Get data and store them on Google cloud using wget.
    3. Clean data by omitting redundant variables and aggregate data files by station.
    4. Get identifiers of all the stations as preparation for pairwise comparison among stations.


Files:
1.get_url_list_by_year.py
This file is used to gether all url to download station csv file from year 2006 - 2016.

2. clean_aggregation.py

This file is used to aggregate files of each year the same station.

We downloaded the data files using "wget" in shell script, with a csv file 
    containing all the urls of data.

Each station has a unique number as identifier. If one station has record of 
    several years, records of each year would be stored in seperated files,
    which is designed by the website providing the data. The first file would
    be stored in id.csv, the the following files would be stored in id.csv.1,
    id.csv.2, ...

To run this in shell, put this .py file in the folder storing all the data
    files, use:
    for file in `ls *.csv`; do
        LST=`ls ${file}*`
        python3 clean_aggregation.py "${LST}"
    done;

########################

3. get_station_id.py

This file is used to get the identifiers (unique filenames) of all the 
    stations, for doing pairwise comparison in the next step.

To run this in shell:
    python3 get_station_id.py

