
Task1 : temperature difference among 5-nearest stations

Files:

1. pair.py

This file is used to get station pair csvfile by using station.csv, which is all station csv list.
The output csv is pair.csv
########################

2. task2_StationPair.py

This file is used to get our final result of top 5 largest temperature difference station among nearest 5 neigbors by year.

To run this in shell:
    python3 task1_StationPair.py --jobconf mapreduce.job.reduces=1 pairs.csv > result.txt
