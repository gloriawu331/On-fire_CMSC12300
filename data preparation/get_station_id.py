# CAPP 30123: FINAL PROJECT
# Group: On-Fire
# Author: Xingyun Wu

# This file is used to get the identifiers (unique filenames) of all the 
# stations, for doing pairwise comparison in the next step.

import csv
import re

with open("myfile.csv") as f:
    reader = csv.reader(f)
    urls = list(reader)

names = []
for line in urls:
    url = line[0]
    name = re.search('[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].csv', url).group(0)
    if name not in names:
        names.append(name)

with open("stations.csv", 'w') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    for name in names:
        wr.writerow([name])