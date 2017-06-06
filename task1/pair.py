import csv


def paircsv():
    '''
    Input all station csvfile name to create station pair csv.
    use this csv to run mrjob below
    
    '''
    with open('pairs.csv', 'w') as csvfile, open('stations.csv', 'r+') as f:
            with open ('stations.csv', 'r+') as g:
                csv_writer = csv.writer(csvfile, quoting = csv.QUOTE_ALL)
                for line1 in f:
                    with open ('stations.csv', 'r+') as g:
                        for line2 in g:
                            name1 = line1.split('/')[-1][:-2]  
                            name2 = line2.split('/')[-1][:-2]
                            if name1 != name2:
                                pair = (name1, name2)  
                                csv_writer.writerow(pair)
                                return