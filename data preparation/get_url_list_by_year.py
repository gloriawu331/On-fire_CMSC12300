import bs4
import requests
import csv


def get_file_by_year(start_year, end_year):
	lst = []

	for i in range(start_year, end_year +1):
		url = 'https://www.ncei.noaa.gov/data/global-hourly/access/{}/'.format(i)
		r = requests.get(url)
		soup = bs4.BeautifulSoup(r.text, 'lxml')
		tag_list = soup.find_all('a')
		for tag in tag_list[4:]:
			if tag.get('href') != '/data/global-hourly/access/':
				csv_name = tag.get('href')
				url_full_name = url + csv_name
				lst.append(url_full_name)

	return lst



def main():
	lst = get_file_by_year(2006, 2016)
	out = csv.writer(open("myfile1.csv","w"), delimiter=',',quoting=csv.QUOTE_ALL)
	for i in lst:
		out.writerow([i])


if __name__== "__main__":
  main()

