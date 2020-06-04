import json
from newsapi import NewsApiClient
from datetime import date, timedelta
import os
import csv
import sys
import json

variablefile = open('/Users/psehgal/dev/Sentiment.Analysis/preeti_final/sentiment_program_variables_ps.json')
df_var = json.load(variablefile)

if df_var["runnewsapiflag"] == 'N':
	sys.exit()
# this will avoid this api to run everytime we test our data pipeline


# Init
newsapi = NewsApiClient(api_key=df_var["newsapikey"])


# Delete CSV to overwrite
if os.path.exists('news.csv'):
	os.remove('news.csv')


# Create new CSV with headers
with open('news.csv', 'w',newline='') as f:
	w = csv.writer(f)
	w.writerow(['date', 'total_articles'])


# Obtain article count per day for date range
#start = '2020-04-14'
#end = '2020-05-14'
start = df_var["newsapistartdate"]
end = df_var["newsapienddate"]

start2 = date(int(start[0:4]), int(start[5:7]), int(start[8:10]))
end2 = date(int(end[0:4]), int(end[5:7]), int(end[8:10]))
day = timedelta(days=1)

i = start2
while i <= end2:

	all_articles = newsapi.get_everything(q='Tesla',
										  from_param=i,
										  to=i,
										  language='en',
										  )

	with open('news.csv', 'a') as f:
		f.write("{},{}\n".format(i.strftime('%Y-%m-%d'), all_articles["totalResults"]))

	i += day

