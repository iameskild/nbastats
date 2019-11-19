import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

url = 'https://www.basketball-reference.com/leagues/NBA_2019_totals.html'

response = requests.get(url)
content = response.content

parser = BeautifulSoup(content, 'html.parser')

page_title = parser.title.text
# table = parser.find('table', {'id' : 'totals_stats'})
table = parser.find('table', id='totals_stats')
table_header = table.find('thead')
table_body = table.find('tbody')

header = table_header.find_all('tr')[0].text
# create list of column names
# and remove two leading columns and trailing column
header_cols = header.split('\n')[2:-1]

# include all rows for players who played on multiple teams during the season
# and exclude reappearing header rows
rows = table_body.find_all('tr', class_=['full_table', 'partial_table'])

# player_stats = pd.DateFrame()
player_stats = []
for r in rows[:3]:
    stats = r.find_all('td')
    stats_list = []
    for s in stats:
        stats_list.append(s.text)
    player_stats.append(stats_list)
    # print(len(player_stats[0]))

# print(header_cols)

df = pd.DataFrame(player_stats, columns=header_cols)
print(df)

# stats = rows[0].find_all('td')
# print(stats[3].text)
# for r in rows[:2]:
#     print(r.text)

# print(table_body)
