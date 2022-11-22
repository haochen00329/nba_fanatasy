import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
data_folder = os.path.join(r".", r"data folder")
if not os.path.exists(data_folder):
    os.mkdir(data_folder)
folder_path = os.path.join(r".", r"data folder")
# df for week, biweek,month,season team
list = [['week', 'https://www.basketball-reference.com/friv/last_n_days.fcgi?n=7&type=per_game'],['biweek','https://www.basketball-reference.com/friv/last_n_days.fcgi?n=15&type=per_game'],\
    ['month','https://www.basketball-reference.com/friv/last_n_days.fcgi?n=30&type=per_game'],['season','https://www.basketball-reference.com/leagues/NBA_2023_per_game.html'],\
        ['team','https://en.wikipedia.org/wiki/Wikipedia:WikiProject_National_Basketball_Association/National_Basketball_Association_team_abbreviations'],['player','https://www.fantasypros.com/nba/stats/avg-overall.php']]
for item in list:
    url = item[1]
    html = requests.get(url).content
    df = pd.read_html(html)[-1]
    if item[0] == 'team':
        df = df[1:]
    elif item[0] == 'player':
        df['Team'] = df['Player'].str.split('(').str[1].str.split(' ').str[0]
        df['Player'] = df['Player'].str.split(' ').str[0] + ' ' + df['Player'].str.split(' ').str[1]
        df = df[['Player','Team']]
    else:
        df['Player']=[re.sub(r"[^a-zA-Z\-\.\s\']",'?', x) for x in df['Player']]
        df = df[df.Tm != "Tm"]
    path = os.path.join(folder_path,fr"{item[0]}.csv")
    df.to_csv(path)

# injury df
header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'}
injury_url = f'https://www.actionnetwork.com/nba/injury-report'
injury_page = requests.get(injury_url,headers=header).text
injury_doc = BeautifulSoup(injury_page, 'html.parser')

injury_table=injury_doc.find(class_="table-layout__table injuries-table-layout")
injury_table = injury_table.find_all('tr')
injury_list = []

for row in injury_table:
    value= row.find_all('td')
    if len(value) == 6:
        name = value[0].string
        pos = value[1].string
        status = value[2].string
        injury = value[3].string
        update = value[4].string
        updated = value[5].string

        injured_player = {"name":name,
                            "pos":pos,
                            "status":status,
                            "injury":injury,
                            "update":update,
                            "updated": updated}
        injury_list.append(injured_player)
injury_df = pd.DataFrame(injury_list)
injury_df = injury_df.dropna()
injury_path = os.path.join(folder_path,r"injury.csv")
injury_df.to_csv(injury_path)



# full schedule df
months = ['october','november','december','january','february','march','april']
schedule_df = pd.DataFrame([])
for i in months:
    url = f'https://www.basketball-reference.com/leagues/NBA_2023_games-{i}.html'
    html = requests.get(url).content
    df = pd.read_html(html)[-1]
    schedule_df = pd.concat([schedule_df,df])

schedule_path = os.path.join(folder_path,fr"schedule.csv")
schedule_df.to_csv(schedule_path)



