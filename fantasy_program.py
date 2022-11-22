#Run fantasy_data first, then run fantasy_csv_to_sql before running this file
#Run this program to check roster, compare stats, or browse free agents with projectedn stats for the week
import mysql.connector
from mysql.connector import Error
import pandas as pd
import pwinput
import os

try:
    db = mysql.connector.connect(
            host = "localhost",
            user = "root",
            passwd = pwinput.pwinput("Password: ") ,
            database = "nba_fantasy_new"
        )
    mycursor = db.cursor()
    print("--------------------------------------------------------------------")

    folder_path = os.path.join(r".", r"data folder")
    path = os.path.join(folder_path,r"roster.csv")
    roster = pd.read_csv(path)
    roster = list(roster.columns)
    while True:
        print('Press ENTER to go exit')
        mode = input('select mode: roster, compare, free agents: ')
        print("--------------------------------------------------------------------")
        if mode == 'compare':
            while True:
                print('Press ENTER to go back')
                stats = input("project stats based on: week, biweek, month, or season? ")
                print("--------------------------------------------------------------------")
                if stats in ['week','biweek', 'month','season']:
                    team1 = 'justin'
                    while True:
                        print('Press ENTER to go back')
                        team2 = input("opponent: ")
                        print("--------------------------------------------------------------------")
                        if team2 in roster:
                            if stats == 'season':
                                pass
                            else:
                                query1 =f'''
                                #create table for each team with projected stats
                                DROP TABLE IF EXISTS {team1};
                                CREATE TABLE {team1} 
                                SELECT *
                                FROM proj_{stats}
                                WHERE Player IN (SELECT r.{team1} FROM roster r);

                                #create table for each team with projected stats
                                DROP TABLE IF EXISTS {team2};
                                CREATE TABLE {team2}
                                SELECT *
                                FROM proj_{stats}
                                WHERE Player IN (SELECT r.{team2} FROM roster r);
                                '''
                            
                                list = query1.split(';')
                                for q in list:
                                    mycursor.execute(q.lstrip())

                            query2 = f'''
                                # my weekly projection
                                SELECT 
                                    'my team' as team,
                                    SUM(games) AS games,
                                    ROUND(SUM(fgm), 1) AS fgm,
                                    ROUND(SUM(fgm)/SUM(fga), 3) AS fgp,
                                    ROUND(SUM(ftm),1) AS ftm,
                                    ROUND(SUM(ftm)/SUM(fta),3) AS ftp,
                                    ROUND(SUM(tpm), 1) AS tpm,
                                    ROUND(SUM(tpm)/SUM(tpa),3) AS tpp,
                                    ROUND(SUM(pts), 1) AS pts,
                                    ROUND(SUM(reb), 1) AS reb,
                                    ROUND(SUM(ast), 1) AS ast,
                                    ROUND(SUM(st), 1) AS st,
                                    ROUND(SUM(blk), 1) AS blk,
                                    ROUND(SUM(tov), 1) AS tov,
                                    ROUND(SUM(pf), 1) AS pf
                                FROM {team1}_{stats} h,healthy_players hp
                                WHERE h.Player = hp.player
                                UNION
                                #opponent's wekly projection
                                SELECT 
                                    '{team2}' AS team,
                                    SUM(games) AS games,
                                    ROUND(SUM(fgm), 1) AS fgm,
                                    ROUND(SUM(fgm)/SUM(fga), 3) AS fgp,
                                    ROUND(SUM(ftm),1) AS ftm,
                                    ROUND(SUM(ftm)/SUM(fta),3) AS ftp,
                                    ROUND(SUM(tpm), 1) AS tpm,
                                    ROUND(SUM(tpm)/SUM(tpa),3) AS tpp,
                                    ROUND(SUM(pts), 1) AS pts,
                                    ROUND(SUM(reb), 1) AS reb,
                                    ROUND(SUM(ast), 1) AS ast,
                                    ROUND(SUM(st), 1) AS st,
                                    ROUND(SUM(blk), 1) AS blk,
                                    ROUND(SUM(tov), 1) AS tov,
                                    ROUND(SUM(pf), 1) AS pf
                                FROM {team2}_{stats} b, healthy_players hp
                                WHERE b.Player = hp.player;
                                '''
                            mycursor.execute(query2)
                            result = mycursor.fetchall()
                            list =[]
                            for row in result:
                                list.append(row)

                            output = pd.DataFrame(list)
                            output.columns= mycursor.column_names
                            print(output)
                            print("--------------------------------------------------------------------")
                            break
                        elif team2 == "":
                            break
                        else:
                            print('Enter a valid team!')
                            continue

                elif stats=="":
                    break
                else:
                    print('Enter a valid value!')
                    continue
            

        elif mode== 'roster':
            while True:
                print('Press ENTER to go back')
                stats = input("project stats based on: week, biweek, month, or season? ")
                print("--------------------------------------------------------------------")
                if stats in ['week','biweek', 'month','season']:
                    while True:
                        print('Press ENTER to go back')
                        team = input('which team? ')
                        print("--------------------------------------------------------------------")
                        if team in roster:
                            query = f'''
                            SELECT *
                            FROM {team}_{stats}
                            '''
                            mycursor.execute(query)
                            result = mycursor.fetchall()
                            list =[]
                            for row in result:
                                list.append(row)

                            output = pd.DataFrame(list)
                            output.columns= mycursor.column_names
                            print(output)
                            print("--------------------------------------------------------------------")
                            break
                        elif team =="":
                            break
                        else:
                            print('Enter a valid team!')
                            continue
                elif stats=="":
                    break
                else:
                    print('Enter a valid value!')
                    continue
            

        elif mode == 'free agents':
            while True:
                print('Press ENTER to go back')
                stats = input("project stats based on: week, biweek, month, or season? ")
                print("--------------------------------------------------------------------")
                if stats in ['week','biweek', 'month','season']:
                    print('Press ENTER to go back')
                    stat = input('Order by which stat? ')
                    print("--------------------------------------------------------------------")

                    try:
                        query = f'''
                        SELECT *
                        FROM available_player_{stats}
                        ORDER BY {stat} DESC;
                        '''
                        #mycursor.execute(query1)
                        mycursor.execute(query)
                        result = mycursor.fetchall()
                        list =[]
                        for row in result:
                            list.append(row)

                        output = pd.DataFrame(list)
                        output.columns= mycursor.column_names
                        print(output.head(20))
                        print("--------------------------------------------------------------------")
                        continue
                    except:
                        print('Not a category!')
                        continue
                    
                elif stats=="":
                    break
                else:
                    print("Please enter valid value!")
                    continue

        elif mode == "":
            break
        else:
            print("Please enter valid value!")
            continue

    mycursor.close()
    db.close()

except Error as e:
    print("Error while connecting to MySQL", e)
