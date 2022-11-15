import mysql.connector
from mysql.connector import Error
import csv
import pwinput
data_folder = r".\data csv"
try:

    pwd = pwinput.pwinput("Password: ")
    db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = pwd
    )
    mycursor = db.cursor()
    mycursor.execute("CREATE DATABASE IF NOT EXISTS nba_fantasy_new;")
    mycursor.close()
    db.close()

    db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = pwd,
        database = "nba_fantasy_new"
    )
    mycursor = db.cursor()
    # readcsvc to db
    tables = ['week','biweek','month','player', 'injury','team','schedule', 'roster', 'season']
    for item in tables:
        csv_data = csv.reader(open(fr"{data_folder}\{item}.csv"))
        header = next(csv_data)
        mycursor.execute(f"DROP TABLE IF EXISTS {item}")
        if item == 'player':
            mycursor.execute(f"CREATE TABLE {item} (`index` int,`player` text,`tm` text)")
            for row in csv_data:
                mycursor.execute(f"INSERT INTO {item} (`index`,`Player`, `tm`) VALUES (%s, %s, %s)",row)

        elif item == 'season':
            mycursor.execute(f"CREATE TABLE {item} (`index` int,`rk` int,`Player` text,`pos` text,`age` int, `tm` text,`G` int,\
                `GS` int, `MP` double, `FG` double,`FGA` double, `FG%` text, `3P` double, `3PA` double,`3P%` text,`2P` double,`2PA` double,\
                    `2P%` text,`EFG%` text,`FT` double,`FTA` double,`FT%` text,`ORB` double,`DRB` double,`TRB` double,`AST` double,\
                        `STL` double,`BLK` double,`TOV` double,`PF` double,`PTS` double)")
            for row in csv_data:
                mycursor.execute(f"INSERT INTO {item} (`index`,`rk`,`Player`,`pos`,`age`, `tm`,`G`,`GS`, `MP`, `FG`,`FGA`, `FG%`, `3P`, `3PA`,`3P%`,`2P`,`2PA`, `2P%`,`EFG%`,`FT`,`FTA`,`FT%`,`ORB`,`DRB`,`TRB`,`AST`,`STL`,`BLK`,`TOV`,`PF`,`PTS`)\
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row)

        
        elif item == 'schedule':
            mycursor.execute(f"CREATE TABLE {item} (`index` int,`date` text,`start` text,`away` text,`ptsa` text,`home` text,`ptsh` text,\
                `bs` text,`ot` text,`attend` text,`arena` text,`notes` text)")
            for row in csv_data:
                mycursor.execute(f"INSERT INTO {item} (`index`,`date`,`start`,`away`,`ptsa`,`home`,`ptsh`,\
                `bs`,`ot`,`attend`,`arena`,`notes`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row)

        elif item == 'roster':
            mycursor.execute(f"CREATE TABLE {item} (`michael` text,`hao` text,`kiefer` text,`justin` text,`bryant` text,`chester` text,`danny` text,`oliver` text)")
            for row in csv_data:
                mycursor.execute(f"INSERT INTO {item} (`michael`,`hao`,`kiefer`,`justin`,`bryant`,`chester`,`danny`,`oliver`)\
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",row)

        elif item == 'team':
            mycursor.execute(f"CREATE TABLE {item} (`index` int,`tm` text,`teams` text)")
            for row in csv_data:
                mycursor.execute(f"INSERT INTO {item} (`index`,`tm`,`teams`) VALUES (%s,%s,%s)",row)

        elif item == 'injury':
            mycursor.execute(f"CREATE TABLE {item} (`index` int,`player` text,`pos` text,`status` text, `inj` text,`update` text, `updated` text)")
            for row in csv_data:
                mycursor.execute(f"INSERT INTO {item} (`index`,`player`,`pos`,`status`, `inj`,`update`, `updated`) VALUES (%s,%s,%s,%s,%s,%s,%s)",row)

        else:
            mycursor.execute(f"CREATE TABLE {item} (`index` int,`rk` int,`Player` text,`Tm` text,`G` int, `GS` int,`MP` double,\
            `FG` double,`FGA` double, `FG%` text, `3P` double, `3PA` double,`3P%` text, `FT` double,`FTA` double,`FT%` text,\
                `ORB` double,`DRB` double,`TRB` double,`AST` double,\
                    `STL` double,`BLK` double,`TOV` double,`PF` double,`PTS` double, `GmSc` double)")
            for row in csv_data:
                mycursor.execute(f"INSERT INTO {item} (`index`,`rk`,`Player`,`Tm`,`G`, `GS`,`MP`,\
                `FG`,`FGA`, `FG%`, `3P`, `3PA`,`3P%`, `FT`,`FTA`,`FT%`,\
                    `ORB`,`DRB`,`TRB`,`AST`,\
                        `STL`,`BLK`,`TOV`,`PF`,`PTS`, `GmSc`)\
                    VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row)

    queries = '''
    UPDATE season s, player p
    SET p.player = s.player
    WHERE s.player LIKE CONCAT(p.player, ' %');

    #change team name to abbreviation
    UPDATE schedule s, team t
    SET s.away = t.tm
    WHERE s.away = t.teams;

    UPDATE schedule s, team t
    SET s.home = t.tm
    WHERE s.home = t.teams;

    # format the date from string to date
    UPDATE schedule s
    SET date = SUBSTRING_INDEX(date, ",", -2);

    UPDATE schedule s
    SET date =  STR_TO_DATE(s.date, "%M %e, %Y");

    # this week's game schedule
    DROP TABLE IF EXISTS week_schedule;
    CREATE TABLE week_schedule
    SELECT
        Date,
        away,
        home
    FROM schedule s
    WHERE WEEK(Date,1) = WEEK(NOW(),1);

    #add number of games per week for each team in team table
    ALTER TABLE team
    ADD COLUMN games INT;
    UPDATE team t  
    SET games = (SELECT COUNT(*) FROM week_schedule WHERE away = t.tm OR home = t.tm);

    # create healthy player table
    DROP TABLE IF EXISTS healthy_players;
    CREATE TABLE healthy_players
    SELECT 
        p.player,
        p.tm
    FROM player p
    LEFT JOIN injury i
    ON p.player = i.player
    WHERE i.Status is NULL
        OR NOT i.status = "Out";
    '''
    list99 = queries.split(';')
    for q in list99:
        mycursor.execute(q.lstrip())

    wbm = ['week','biweek','month']
    stats =  ['week','biweek','month', 'season']
    roster =['michael','hao','justin','oliver','kiefer','bryant','danny','chester']
    for item in wbm:
        query0 = f'''
        ALTER TABLE {item}
        ADD COLUMN `2P` double;
        UPDATE {item}
        SET `2P` = `FG`-`3P`;

        ALTER TABLE {item}
        ADD COLUMN `2PA` double;
        UPDATE {item}
        SET `2PA` = `FGA`-`3PA`;

        ALTER TABLE {item}
        ADD COLUMN `2P%` double;
        UPDATE {item}
        SET `2P%` = IF (`2PA` <> 0, `2P`/`2PA`,0);
        '''
        list0 = query0.split(';')
        for q in list0:
            mycursor.execute(q.lstrip())

    for item in stats:
        query1 = f'''
        #change teams to correct abbreviations
        UPDATE {item}
        SET  Tm = (CASE WHEN Tm = 'PHO' THEN 'PHX'
                        WHEN Tm = 'CHO' THEN 'CHA'
                        WHEN Tm = 'BRK' THEN 'BKN'
                        ELSE Tm
                        END);
        
        #gets rid of letters with accent and keep what ever is infront
        UPDATE {item}
        SET Player = SUBSTRING_INDEX(Player, "?", 1)
        WHERE Player LIKE '%?%';

        #replace those names with ? to actaul name without accent
        UPDATE 
            {item} s,
            player p
        SET s.Player = p.player
        WHERE p.player LIKE CONCAT(s.Player, '%');

        #add number of games each player plays each week to season table
        ALTER TABLE {item}
        ADD COLUMN games INT;
        UPDATE {item} s
        LEFT JOIN team t
                    ON s.tm = t.tm
        SET s.games = t.games;
        
        # create available player list
        DROP TABLE IF EXISTS available_player_{item};
        CREATE TABLE available_player_{item}
        SELECT 
            Player,
            Tm,
            games,
            `2P` AS fgm,
            `2PA` AS fga,
            `2P%` AS fgp,
            FT AS ftm,
            FTA AS fta,
            `FT%` AS ftp,
            `3P` AS tpm,
            `3PA` AS tpa,
            `3p%` AS tpp,
            PTS AS pts,
            TRB AS reb,
            AST AS ast,
            STL AS st,
            BLK AS blk,
            TOV AS tov,
            PF AS pf
        FROM {item}
        WHERE Player NOT IN
        (SELECT r.michael AS player FROM roster r
        UNION
        SELECT r.hao FROM roster r
        UNION
        SELECT r.kiefer FROM roster r
        UNION
        SELECT r.danny FROM roster r
        UNION
        SELECT r.bryant FROM roster r
        UNION
        SELECT r.chester FROM roster r
        UNION
        SELECT r.justin FROM roster r
        UNION
        SELECT r.Oliver FROM roster r);

        # projected total stats based on season average
        DROP TABLE IF EXISTS proj_{item};
        CREATE TABLE proj_{item}
        SELECT
            p.Player,
            w.Tm,
            w.games,
            ROUND(w.`2P` * w.games, 1) AS fgm,
            ROUND(w.`2PA` * w.games, 1) AS fga,
            w.`2P%` AS fgp,
            ROUND(w.FT * w.games,1) AS ftm,
            ROUND(w.FTA * w.games,1) AS fta,
            w.`FT%` AS ftp,
            ROUND(w.`3P` * w.games,1) AS tpm,
            ROUND(w.`3PA` * w.games,1) AS tpa,
            w.`3P%` AS tpp,
            ROUND(w.PTS * w.games,1) AS pts,
            ROUND(w.TRB * w.games,1) AS reb,
            ROUND(w.AST * w.games,1) AS ast,
            ROUND(w.STL * w.games,1) AS st,
            ROUND(w.BLK * w.games,1) AS blk,
            ROUND(w.TOV * w.games,1) AS tov,
            ROUND(w.PF * w.games,1) AS pf
        FROM {item} w
        RIGHT JOIN player p
            ON w.player = p.player;
        '''
        list1 = query1.split(';')
        for q in list1:
            mycursor.execute(q.lstrip())

        for name in roster:
            query2 = f'''
            DROP TABLE IF EXISTS {name}_{item};
            CREATE TABLE {name}_{item}
            SELECT *
            FROM proj_{item}
            WHERE Player IN (SELECT r.{name} FROM roster r);
            '''

            list2 = query2.split(';')
            for q in list2:
                mycursor.execute(q.lstrip())



    db.commit()
    mycursor.close()
    db.close()

except Error as e:
    print("Error while connecting to MySQL", e)
