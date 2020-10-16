#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  9 15:44:49 2020

@author: jamesjirsa
"""


import pandas as pd
import requests
import json
import datetime
import SQL
import numpy as np
import time


def bet_rivers(sleep = 1):
    url = ('https://il.betrivers.com/api/service/sportsbook/offering/feed?key=f9907e79-4295-4f1c-b233-e01d9eb2d480')
    response = requests.get(url)
    json_data = json.loads(response.text)
    games = json_data['events']
    df = pd.DataFrame()
    for i in range(0,len(games)):
        games4 = pd.DataFrame()
        for l in range(0,len(games[i]['participants'])):
            ha = games[i]['participants'][l]
            games5 = pd.DataFrame.from_dict([ha])
            games5['label'] = l
            if len(games4.columns) < 1:
                games4 = games5
            else:
                games4 = pd.concat([games4,games5])
        games4['label'] = games4['label'] + 1
        games4 = games4.drop('id', axis = 1)
        games6 = games4.T
        games6.columns = ['home','away']
        games6 = games6.drop('label')
        games6['temp1'] = i
    
                 
        xtra = games[i]   
        del xtra['format']
        del xtra['state']
        del xtra['webStreaming']
        del xtra['mobileStreaming']
        del xtra['eventInfo']
        del xtra['participants']
        #del xtra['betOffers']
        df1 = pd.DataFrame.from_dict([xtra])
        df1 = df1.drop('betOffers', axis = 1)
        df1 = df1[['name', 'id', 'start']]
        df1 = df1.drop_duplicates()
        for j in range(0, len(games[i]['betOffers'])):
            game2 = games[i]['betOffers'][j]
            # del game2['tags']
            # del game2['gameLineMarketName']
            df2 = pd.DataFrame.from_dict(game2)
            df2 = df2.drop_duplicates(subset = ['id'])
            df2 = df2[['id', 'betDescription']]
            df1['temp1'] = i
            df1['temp2'] = j
            df2['temp1'] = i
            df2['temp2'] = j
            fin = pd.merge(df1,df2, on = ['temp1', 'temp2'])
            fin = pd.merge(fin,games6, on = ['temp1'])
            for k in range(0, len(games[i]['betOffers'][j]['outcomes'])):
                game3 = games[i]['betOffers'][j]['outcomes'][k]
                df3 = pd.DataFrame.from_dict([game3])
                if 'line' in df3.columns:
                    df3 = df3[['label', 'line', 'odds', 'id']]
                    df3 = df3.drop_duplicates()
                    if len(df.columns) < 1:
                        df3['temp1'] = i
                        df3['temp2'] = j
                        abc = pd.merge(fin, df3, on = ['temp1', 'temp2'])
                        abc = abc.drop(['temp1', 'temp2'], axis = 1)
                        abc = abc.rename(columns = {'id_x' : 'game_id', 'name': 'name',
                                   'home': 'homeTeamName', 'away': 'awayTeamName',
                                   'id_y': 'bet_id', 'id': 'id',
                                   'betDescription': 'type', 'line': 'line',
                                   'odds': 'oddsDecimal', 'label': 'label', 'start': 'startDate'})
                        df = abc
                                    
                    else:
                        df3['temp1'] = i
                        df3['temp2'] = j
                        abc = pd.merge(fin, df3, on = ['temp1', 'temp2'])
                        abc = abc.drop(['temp1', 'temp2'], axis = 1)
                        abc = abc.rename(columns = {'id_x' : 'game_id', 'name': 'name',
                                   'home': 'homeTeamName', 'away': 'awayTeamName',
                                   'id_y': 'bet_id', 'id': 'id',
                                   'betDescription': 'type', 'line': 'line',
                                   'odds': 'oddsDecimal', 'label': 'label', 'start': 'startDate'})
                        df = pd.concat([df,abc])
                else:
                    df3 = df3[['label', 'odds', 'id']]
                    df3['label'] = df3['label'].astype(int)
                    df3 = pd.merge(df3, games4, on = 'label', how = 'left')
                    df3 = df3.drop('label', axis = 1)
                    df3 = df3.rename(columns = {'name':'label'})
                    df3 = df3.drop_duplicates()
                    if len(df.columns) < 1:
                        df3['temp1'] = i
                        df3['temp2'] = j
                        abc = pd.merge(fin, df3, on = ['temp1', 'temp2'])
                        abc = abc.drop(['temp1', 'temp2'], axis = 1)
                        abc = abc.rename(columns = {'id_x' : 'game_id', 'name': 'name',
                                   'home': 'homeTeamName', 'away': 'awayTeamName',
                                   'id_y': 'bet_id', 'id': 'id',
                                   'betDescription': 'type', 'line': 'line',
                                   'odds': 'oddsDecimal', 'label': 'label', 'start': 'startDate'})
                        df = abc
                                    
                    else:
                        df3['temp1'] = i
                        df3['temp2'] = j
                        abc = pd.merge(fin, df3, on = ['temp1', 'temp2'])
                        abc = abc.drop(['temp1', 'temp2'], axis = 1)
                        abc = abc.rename(columns = {'id_x' : 'game_id', 'name': 'name',
                                   'home': 'homeTeamName', 'away': 'awayTeamName',
                                   'id_y': 'bet_id', 'id': 'id',
                                   'betDescription': 'type', 'line': 'line',
                                   'odds': 'oddsDecimal', 'label': 'label', 'start': 'startDate'})
                        df = pd.concat([df,abc])
    
    
    
    
    
    df['Book'] = 'BetRivers'                
    df['DateTime'] = datetime.datetime.now()
    df = df[['game_id' ,'name', 'homeTeamName', 'awayTeamName','startDate', 'bet_id', 'type',
            'id', 'label', 'line', 'oddsDecimal', 'Book', 'DateTime']]
    ipad = SQL.ipad()
    # sb = SQL.r_sql(ip = ipad, database = 'SportsBet', table = 'NFL_Lines')
    # df = df.compare(sb)
    SQL.w_sql(df,ip = ipad, server = 'SportsBet', database = 'NFL_Lines')
    print('BetRivers Finished')
    time.sleep(sleep)
