#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 10:24:46 2020

@author: jamesjirsa
"""
#need to add database feed

import pandas as pd
import requests
import json
import datetime
import numpy as np
import time


def points_bet(sleep = 1):
    url = ('https://api.il.pointsbet.com/api/v2/competitions/2/events/featured?includeLive=true')
    response = requests.get(url)
    json_data = json.loads(response.text)
    games = json_data['events']
    df = pd.DataFrame()
    for i in range(0,len(games)):
        xtra = games[i]
        del xtra['numberOfSpreadMarkets']
        del xtra['fixedOddsMarkets']
        del xtra['markets']
        del xtra['score']
        del xtra['tags']
        del xtra['eventInfo']
        del xtra['vision']
        df1 = pd.DataFrame.from_dict(xtra)
        df1 = df1.drop('specialFixedOddsMarkets', axis = 1)
        df1 = df1[['timestamp','name', 'key', 'homeTeam', 'awayTeam', 'startsAt']]
        df1 = df1.drop_duplicates()
        for j in range(0, len(games[i]['specialFixedOddsMarkets'])):
            game2 = games[i]['specialFixedOddsMarkets'][j]
            del game2['tags']
            del game2['gameLineMarketName']
            df2 = pd.DataFrame.from_dict(game2)
            df2 = df2.drop_duplicates(subset = ['key'])
            df2 = df2[['key', 'eventClass']]
            df1['temp1'] = i
            df1['temp2'] = j
            df2['temp1'] = i
            df2['temp2'] = j
            fin = pd.merge(df1,df2, on = ['temp1', 'temp2'])
            for k in range(0, len(games[i]['specialFixedOddsMarkets'][j]['outcomes'])):
                game3 = games[i]['specialFixedOddsMarkets'][j]['outcomes'][k]
                df3 = pd.DataFrame.from_dict([game3])
                df3 = df3[['name', 'points', 'price', 'fixedMarketId']]
                df3 = df3.drop_duplicates()
                if len(df.columns) < 0:
                    df3['temp1'] = i
                    df3['temp2'] = j
                    abc = pd.merge(fin, df3, on = ['temp1', 'temp2'])
                    abc = abc.drop(['temp1', 'temp2'], axis = 1)
                    abc = abc.rename(columns = {'key_x' : 'game_id', 'name_x': 'name',
                              'homeTeam': 'homeTeamName', 'awayTeam': 'awayTeamName',
                              'key_y': 'bet_id', 'fixedMarketId': 'id',
                              'eventClass': 'type', 'points': 'line',
                              'price': 'oddsDecimal', 'name_y': 'label'})
                    df = abc
                                
                else:
                    df3['temp1'] = i
                    df3['temp2'] = j
                    abc = pd.merge(fin, df3, on = ['temp1', 'temp2'])
                    abc = abc.drop(['temp1', 'temp2'], axis = 1)
                    abc = abc.rename(columns = {'key_x' : 'game_id', 'name_x': 'name',
                                              'homeTeam': 'homeTeamName', 'awayTeam': 'awayTeamName',
                                              'key_y': 'bet_id', 'fixedMarketId': 'id',
                                              'eventClass': 'type', 'points': 'line',
                                              'price': 'oddsDecimal', 'name_y': 'label', 'startsAt': 'startDate'})
                    df = pd.concat([df,abc])
    df['Book'] = 'PointsBet'                
    df['DateTime'] = datetime.datetime.now()
    df = df[['game_id' ,'name', 'homeTeamName', 'awayTeamName','startDate', 'bet_id', 'type',
             'id', 'label', 'line', 'oddsDecimal', 'Book', 'DateTime']]
    ipad = SQL.ipad()
    #sb = SQL.r_sql(ip = ipad, database = 'SportsBet', table = 'NFL_Lines')
    #df = df.compare(sb)
    # add datafeed here
    print('PointsBet Finished')
    time.sleep(sleep)


