#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 17 15:57:41 2020

@author: jamesjirsa
"""


import pandas as pd
import requests
import json
import datetime
import SQL
import numpy as np
import time



start_int = 10337110
url = (f'https://fcast.espncdn.com/FastcastService/pubsub/profiles/12000/topic/scoreboard-football-college-football/message/{start_int}/checkpoint')
response = requests.get(url)
json_data = json.loads(response.text)
games = json_data['events']

df = pd.DataFrame()
for i in range(0,len(games)):
    xtra = games[i]
    del xtra['uid']
    del xtra['shortName']
    del xtra['season']
    del xtra['links']
    #del xtra['weather']
    #del xtra['status']
    #del xtra['competitions']
    df1 = pd.DataFrame.from_dict([xtra])
    df1['temp'] = i
    df1 = df1.drop('competitions', axis = 1)
    df3 = games[i]['competitions'][0]
    for j in range(0,len(games[i]['competitions'][0]['competitors'])):
        aaa = games[i]['competitions'][0]['competitors'][j]
        if aaa['homeAway'] == 'home':
            ccc = aaa
            bbb = aaa['team']
            bbb = bbb['displayName']
            #del ccc['linescores']
            del ccc['records']
            del ccc['statistics']
            del ccc['team']
            ccc = pd.DataFrame.from_dict([ccc])
            ccc = ccc[['id','score']]
            ccc.columns = ['home_id','home_score']
            ccc['home_team'] = bbb
            home = ccc
            home['temp'] = i
        else:
            ccc = aaa
            bbb = aaa['team']
            bbb = bbb['displayName']
            #del ccc['linescores']
            del ccc['records']
            del ccc['statistics']
            del ccc['team']
            ccc = pd.DataFrame.from_dict([ccc])
            ccc = ccc[['id','score']]
            ccc.columns = ['away_id','away_score']
            ccc['away_team'] = bbb
            away = ccc
            away['temp'] = i
        
    df1 = pd.merge(df1,home, on = 'temp')
    df1 = pd.merge(df1,away, on = 'temp')
        
    df3 = pd.DataFrame.from_dict([df3])
    df4 = games[i]['status']
    df4 = pd.DataFrame.from_dict([df4])
    df4 = df4.drop('type', axis = 1)
    df4['temp'] = i
    df5 = games[i]['status']['type']
    df5 = pd.DataFrame.from_dict([df5])
    df5['temp'] = i
    df1 = pd.merge(df1,df4, on = 'temp')
    df1 = pd.merge(df1,df5, on = 'temp')
    df1 = df1.drop('status', axis = 1)
    
    if 'situation' in df3.columns:
        xtra2 = games[i]['competitions'][0]['situation']
        del xtra2['lastPlay']
        df2 = pd.DataFrame.from_dict([xtra2])
        df2['temp'] = i
        fin = pd.merge(df1,df2, on = 'temp')
        fin = fin.drop('temp', axis = 1)
        if len(df.columns) < 1:
            df = fin
        else:
            df = pd.concat([df,fin])
    else:
        if len(df.columns) < 1:
            fin = df1
            fin = fin.drop('temp', axis = 1)
            df = fin
        else:
            fin = df1
            fin = fin.drop('temp', axis = 1)
            df = pd.concat([df,fin])

