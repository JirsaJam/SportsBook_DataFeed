#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 10:24:46 2020

@author: jamesjirsa
"""

import pandas as pd
import requests
import json
import datetime
import SQL
import numpy as np
import time

def draftkings(sleep = 1):   
    url = ('https://sportsbook.draftkings.com/api/odds/v1/leagues/3/offers/gamelines.json')
    response = requests.get(url)
    json_data = json.loads(response.text)
    games = json_data['events']
    df = pd.DataFrame()
    for i in range(0,len(games)):
        for j in range(0, len(games[i]['offers'])):
            df1 = pd.DataFrame.from_dict(games[i])
            df1 = df1.drop('offers', axis = 1)
            df1 = df1.drop_duplicates()
            df1 = df1.rename(columns = {'id': 'game_id'})
            if 'main' in games[i]['offers'][j].keys():
                if games[i]['offers'][j]['main'] == True:
                    game2 = games[i]['offers'][j]
                    df2 = pd.DataFrame.from_dict(games[i]['offers'][j])
                    df2 = df2.drop(['main', 'outcomes'], axis = 1)
                    df2 = df2.drop_duplicates()
                    df2 = df2.rename(columns = {'id': 'bet_id', 'label':'type'})
                    df1['temp1'] = i
                    df1['temp2'] = j
                    df2['temp1'] = i
                    df2['temp2'] = j
                    fin = pd.merge(df1,df2, on = ['temp1', 'temp2'])
                    for t in range(0,len(game2['outcomes'])):
                        dat = game2['outcomes'][t]
                        if len(df.columns) < 0:
                            abc = pd.DataFrame.from_dict([dat])
                            abc['temp1'] = i
                            abc['temp2'] = j
                            abc = pd.merge(fin, abc, on = ['temp1', 'temp2'])
                            abc = abc.drop(['temp1', 'temp2'], axis = 1)
                            df = abc
                            
                        else:
                            abc = pd.DataFrame.from_dict([dat])
                            abc['temp1'] = i
                            abc['temp2'] = j
                            abc = pd.merge(fin, abc, on = ['temp1', 'temp2'])
                            abc = abc.drop(['temp1', 'temp2'], axis = 1)
                            df = pd.concat([df,abc])
                else:
                    pass
            else:
                pass
    df['Book'] = 'DraftKings'        
    df['DateTime'] = datetime.datetime.now()
    df = df.drop(['oddsAmerican', 'oddsFractional', 'participant'], axis = 1)
    ipad = SQL.ipad()
    #sb = SQL.r_sql(ip = ipad, database = 'SportsBet', table = 'NFL_Lines')
    #df = df.compare(sb)
    SQL.w_sql(df,ip = ipad, server = 'SportsBet', database = 'NFL_Lines')
    print('DraftKings Finished')
    time.sleep(sleep)
