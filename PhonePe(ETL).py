import pandas as pd
import json
import os
import plotly.express as px
import requests
import subprocess
import plotly.graph_objects as go
from IPython.display import display
import psycopg2

#---------------------------------------------------AGGREGATE TRANSACTION-----------------------------------------------------------------

path1 = "data/aggregated/transaction/country/india/state"
agg_trans_list = os.listdir(path1)
column1 = {'State':[], 'Year':[], 'Quarter':[], 'Transaction_Type':[], 'Transaction_Count':[], 'Transaction_Amount':[]}
for i in agg_trans_list:
  p_i = path1+"/"+i
  agg_year = os.listdir(p_i)
  for j in agg_year:
    p_j = p_i+"/"+j
    agg_year_list = os.listdir(p_j)
    for k in agg_year_list:
     p_k = p_j+"/"+k
     Data1 = open (p_k,'r')
     A = json.load(Data1)
     for l in A['data']['transactionData']:
      Name = l['name']
      Count = l['paymentInstruments'][0]['count']
      Amount = l['paymentInstruments'][0]['amount']
      column1['Transaction_Type'].append(Name)
      column1['Transaction_Count'].append(Count)
      column1['Transaction_Amount'].append(Amount)
      column1['State'].append(i)
      column1['Year'].append(j)
      column1['Quarter'].append(int(k.strip('.json')))
agg_trans = pd.DataFrame(column1)

#-----------------------------------------------------AGGREGATE USER-----------------------------------------------------------------------

path2 = "data/aggregated/user/country/india/state"
agg_user_list = os.listdir(path2)
column2 = {'State':[], 'Year':[], 'Quarter':[], 'Mobile_Brand':[], 'Transaction_Count':[], 'Percentage':[]}
for i in agg_user_list:
  p_i = path2+"/"+i
  agg_year = os.listdir(p_i)
  for j in agg_year:
    p_j = p_i+"/"+j
    agg_year_list = os.listdir(p_j)
    for k in agg_year_list:
     p_k = os.path.join(p_j,k)
     if os.path.isfile(p_k):
      with open(p_k,'r') as Data2:
        B = json.load(Data2)
        try:
          for l in B['data']['usersByDevice']:
            Brand = l['brand']
            Count = l['count']
            Percentage = l['percentage']
            column2['Mobile_Brand'].append(Brand)
            column2['Transaction_Count'].append(Count)
            column2['Percentage'].append(Percentage)
            column2['State'].append(i)
            column2['Year'].append(j)
            column2['Quarter'].append(int(k.strip('.json')))
        except:
          pass
agg_user = pd.DataFrame(column2)

#------------------------------------------------------MAP TRANSACTION---------------------------------------------------------------------

path3 = "data/map/transaction/hover/country/india/state"
map_trans_list = os.listdir(path3)
column3 = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Transaction_Count':[], 'Transaction_Amount':[]}
for i in map_trans_list:
  p_i = path3+"/"+i
  map_year = os.listdir(p_i)
  for j in map_year:
    p_j = p_i+"/"+j
    map_year_list = os.listdir(p_j)
    for k in agg_year_list:
     p_k = p_j+"/"+k
     Data3 = open (p_k,'r')
     C = json.load(Data3)
     for l in C['data']['hoverDataList']:
      District = l['name']
      Count = l['metric'][0]['count']
      Amount = l['metric'][0]['amount']
      column3['District'].append(District)
      column3['Transaction_Count'].append(Count)
      column3['Transaction_Amount'].append(Amount)
      column3['State'].append(i)
      column3['Year'].append(j)
      column3['Quarter'].append(int(k.strip('.json')))
map_trans = pd.DataFrame(column3)

#----------------------------------------------------MAP USER-----------------------------------------------------------------------------

path4 = "data/map/user/hover/country/india/state"
map_user_list = os.listdir(path4)
column4 = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Registered_Users':[] , 'Apps_Opened':[]}
for i in map_user_list:
  p_i = path4+"/"+i
  map_year = os.listdir(p_i)
  for j in map_year:
    p_j = p_i+"/"+j
    map_year_list = os.listdir(p_j)
    for k in agg_year_list:
     p_k = p_j+"/"+k
     Data4 = open (p_k,'r')
     D = json.load(Data4)
     for l in D['data']['hoverData'].items():
      District = l[0]
      Registered_Users = l[1]['registeredUsers']
      Apps_Opened = l[1]['appOpens']
      column4['District'].append(District)
      column4['Registered_Users'].append(Registered_Users)
      column4['Apps_Opened'].append(Apps_Opened)
      column4['State'].append(i)
      column4['Year'].append(j)
      column4['Quarter'].append(int(k.strip('.json')))
map_user = pd.DataFrame(column4)

#-------------------------------------------------------TOP TRANSACTION-------------------------------------------------------------------

path5 = "data/top/transaction/country/india/state"
top_trans_list = os.listdir(path5)
column5 = {'State':[], 'Year':[], 'Quarter':[], 'PinCode':[], 'Transaction_Count':[], 'Transaction_Amount':[]}
for i in top_trans_list:
  p_i = path5+"/"+i
  map_year = os.listdir(p_i)
  for j in map_year:
    p_j = p_i+"/"+j
    map_year_list = os.listdir(p_j)
    for k in agg_year_list:
     p_k = p_j+"/"+k
     Data5 = open (p_k,'r')
     E = json.load(Data5)
     for l in E['data']['pincodes']:
      PinCode = l['entityName']
      Count = l['metric']['count']
      Amount = l['metric']['amount']
      column5['PinCode'].append(PinCode)
      column5['Transaction_Count'].append(Count)
      column5['Transaction_Amount'].append(Amount)
      column5['State'].append(i)
      column5['Year'].append(j)
      column5['Quarter'].append(int(k.strip('.json')))
top_trans = pd.DataFrame(column5)

#-----------------------------------------------------------TOP USER-----------------------------------------------------------------------

path6 = "data/top/user/country/india/state"
top_user_list = os.listdir(path6)
column6 = {'State':[], 'Year':[], 'Quarter':[], 'PinCode':[], 'Registered_Users':[]}
for i in top_user_list:
  p_i = path6+"/"+i
  map_year = os.listdir(p_i)
  for j in map_year:
    p_j = p_i+"/"+j
    map_year_list = os.listdir(p_j)
    for k in agg_year_list:
     p_k = p_j+"/"+k
     Data6 = open (p_k,'r')
     F = json.load(Data6)
     for l in F['data']['pincodes']:
      PinCode = l['name']
      Registered_Users = l['registeredUsers']
      column6['PinCode'].append(PinCode)
      column6['Registered_Users'].append(Registered_Users)
      column6['State'].append(i)
      column6['Year'].append(j)
      column6['Quarter'].append(int(k.strip('.json')))
top_user = pd.DataFrame(column6)

#-------------------------------------------------PSYCOPG CONNECTION-----------------------------------------------------------------------

projectB = psycopg2.connect(host='host',user='userId',password='password',database='DataBaseName')
cursor = projectB.cursor()

projectB.rollback()

cursor.execute('''create table if not exists Aggregate_Transaction(State varchar(50),
           Year int, 
           Quarter int, 
           Transaction_Type varchar(50),
           Transaction_Count bigint,
           Transaction_Amount bigint)'''
           )
projectB.commit()

for _, row in agg_trans.iterrows():
            insert_query = '''
                INSERT INTO Aggregate_Transaction (State, Year, Quarter, Transaction_Type, Transaction_Count, Transaction_Amount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['Transaction_Type'],
                row['Transaction_Count'],
                row['Transaction_Amount']
            )
            cursor.execute(insert_query,values)
projectB.commit()

cursor.execute('''create table if not exists Aggregate_User(State varchar(50),
            Year int, 
            Quarter int,
            Mobile_Brand varchar(50),
            Transaction_Count bigint,
            Percentage float)'''
            )
projectB.commit()

for _, row in agg_user.iterrows():
            insert_query = '''
                INSERT INTO Aggregate_User (State, Year, Quarter, Mobile_Brand, Transaction_Count, Percentage)
                VALUES (%s, %s, %s, %s, %s, %s)
            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['Mobile_Brand'],
                row['Transaction_Count'],
                row['Percentage']
            )
            cursor.execute(insert_query,values)
projectB.commit()

cursor.execute('''create table if not exists Map_Transaction(State varchar(50),
           Year int, 
           Quarter int, 
           District varchar(50),
           Transaction_Count bigint,
           Transaction_Amount bigint)'''
           )
projectB.commit()

for _, row in map_trans.iterrows():
            insert_query = insert_query = '''
                INSERT INTO Map_Transaction (State, Year, Quarter, District, Transaction_Count, Transaction_Amount)
                VALUES (%s, %s, %s, %s, %s, %s)
            '''

            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['District'],
                row['Transaction_Count'],
                row['Transaction_Amount']
            )
            cursor.execute(insert_query,values)
projectB.commit()

cursor.execute('''create table if not exists Map_User(State varchar(50),
           Year int, 
           Quarter int,
           District varchar(50),
           Registered_Users bigint,
           Apps_Opened bigint)'''
           )
projectB.commit()

for _, row in map_user.iterrows():
            insert_query = '''
                INSERT INTO Map_User (State, Year, Quarter, District, Registered_Users, Apps_Opened)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['District'],
                row['Registered_Users'],
                row['Apps_Opened']
            )
            cursor.execute(insert_query,values)
projectB.commit()

cursor.execute('''create table if not exists Top_Transaction(State varchar(50),
           Year int, 
           Quarter int, 
           PinCode int,
           Transaction_Count bigint,
           Transaction_Amount bigint)'''
           )
projectB.commit()

for _, row in top_trans.iterrows():
            insert_query = '''
                INSERT INTO Top_Transaction (State, Year, Quarter, PinCode, Transaction_Count, Transaction_Amount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['PinCode'],
                row['Transaction_Count'],
                row['Transaction_Amount']
            )
            cursor.execute(insert_query,values)
projectB.commit()

cursor.execute('''create table if not exists Top_User(State varchar(50),
           Year int, 
           Quarter int,
           PinCode int,
           Registered_Users bigint)'''
           )
projectB.commit()

for _, row in top_user.iterrows():
            insert_query = '''
                INSERT INTO Top_User (State, Year, Quarter, PinCode, Registered_Users)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['PinCode'],
                row['Registered_Users']
            )
            cursor.execute(insert_query,values)
projectB.commit()

#-------------------------------------------------------------------------------------------------------------------------------------------
