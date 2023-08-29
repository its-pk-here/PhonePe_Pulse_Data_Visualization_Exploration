import pandas as pd
import json
import os
import plotly.express as px
import requests
import subprocess
import plotly.graph_objects as go
from IPython.display import display
import psycopg2
import streamlit as st
from PIL import Image as PILImage

#---------------------------------------------------PSYCOPG CONNECTION---------------------------------------------------------------------

projectB = psycopg2.connect(host='<host>',user='<username>',password='<password>',database='<database>')
cursor = projectB.cursor()

cursor.execute('''select * from aggregate_transaction;''')
projectB.commit()
t1 = cursor.fetchall()
agg_trans = pd.DataFrame(t1, columns=['State', 'Year', 'Quarter', 'Transaction_Type', 'Transaction_Count', 'Transaction_Amount'])

cursor.execute('''select * from aggregate_user;''')
projectB.commit()
t2 = cursor.fetchall()
agg_user = pd.DataFrame(t2, columns=['State', 'Year', 'Quarter', 'Mobile_Brand', 'Transaction_Count', 'Percentage'])

cursor.execute('''select * from map_transaction;''')
projectB.commit()
t3 = cursor.fetchall()
map_transaction = pd.DataFrame(t3, columns=['State', 'Year', 'Quarter', 'District', 'Transaction_Count', 'Transaction_Amount'])

cursor.execute('''select * from map_user;''')
projectB.commit()
t4 = cursor.fetchall()
map_user = pd.DataFrame(t4, columns=['State', 'Year', 'Quarter', 'District', 'Registered_Users', 'Apps_Opened'])

cursor.execute('''select * from top_transaction;''')
projectB.commit()
t5 = cursor.fetchall()
top_trans = pd.DataFrame(t5, columns=['State', 'Year', 'Quarter', 'PinCode', 'Transaction_Count', 'Transaction_Amount'])

cursor.execute('''select * from top_user;''')
projectB.commit()
t6 = cursor.fetchall()
top_user = pd.DataFrame(t6, columns=['State', 'Year', 'Quarter', 'PinCode', 'Registered_Users'])

#---------------------------------------------------Exploration-----------------------------------------------------------------------------

# OVERALL MAP - AMOUNT

def map_amount_overall():
    a_t2 = agg_trans[['State','Transaction_Amount']]
    a_t2 = a_t2.sort_values(by='State')
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()
    df_state_names_tra = pd.DataFrame({'State':state_names_tra})

    a_t2['State'] = a_t2['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
    a_t2['State'] = a_t2['State'].str.replace('---', ' & ')
    a_t2['State'] = a_t2['State'].str.replace('-', ' ')
    a_t2['State'] = a_t2['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    a_t2['State'] = a_t2['State'].str.title()

    merge_df = df_state_names_tra.merge(a_t2, on='State')

    trans_fig = px.choropleth(merge_df, 
                geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                             featureidkey = 'properties.ST_NM', locations='State', color='Transaction_Amount', color_continuous_scale='dense' , range_color=(0,150000000000))
    trans_fig.update_geos(fitbounds="locations", visible=False)
    #trans_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
    st.plotly_chart(trans_fig)

# YEARLY MAP - Amount

def map_amount(yr, q):
    year = int(yr)
    qr = int(q)
    a_t = agg_trans[['State','Year','Quarter','Transaction_Amount']]
    a_t1 = a_t.loc[(a_t['Year']==year) & (a_t['Quarter']==qr)]
    a_t2 = a_t1[['State','Transaction_Amount']]
    a_t2 = a_t2.sort_values(by='State')
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()
    df_state_names_tra = pd.DataFrame({'State':state_names_tra})

    a_t2['State'] = a_t2['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
    a_t2['State'] = a_t2['State'].str.replace('---', ' & ')
    a_t2['State'] = a_t2['State'].str.replace('-', ' ')
    a_t2['State'] = a_t2['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    a_t2['State'] = a_t2['State'].str.title()

    merge_df = df_state_names_tra.merge(a_t2, on='State')

    trans_fig = px.choropleth(merge_df, 
                geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                             featureidkey = 'properties.ST_NM', locations='State', color='Transaction_Amount', color_continuous_scale='deep', range_color=(0, 200000000000))
    trans_fig.update_geos(fitbounds="locations", visible=False)
    #trans_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
    st.plotly_chart(trans_fig)

# OVERALL MAP - COUNT

def map_count_overall():
    a_t2 = agg_trans[['State','Transaction_Count']]
    a_t2 = a_t2.sort_values(by='State')
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()
    df_state_names_tra = pd.DataFrame({'State':state_names_tra})

    a_t2['State'] = a_t2['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
    a_t2['State'] = a_t2['State'].str.replace('---', ' & ')
    a_t2['State'] = a_t2['State'].str.replace('-', ' ')
    a_t2['State'] = a_t2['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    a_t2['State'] = a_t2['State'].str.title()

    merge_df = df_state_names_tra.merge(a_t2, on='State')

    trans_fig = px.choropleth(merge_df, 
                geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                             featureidkey = 'properties.ST_NM', locations='State', color='Transaction_Count', color_continuous_scale='dense' , range_color=(0,80000000))
    trans_fig.update_geos(fitbounds="locations", visible=False)
    #trans_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
    st.plotly_chart(trans_fig)
    
# YEARLY MAP - COUNT

def map_count(yr, q):
    year = int(yr)
    qr = int(q)
    a_t = agg_trans[['State','Year','Quarter','Transaction_Count']]
    a_t1 = a_t.loc[(a_t['Year']==year) & (a_t['Quarter']==qr)]
    a_t2 = a_t1[['State','Transaction_Count']]
    a_t2 = a_t2.sort_values(by='State')
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()
    df_state_names_tra = pd.DataFrame({'State':state_names_tra})

    a_t2['State'] = a_t2['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
    a_t2['State'] = a_t2['State'].str.replace('---', ' & ')
    a_t2['State'] = a_t2['State'].str.replace('-', ' ')
    a_t2['State'] = a_t2['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    a_t2['State'] = a_t2['State'].str.title()
    
    merge_df = df_state_names_tra.merge(a_t2, on='State')

    trans_fig = px.choropleth(merge_df, 
                geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                             featureidkey = 'properties.ST_NM', locations='State', color='Transaction_Count', color_continuous_scale='deep', range_color=(0, 200000000))
    trans_fig.update_geos(fitbounds="locations", visible=False)
    #trans_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
    st.plotly_chart(trans_fig)    
    
    
# OVERALL DATAFRAME - AMOUNT
    
def df_overall_amount():
    a_t = agg_trans[['Transaction_Type','Transaction_Amount']]
    transaction_type = a_t.groupby('Transaction_Type')['Transaction_Amount'].sum()
    df1 = pd.DataFrame(transaction_type).reset_index()
    return st.dataframe(df1)

# YEARLY DATAFRAME - AMOUNT

def df_amount(yr,q):
    year = int(yr)
    qr = int(q)
    a_t = agg_trans[['Year','Quarter','Transaction_Type','Transaction_Amount']]
    a_t1 = a_t.loc[(a_t['Year']==year) & (a_t['Quarter']==qr)]
    a_t2 = a_t1[['Transaction_Type','Transaction_Amount']]
    transaction_type = a_t2.groupby('Transaction_Type')['Transaction_Amount'].sum()
    df1 = pd.DataFrame(transaction_type).reset_index()
    return st.dataframe(df1)

# OVERALL DATAFRAME - COUNT
    
def df_overall_count():
    a_t = agg_trans[['Transaction_Type','Transaction_Count']]
    transaction_type = a_t.groupby('Transaction_Type')['Transaction_Count'].sum()
    df1 = pd.DataFrame(transaction_type).reset_index()
    return st.dataframe(df1)

# YEARLY DATAFRAME - COUNT

def df_count(yr,q):
    year = int(yr)
    qr = int(q)
    a_t = agg_trans[['Year','Quarter','Transaction_Type','Transaction_Count']]
    a_t1 = a_t.loc[(a_t['Year']==year) & (a_t['Quarter']==qr)]
    a_t2 = a_t1[['Transaction_Type','Transaction_Count']]
    transaction_type = a_t2.groupby('Transaction_Type')['Transaction_Count'].sum()
    df1 = pd.DataFrame(transaction_type).reset_index()
    return st.dataframe(df1)

# STATEWISE MOBILE BRAND

def statewise_mb(State):
    Agg_user = agg_user[['State','Mobile_Brand','Transaction_Count']]
    Agg_user['State'] = Agg_user['State'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    Agg_user['State'] = Agg_user['State'].str.replace('---', ' & ')
    Agg_user['State'] = Agg_user['State'].str.replace('-', ' ')
    Agg_user['State'] = Agg_user['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    Agg_user['State'] = Agg_user['State'].str.title()
    
    au2 = Agg_user.loc[(Agg_user['State'] == State)]
    brand_transaction_counts = au2.groupby('Mobile_Brand')['Transaction_Count'].sum()
    bt = pd.DataFrame(brand_transaction_counts).reset_index()
    fig2 = px.bar(bt, y='Transaction_Count',
                 x='Mobile_Brand',
                 color_discrete_sequence = px.colors.sequential.Aggrnyl)
    fig2.update_layout(width=1300, height=400)
    st.plotly_chart(fig2)


#-----------------------------------------------------------QUERIES-----------------------------------------------------------------------    

# (1) TOP 10 TRANSACTIONS

def one():   
    ag1 = agg_trans[['State','Transaction_Amount']]
    ag1 = ag1.groupby('State')['Transaction_Amount'].sum()
    ag1 = ag1.sort_values()

    agi = ag1.tail(10)
    fig = px.bar(agi, x=agi.index, y='Transaction_Amount', color_discrete_sequence = px.colors.sequential.Rainbow)
    st.plotly_chart(fig)


# (2) LEAST 10 TRANSACTIONS

def two():
    ag1 = agg_trans[['State','Transaction_Amount']]
    ag1 = ag1.groupby('State')['Transaction_Amount'].sum()
    ag1 = ag1.sort_values()

    agi = ag1.head(10)
    fig = px.bar(agi, x=agi.index, y='Transaction_Amount', color_discrete_sequence = px.colors.sequential.Agsunset)
    st.plotly_chart(fig)


# (3) MOBILE BRAND vs TRANSACTION COUNT

def three():
    au=agg_user[['Mobile_Brand','Transaction_Count']]
    brand_transaction_counts = au.groupby('Mobile_Brand')['Transaction_Count'].sum()
    bt = pd.DataFrame(brand_transaction_counts).reset_index()
    fig2 = px.pie(bt, values='Transaction_Count',
                 names='Mobile_Brand',
                 color_discrete_sequence = px.colors.qualitative.Antique)
    st.plotly_chart(fig2)


 # (4)TOP 10 APPS OPENED STATES

def four():
    mu1 = map_user[['State', 'Apps_Opened']]
    apps_opened = mu1.groupby('State')['Apps_Opened'].sum()
    ao = pd.DataFrame(apps_opened).reset_index()
    ao = ao.sort_values(by='Apps_Opened', ascending=False)  # Sort by 'Apps_Opened' column in descending order
    ao = ao.head(10)
    fig = px.bar(ao,x='State',y='Apps_Opened', color_discrete_sequence=px.colors.sequential.ice)
    st.plotly_chart(fig)


 # (5) TOP 10 REGISTERED USERS

def five():
    mu = map_user[['State','Registered_Users']]
    registered_user = mu.groupby('State')['Registered_Users'].sum()
    ru = pd.DataFrame(registered_user).reset_index()
    ru = ru.sort_values(by = 'Registered_Users', ascending=False)
    ru = ru.head(10)
    fig1 = px.bar(ru,x='State',y='Registered_Users', color_discrete_sequence=px.colors.sequential.Viridis)
    st.plotly_chart(fig1)


 # (6) DISTRICT TRANSACTION

def six():
    mt = map_transaction[['District','Transaction_Amount']]
    district_transaction = mt.groupby('District')['Transaction_Amount'].sum()
    dt = pd.DataFrame(district_transaction).reset_index()
    dt = dt.sort_values(by = 'Transaction_Amount', ascending=False)
    dt = dt.head(10)
    fig2 = px.bar(dt,x='District',y='Transaction_Amount', color_discrete_sequence=px.colors.sequential.Jet)
    st.plotly_chart(fig2)
  

 # (7) DISTRICT vs REGISTERED USERS

def seven():
    mu = map_user[['State','Year','Quarter','District','Registered_Users','Apps_Opened']]
    district_users = mu.groupby('District')['Registered_Users'].sum()
    du = pd.DataFrame(district_users).reset_index()
    du = du.sort_values(by = 'Registered_Users', ascending = False)
    du = du.head(10)
    fig3 = px.bar(du,x='District',y='Registered_Users', color_discrete_sequence=px.colors.sequential.Cividis)
    st.plotly_chart(fig3)


# (8) TOP 10 APPS OPENED DISTRICTS

def eight():
    mu2 = map_user[['District', 'Apps_Opened']]
    apps_opened_district = mu2.groupby('District')['Apps_Opened'].sum()
    aod = pd.DataFrame(apps_opened_district).reset_index()
    aod = aod.sort_values(by='Apps_Opened', ascending=False)  # Sort by 'Apps_Opened' column in descending order
    aod = aod.head(10)
    fig8 = px.bar(aod,x='District',y='Apps_Opened', color_discrete_sequence=px.colors.sequential.Burg)
    st.plotly_chart(fig8)
 
# (9) LEAST 10 APPS OPENED DISTRICTS

def nine():
    mu3 = map_user[['District', 'Apps_Opened']]
    least_apps_opened_district = mu3.groupby('District')['Apps_Opened'].sum()
    aod1 = pd.DataFrame(least_apps_opened_district).reset_index()
    aod1 = aod1.sort_values(by='Apps_Opened')  # Sort by 'Apps_Opened' column in descending order
    aod1 = aod1.head(10)
    fig9 = px.bar(aod1,x='District',y='Apps_Opened', color_discrete_sequence=px.colors.sequential.Bluered)
    st.plotly_chart(fig9)
    
#-----------------------------------------------------------STREAMLIT----------------------------------------------------------------------

st.set_page_config(layout="wide") #Wide screen

st.title("PHONE PE PULSE DATA VISUALIZATION AND EXPLORATION")
                                        
tab1, tab2, tab3 = st.tabs(['HOME', 'EXPLORE','ANALYSIS'])

with tab1:
    col1,col2 = st.columns(2)
    with col1:
        image_path = r"C:\Users\pavan\Downloads\phone-pe.png"
        col1.image(PILImage.open(image_path), width=500)
        st.write("PhonePe is a leading digital payments and financial technology platform that has revolutionized the way people in India manage their finances and make transactions. Launched in 2015, PhonePe quickly gained popularity as a user-friendly and secure mobile app that enables seamless digital transactions.")
        
        st.write("APP DOWNLOAD LINK - https://www.phonepe.com/app-download/")
        
    with col2:
        st.caption("KEY FEATURES AND SERVICES OF PHONE PE :")
        st.write("1. MONEY TRANSFER : PhonePe allows users to send and receive money instantly from their bank accounts. Users can also request money from friends and family.")
        st.write("2. BILL PAYMENTS AND RECHARGES : Users can conveniently pay their utility bills, such as electricity, water, gas and more, directly from the app. They can also recharge mobile prepaid plans and DTH services.")
        st.write("3. ONLINE AND OFFLINE PAYMENTS : PhonePe facilitates payments at both online and offline merchant establishments. Users can simply scan QR codes or use the 'Tap to Pay' feature for swift transactions.")
        st.write("4. REWARDS AND OFFERS : PhonePe offers various cashback rewards, discounts, and exclusive deals to users who make transactions using the platform.")
        st.write("5. SECURITY AND PRIVACY : PhonePe employs robust security measures to safeguard user data and financial information. It requires secure PIN or biometric authentication for transactions.")
        st.write("6. USER-FRIENDLY INTERFACE : The app is designed with a user-friendly and intuitive interface, making it easy for people of all ages and backgrounds to navigate and perform transactions.")



with tab2:
    
    col1,col2 = st.columns(2)
    with col1:
        tr_year = st.selectbox('**Select Year**', ('Overall','2018','2019','2020','2021','2022'))
    with col2:
        tr_quarter = st.selectbox('**Select Quarter**',(1,2,3,4))
    
    
    col1,col2 = st.columns(2)
    
    with col1:
        st.caption('TRANSACTION TYPE VS TRANSACTION AMOUNT')
        if tr_year == 'Overall':
            df_overall_amount()
            map_amount_overall()
        else:
            df_amount(tr_year , tr_quarter)
            map_amount(tr_year , tr_quarter)
            
    with col2:
        st.caption('TRANSACTION TYPE VS TRANSACTION COUNT')
        if tr_year == 'Overall':
            df_overall_count()
            map_count_overall()
        else:
            df_count(tr_year, tr_quarter)
            map_count(tr_year, tr_quarter)

    state=st.selectbox('**Select State**',('Andaman & Nicobar Islands','Andhra Pradesh', 'Arunachal Pradesh',
       'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
       'Dadra & Nagar Haveli & Daman & Diu', 'Delhi', 'Goa', 'Gujarat',
       'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir', 'Jharkhand',
       'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh',
       'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland',
       'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
       'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
       'Uttarakhand', 'West Bengal'))
    statewise_mb(state)
                
with tab3:
    query = st.selectbox("Select Table",('None','Top 10 Transactions', 'Least 10 Transactions',
         'Mobile Brand vs Transaction Count', 'Top 10 Apps Opened States', 'Top 10 Registered Users' ,'Top 10 District Transaction', 
         'District vs Registered Users', 'Top 10 Apps Opened Districts', 'Least 10 Apps Opened Districts'))

    st.write('You selected: ',query)
    
    if query=='None':
        st.write("Select table")
    elif query=='Top 10 Transactions':
        one()
    elif query=='Least 10 Transactions':
        two()
    elif query=='Mobile Brand vs Transaction Count':
        three()
    elif query=='Top 10 Apps Opened States':
        four()
    elif query=='Top 10 Registered Users':
        five()
    elif query=='Top 10 District Transaction':
        six()
    elif query=='District vs Registered Users':
        seven()
    elif query=='Top 10 Apps Opened Districts':
        eight()
    elif query=='Least 10 Apps Opened Districts':
        nine()
#--------------------------------------------------------------THE END---------------------------------------------------------------------  



