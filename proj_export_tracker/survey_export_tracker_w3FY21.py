# %%
# 1. start off with distinctive list of w3 contacts
# 1.1   add those contacts who were invited in w2 
# 2. work out how many times each contacts were invited out of 6 waves.

#%%
## get working directory start and the project level
import os
os.chdir("..")

# %%
import pyodbc as pyodbc
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import uuid

pd.set_option('float_format', '{:.2f}'.format)
# Folder that hold outputs
outDir  = 'data//survey_w3_FY21//'
scriptDir = 'src//'

from src.azure_db import get_connection_hardcode, get_sql_credential, get_connection
# username , password = get_sql_credential()
# cnxn = get_connection(username, password)
cnxn = get_connection_hardcode()
# %%
## 1. read w3 master contacts - **TO BE REPLACED WITH ACTUALLY VALUE FROM DB**
ect_master_w3FY21 =  pd.read_csv(outDir + 'w3FY21_dummy_MasterContacts.csv')
ect_master_w3FY21.shape             #expect (2023,24)

w3_contacts = ect_master_w3FY21.groupby(['Contact_Name', 'Contact_Key', 'Is_Duplicate_Invite']).size().reset_index(name = 'TMs')

# %%

## 1.1 read FY21 from database - 
## output : distinctive list of contacts for analysis FY21 W3, including those invited twice in wave2
with open('scripts//ect_master_dummy_w3FY21.sql', 'r') as sql_file:
        ect_master_w2FY21 = pd.read_sql(sql_file.read(), cnxn)
                                  
w2_FY21_master_contact_twice_invite  = ect_master_w2FY21.copy().query('Is_Duplicate_Invite == 1 and wave == 2')
w2_FY21_master_contact_twice_invite['Contact_Key'].nunique()        #expect  333 contacts
w2_contacts = w2_FY21_master_contact_twice_invite.groupby(['Contact_Name', 'Contact_Key', 'Is_Duplicate_Invite']).size().reset_index(name = 'TMs')

w3_contacts_activities = w3_contacts.append(w2_contacts)
w3_contacts_activities.shape        #expect (1963,4)

#%%
# 2. work out how many times each contacts were invited out of 4 waves.

with open('scripts//ect_master.sql', 'r') as sql_file:
        ect_master_prev = pd.read_sql(sql_file.read(), cnxn)

ect_master = ect_master_prev.append(ect_master_w3FY21)
ect_master.groupby(by = ['Fiscal_Year', 'wave'])['Contact_Key'].count()
ect_master['year_wave']  = ect_master['Fiscal_Year'].astype(str) + "-" + ect_master['wave'].astype(str)

## 3361 distinctive contacts across 2 Fiscal Years, but 1963  for latest wave
ect_master_FY20FY1 = ect_master.groupby(['Contact_Name', 'Contact_Key'])['year_wave'].nunique().reset_index(name = "invites_count").sort_values("invites_count", ascending = False)

master_contacts_latestFY =  pd.merge(w3_contacts_activities, ect_master_FY20FY1[["Contact_Key", "Contact_Name","invites_count"]], how = 'left', on = ["Contact_Key", "Contact_Name"])

master_contacts_latestFY.shape

# %%
# 2.1 work out how many times each contacts responded 
## read responses from static files in OneDrive (used in previous analysis)

response_FY20 =  pd.read_excel('C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave2_FY21//21-03-08 FY21_w2_data.xlsx'
                , sheet_name = 'FY20_Q1_Q3_AllWaves(src)')
 
response_FY21_w1_w2 =  pd.read_excel('C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave2_FY21//21-03-08 FY21_w2_data.xlsx'
                , sheet_name = 'FY21_q1_q3_w1w2(src)')

prev_response = response_FY20.append(response_FY21_w1_w2)                
prev_response['Contact_Key'] = prev_response['Contact.Wave.Year'].str.split("-wave").str[0]
prev_response['Contact_Name'] = prev_response['Survey Tracker Answer Respondent Name'].str.title()                  #convert to Camel Case

# %%
master_contacts_response_by_wave = (prev_response.groupby(by = ['Contact_Key', 'Contact_Name','Wave.Year']).size()
                                .reset_index(name = "count")
                                .assign(response_count = lambda x: x['count'].map(lambda count: 1 if count > 0 else 0)) 
                                .pivot_table(index = ['Contact_Key','Contact_Name'], columns = 'Wave.Year', values = "response_count")
                                .fillna(0)
                                .sort_values('Contact_Name')
                                )
master_contacts_response_rate = (pd.merge(master_contacts_latestFY, master_contacts_response_by_wave, how = 'left', on = ['Contact_Key', 'Contact_Name'])
                                .fillna(0)
                                .sort_values('Contact_Name')
                                )
#rename columns for response count
master_contacts_response_rate.columns = master_contacts_response_rate.columns.str.replace("wave", "R_w")
#change from float to int
filter_col = [col for col in master_contacts_response_rate if col.startswith('R_w')]
master_contacts_response_rate[filter_col] = master_contacts_response_rate[filter_col].astype(int)

#counts how many response times for each contact
idx = master_contacts_response_rate.columns.str.startswith('R_w')
master_contacts_response_rate['total_responses'] = master_contacts_response_rate.iloc[:, idx].sum(axis = 1)
master_contacts_response_rate['response_rate'] = master_contacts_response_rate['total_responses']/master_contacts_response_rate['invites_count']

# %%
# 
master_contacts_response_rate.info()
# %%
master_contacts_response_rate.groupby(by = ['response_rate', 'invites_count']).size()
# %%

master_contacts_response_rate.groupby(by = ['response_rate']).size()
# %%
master_contacts_response_rate['response_rate'].value_counts()
# %%
``