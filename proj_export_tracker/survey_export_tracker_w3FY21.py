# %%
# 1. start off with distinctive list of w3 contacts
# 1.1   add those contacts who were invited in w2 
# 2. work out how many times each contacts were invited out of 6 waves.
# 3. read the Engagement Intensity of customers (from https://app.powerbi.com/groups/40b6948e-1464-4281-98ee-0a27963502e9/reports/c531938d-1cca-4345-a486-7207dd3f25e2/ReportSection)

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

from src.azure_db import get_sql_credential, get_connection
username , password = get_sql_credential()
cnxn = get_connection(username, password)

#cnxn = get_connection_hardcode()
# %%
## 1. read w3 master contacts - **TO BE REPLACED WITH ACTUALLY VALUE FROM DB**
# ect_master_w3FY21 =  pd.read_csv(outDir + 'w3FY21_dummy_MasterContacts.csv')
# ect_master_w3FY21.shape             #expect (2023,24)

with open('scripts//ect_master.sql', 'r') as sql_file:
        ect_master = pd.read_sql(sql_file.read(), cnxn)

## add year_wave for future grouping
ect_master['year_wave']  = ect_master['Fiscal_Year'].astype(str) + "-" + ect_master['wave'].astype(str)
                                  
ect_master_w3FY21  = ect_master.copy().query('wave == 3 and Fiscal_Year == 2021')
print(ect_master_w3FY21.shape)

w3_contacts = ect_master_w3FY21.groupby(['Contact_Name', 'Contact_Key'
                                        , 'Is_Duplicate_Invite','Organisation_key'
                                        , 'Organisation_Name' , 'Contact_Type', 'Contact_Email']).size().reset_index(name = 'TMs')
print("wave 3 FY21 contacts")
print(w3_contacts.shape)        #1636 contacts surveyed in w3 FY21

# %%

## 1.1 read those duplicated in w2  FY21 and used the list as to find NR over 2 FYs 
## output : distinctive list of contacts for analysis FY21 W3, including those invited twice in wave2
# w2_FY21_master_contact_twice_invite  = ect_master.copy().query('wave == 2 and Fiscal_Year == 2021 and Is_Duplicate_Invite ==1')
# w2_FY21_master_contact_twice_invite['Contact_Key'].nunique()        #expect  333 contacts
# w2_contacts = w2_FY21_master_contact_twice_invite.groupby(['Contact_Name', 'Contact_Key'
#                                 , 'Is_Duplicate_Invite', 'Organisation_key'
#                       dfd          , 'Organisation_Name' ]).size().reset_index(name = 'TMs')

# w3_contacts_activities = w3_contacts.append(w2_contacts)
# print("w3 contacts (including the duplicated ones in w2)")
# print(w3_contacts_activities.shape)        #expect (1969,6) and should remain the same throughout this analysis

#%%
#whole contacts in FY20-FY21
#including company details so that contacts response rate reflecting both companies
contact_cols = ['Contact_Name', 'Contact_Key','Organisation_key', 'Organisation_Name' , 'Contact_Type', 'Contact_Email']
all_contacts = (ect_master.groupby(contact_cols + ['year_wave']).size().reset_index(name = 'rows')
                        .pivot_table(index  = contact_cols, columns = 'year_wave', values = 'rows')
                        .reset_index()
                )
print("all contacts in the last 2 years")
print(all_contacts.shape)

#%%
# ??2. work out how many times each contacts were invited out of 4 waves.
# ect_master.groupby(by = ['Fiscal_Year', 'wave'])['Contact_Key'].count()

## 3361 distinctive contacts across 2 Fiscal Years, but 1963  for latest wave
ect_master_invite_FY20FY21 = ect_master.groupby(['Contact_Name', 'Contact_Key'])['year_wave'].nunique().reset_index(name = "invites_count").sort_values("invites_count", ascending = False)

master_contacts_latestFY =  pd.merge(all_contacts
                        , ect_master_invite_FY20FY21[["Contact_Key", "Contact_Name","invites_count"]]
                        , how = 'left', on = ["Contact_Key", "Contact_Name"])

print("w3 contacts + tms + #invites")
print(master_contacts_latestFY.shape)

# %%
# 2.1 work out how many times each contacts responded 
## read responses from static files in OneDrive (used in previous analysis)

response_FY21 = pd.read_excel('C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave3_FY21//21-06-16 FY21_w3_data.xlsx'
                , sheet_name = 'FY21_q1_q3_w1w2w3(src)')
response_FY20 =  pd.read_excel('C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave2_FY21//21-03-08 FY21_w2_data.xlsx'
                , sheet_name = 'FY20_Q1_Q3_AllWaves(src)')
# response_FY21_w1_w2_w3 =  pd.read_excel('C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave2_FY21//21-03-08 FY21_w2_data.xlsx'
#                 , sheet_name = 'FY21_q1_q3_w1w2w3(src)')

prev_response = response_FY20.append(response_FY21)                
prev_response['Contact_Key'] = prev_response['Contact.Wave.Year'].str.split("-wave").str[0]
prev_response['Contact_Name'] = prev_response['Survey Tracker Answer Respondent Name'].str.title()                  #convert to Camel Case
# 1584 contacts responded in FY20 and FY21

# %%
master_contacts_response_by_wave = (prev_response.groupby(by = ['Contact_Key', 'Contact_Name', 'Wave.Year']).size()
                                .reset_index(name = "count")
                                .assign(response_count = lambda x: x['count'].map(lambda count: 1 if count > 0 else 0)) 
                                .pivot_table(index = ['Contact_Key','Contact_Name'], columns = 'Wave.Year', values = "response_count")
                                .fillna(0)
                                .sort_values('Contact_Name')
                                #.reset_index()
                                )
master_contacts_response_rate = (pd.merge(master_contacts_latestFY, master_contacts_response_by_wave
                                                , how = 'left', on = ['Contact_Key']) 
                                .fillna(0)
                                .sort_values('Contact_Name')
                                )
#rename columns for response count
master_contacts_response_rate.columns = master_contacts_response_rate.columns.str.replace("wave", "R_w")
#change from float to int
filter_col = [col for col in master_contacts_response_rate if '20' in col]
master_contacts_response_rate[filter_col] = master_contacts_response_rate[filter_col].astype(int)

#counts how many response times for each contact
idx = master_contacts_response_rate.columns.str.startswith('R_w')
master_contacts_response_rate['total_responses'] = master_contacts_response_rate.iloc[:, idx].sum(axis = 1)
master_contacts_response_rate['response_rate'] = master_contacts_response_rate['total_responses']/master_contacts_response_rate['invites_count']

master_contacts_response_rate.info()

# %%
master_contacts_response_rate.groupby(by = ['response_rate', 'invites_count']).size()

master_contacts_response_rate.groupby(by = ['response_rate']).size()

master_contacts_response_rate['response_rate'].value_counts()


# %%
## 3.  Read Engagment Intensity 
with open('scripts//ect_CMIntensityModel.sql', 'r') as sql_file:
        egm_intensity = pd.read_sql(sql_file.read(), cnxn)

egm_intensity =  egm_intensity.rename(columns = { 'Legal_Name':'Organisation_Name'
                                , 'Org_Key': 'Organisation_key' })
egm_intensity.columns.to_list()


#%% 
##3.1 adding Intensity to the master_contacts_list
master_contacts_response_rate_egm = pd.merge(master_contacts_response_rate
                                        , egm_intensity#[['Organisation_key', 'IntensityScore_Opt1']]
                                        , how = 'left', on = 'Organisation_key').fillna("Undefined")


(master_contacts_response_rate_egm.groupby(by = ['response_rate', 'IntensityScore_Opt1']).size().reset_index(name = 'count')
        .pivot_table(index = 'response_rate', columns = 'IntensityScore_Opt1', values = 'count')
        .fillna(0)
        .astype('int')
)
# %%
##3.1.1 adding Intensity to the master_contacts_list (using WJ model)
df_compared = pd.read_csv('data/cluster_aftr20210602.csv')
wj_intensity = df_compared[['cluster_label_x', 'Organisation Key']].rename(columns = {'Organisation Key': 'Organisation_key'})

wj_intensity['cluster_label_x'].value_counts()

master_contacts_response_rate_egm_wj = pd.merge(master_contacts_response_rate_egm
                                        , wj_intensity[['Organisation_key', 'cluster_label_x']]
                                        , how = 'left', on = 'Organisation_key').fillna("Undefined")


master_contacts_response_rate_egm_wj =  master_contacts_response_rate_egm_wj.rename(columns = {'cluster_label_x':'intesity_wj'})
                                
                                

(master_contacts_response_rate_egm_wj.groupby(by = ['response_rate', 'intesity_wj']).size().reset_index(name = 'count')
        .pivot_table(index = 'response_rate', columns = 'intesity_wj', values = 'count')
        .fillna(0)
        .astype('int')
)
# %%
master_contacts_response_rate_egm_wj.groupby(by = ['intesity_wj','IntensityScore_Opt1']).size()

#added more features
# w3_contacts_response_rate = pd.merge(master_contacts_response_rate_egm_wj
#                                         , master_contacts_response_rate_egm
#                                         , how = 'left', on = 'Organisation_key').fillna("Undefined")


#save to excel
master_contacts_response_rate_egm_wj.to_excel(outDir + "intensity_comparison.xlsx",  index = False, header = True , encoding = "utf-8")
#%%
master_contacts_response_rate_egm_wj.shape
# %%
df = master_contacts_response_rate_egm_wj['IntensityScore_Opt1'].value_counts().reset_index()
df['pct'] = df['IntensityScore_Opt1'] / df['IntensityScore_Opt1'].sum()
df
# %%
master_contacts_response_by_wave = (prev_response.groupby(by = ['Contact_Key', 'Contact_Name','Wave.Year']).size()
                                .reset_index(name = "count")
                                .assign(response_count = lambda x: x['count'].map(lambda count: 1 if count > 0 else 0)) 
                                .pivot_table(index = ['Contact_Key','Contact_Name'], columns = 'Wave.Year', values = "response_count")
                                .fillna(0)
                                .sort_values('Contact_Name')
                                )
# master_contacts_response_rate = (pd.merge(master_contacts_latestFY, master_contacts_response_by_wave
#                                                 , how = 'left', on = ['Contact_Key']) 
#                                 .fillna(0)
#                                 .sort_values('Contact_Name')
#                                 )

# %%
## Company Level : work out response rate at customer level
#######################################
#2,518 customers were invited over 2 FYs
customers = (ect_master#.query("Organisation_key == 'AC01C543-C8F5-DF11-A1F6-02BF0ADC02DB'")
                #.query("Organisation_Name == 'Daifuku Oceania Limited'")
                .groupby(['Organisation_key'
                        #, 'Organisation_Name'
                        ,'year_wave'])['Contact_Key'].nunique()
                .reset_index(name = 'invited_contacts')
                .pivot_table(index = ['Organisation_key'] #, 'Organisation_Name']
                                , columns=['year_wave'] , values = 'invited_contacts')
                .reset_index()
                #.sort_values('Organisation_Name')                               
                .fillna(0)                
                )                
# #counts how many response times for each contact

# customers['total_invite_waves'] = lambda x: x[]
# ##customer.iloc[:, idx].sum(axis = 1)

idx = customers.columns.str.startswith('20')
filter_col = [col for col in customers if col.startswith('20')]                       
customers[filter_col] = customers[filter_col].astype(int)
#add count invited_contacts of a company over 2 FYs
customers['max_invited_contacts'] =  customers.iloc[:, idx].max(axis = 1)

# join with customers survey invite list to limits customers list down to just
# current Focus 1,522 companies
focus_survey_with_CF_intensity = pd.merge(customers, egm_intensity, how = 'inner'
                , on = "Organisation_key").fillna(0)             
focus_survey_with_CF_intensity



#%%
## NON Response || customers level
focus = master_contacts_response_rate_egm_wj.query("Organisation_Name_y != 'Undefined'")

focus_activity = (focus.groupby(by = ['Organisation_key' 
                                #,'Organisation_Name_x'
                                , 'NZTE_Sector'])['response_rate'].sum()
                       .reset_index(name = "response_rate")
)

# %%
focus_rr = (pd.merge(focus_activity, customers, how = 'left'
                , on = "Organisation_key")
                #.sort_values('Organisation_Name')
)


# focus_rr["Organisation_Name_x"].value_counts()
idx = focus_rr.columns.str.startswith('20')
#count only waves that invited contacts  greater than zero
focus_rr['invites_count'] =  focus_rr.iloc[:, idx].gt(0).sum(axis=1)
focus_rr.query("Organisation_key == 'AC01C543-C8F5-DF11-A1F6-02BF0ADC02DB'")
#%%
#get rid of double records due to the company name changes.
focus_rr_cln = (focus_rr.groupby(by = ['Organisation_key']).first()                
                        .reset_index()
                        #.sort_values('Organisation_Name')
                        .merge(egm_intensity, how = 'left', on = "Organisation_key")
)
focus_rr_cln.query("Organisation_key == 'AC01C543-C8F5-DF11-A1F6-02BF0ADC02DB'") 

# %%
focus_nr = focus_rr_cln.query("response_rate == 0")

focus_rr_cln.to_excel(outDir + "response_rate_customer2.xlsx",  index = False, header = True , encoding = "utf-8")
# %%

# %%
