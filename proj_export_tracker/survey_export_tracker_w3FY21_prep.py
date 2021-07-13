#%%
## 3 things to generate as dummy data from wave 3 FY21:
## - a master file      (ECT.ExportCustomerTracker)
## - a responses file   (CRM.SurveyTrackerAnswers)
## - an invite file     (CRM.Invite??)

#%%
import os
os.chdir("..")

# %%
import pyodbc as pyodbc
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import uuid

pd.set_option('float_format', '{:.0f}'.format)
# Folder that hold outputs
outDir  = 'data//survey_w3_FY21//'
scriptDir = 'src//'

from src.azure_db import get_sql_credential, get_connection
username , password = get_sql_credential()
cnxn = get_connection(username, password)


# %%
# Extract Investor list
## read from database
with open('scripts//ect_master_dummy_w3FY21.sql', 'r') as sql_file:
        ect_master= pd.read_sql(sql_file.read(), cnxn)
                                  
#%%
# create wave3 dummy data (from combination of w1 and w2 FY21 , excluding those were invited twice in wave1 and wave2)
#1630 contacts to be invited in wave3 FY21
ect_master_w3_fy21 = ect_master.copy().query('Is_Duplicate_Invite == 0')
ect_master_w3_fy21['wave'] = 3
ect_master_w3_fy21.to_csv(outDir + "w3FY21_dummy_MasterContacts.csv",  sep = ",",  index = False, header = True , encoding = "utf-8", line_terminator='\n')

w3_contacts = (ect_master_w3_fy21['Contact_Key'].value_counts()
                .reset_index()
                .rename(columns = {"index": "Survey Tracker Answer Respondent Key", "Contact_Key": "count"})
        )        

#%%
## pick random contact from w3_contacts list to generate dummy responses (10%  response rate)
## as a replacement of ECT.ExportCustomerTracker
## uncomment the following 2 lines to rerandom again
# sample_respondents = w3_contacts.sample(n = 200)
# sample_respondents.to_csv(outDir + "w3FY21_dummy_respondent.csv",  sep = ",",  index = False, header = True , encoding = "utf-8", line_terminator='\n')

sample_respondents  = pd.read_csv(outDir + 'w3FY21_dummy_respondent.csv')

previous_results_sql = """
            select *
            from [CRM].[SurveyTrackerAnswers] """
previous_results = pd.read_sql(previous_results_sql, cnxn)
print(previous_results.shape)

##retrieve results in previous wave of the sample respondents
sample_prev_results = pd.merge(sample_respondents['Survey Tracker Answer Respondent Key']
                        , previous_results
                        , how = "inner", on = "Survey Tracker Answer Respondent Key")
#df = sample_prev_results.drop(["count"], axis = 1)


#%%
## random pick a wave for their w3 dummy answers
## as a replacement of CRM.SurveyTrackerAnswers
## expected results - only one wave response from each Contact Key
## note: one 'Contact Key' expects to have only one 'Response Key' for each wave but many 'Answer Key'

sample_wave = (sample_prev_results
        #.query("`Survey Tracker Answer Respondent Key` == '025CA79B-4B79-EA11-A811-000D3ACACF4A' or `Survey Tracker Answer Respondent Key` == '06EDADD3-812D-E911-A97A-000D3AD10801' ")
        .groupby(by = ['Survey Tracker Answer Respondent Key'])
        .apply(lambda x: x.sample(1))
        .reset_index(drop=True)
        )
## re-generate random 'Survey Response Keya' for each contact key and each wave
sample_wave["Survey Tracker Answer Survey Response Key"] = sample_wave.apply(lambda _: uuid.uuid4(), axis=1)

##retrieve results in previous wave of the sample respondents
w3_results = pd.merge(sample_prev_results.drop(['Survey Tracker Answer Survey Response Key'], axis= 1)
                         , sample_wave[['Survey Tracker Answer Respondent Key','Survey Tracker Answer Sent On Date', 
                                        'Survey Tracker Answer Survey Response Key']]
                         , how = "inner"
                         , on = ["Survey Tracker Answer Respondent Key", 'Survey Tracker Answer Sent On Date'])

## re-generate random Keys for w3FY21 dummy responses for every single row
w3_results['Survey Tracker Answer Sent On Date'] = pd.to_datetime("2021-06-14")
w3_results["Survey Tracker Answer Key"] = w3_results.apply(lambda _: uuid.uuid4(), axis = 1)

w3_results.groupby(by = ["Survey Tracker Answer Respondent Key","Survey Tracker Answer Survey Response Key"])['Survey Tracker Answer Key'].nunique()

## save to to file for reference
w3_results.to_csv(outDir + "w3FY21_dummy_responses.csv",  sep = ",",  index = False, header = True , encoding = "utf-8", line_terminator='\n')

#%%






# %%
sample_wave.groupby(by = ["Survey Tracker Answer Respondent Key", "Survey Tracker Answer Sent On Date"]).size()
                        


# %%
