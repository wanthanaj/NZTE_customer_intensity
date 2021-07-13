# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
path="C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//src//customer-intensity//"
os.chdir(path)
os.getcwd()


# %%
import pyodbc as pyodbc
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
pd.set_option('float_format', '{:.0f}'.format)
# Folder that hold outputs
outDir  = 'data//survey_w3_FY21//'

#connections
from src.azure_db import get_sql_credential, get_connection
username , password = get_sql_credential()
cnxn = get_connection(username, password)

#%%
####################################################################################
## read from database
with open('scripts//ect_master.sql', 'r') as sql_file:
        ect_master= pd.read_sql(sql_file.read(), cnxn)

ect_master.shape


# %%
ect_master_FY21 = ect_master.query("Fiscal_Year == 2021")
ect_master_FY21.shape


# %%
#NPS_FY21 both q1 and q3 - contains al respondents for both q1 and q3
#file_url = 'https://nztradeandenterprise.sharepoint.com/sites/Strategy/InsightsAnalysis/Customer%20Survey/Report/21-06-16%20FY21_w3_data.xlsx'
file_url = 'C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave3_FY21//21-06-16 FY21_w3_data.xlsx'

nps_q1q3_fy21 = pd.read_excel(file_url,  sheet_name = 'FY21_q1_q3_w1w2w3(src)')

# nps_q3_fy21.dropna(axis = 0, how ="all", inplace = True)
nps_q1q3_fy21.shape


# %%
#map engagement intensity level
nps_mapping = {0:"Passive", -100:'Detractor', 100: 'Promoter'}

#question 1
nps_q1_fy21 = nps_q1q3_fy21.query("`NPS (Q1)`.notnull()")
print("q1: " , str(nps_q1_fy21.shape))
#question 3
nps_q3_fy21 = nps_q1q3_fy21.query("`NPS (Q3)`.notnull()")
print("q3: " , str(nps_q3_fy21.shape))

nps_q1_fy21 =pd.DataFrame(
                nps_q1_fy21[["Responses","NPS (Q1)", "# NPS Raw Score", "Survey Tracker Answer Respondent Name"
                            , "Survey Tracker Answer Response", "Contact.Wave.Year", "Wave.Year"]]
                .rename(columns= {'Survey Tracker Answer Respondent Name': "Contact_Name"})
                .fillna('')
                )
nps_q1_fy21["wave"] = pd.to_numeric(nps_q1_fy21["Wave.Year"].str[5:7])
nps_q1_fy21["Fiscal_Year"] = pd.to_numeric(nps_q1_fy21["Wave.Year"].str[-4:])
nps_q1_fy21['Contact_Key'] = nps_q1_fy21['Contact.Wave.Year'].str.split("-wave").str[0]
nps_q1_fy21['nps_category'] = nps_q1_fy21['NPS (Q1)'].map(nps_mapping)

nps_q3_fy21 =pd.DataFrame(
                nps_q3_fy21[["Responses","NPS (Q3)", "# NPS Raw Score", "Target Market", "Survey Tracker Answer Respondent Name"
                            , "Survey Tracker Answer Response", "Contact.Wave.Year", "Wave.Year"]]
                .rename(columns= {'Survey Tracker Answer Respondent Name': "Contact_Name"
                                , 'Target Market': "Target_Market_Name"})
                .fillna('')
                )
nps_q3_fy21["wave"] = pd.to_numeric(nps_q3_fy21["Wave.Year"].str[5:7])
nps_q3_fy21["Fiscal_Year"] = pd.to_numeric(nps_q3_fy21["Wave.Year"].str[-4:])
nps_q3_fy21['Contact_Key'] = nps_q3_fy21['Contact.Wave.Year'].str.split("-wave").str[0]
nps_q3_fy21['nps_category'] = nps_q3_fy21['NPS (Q3)'].map(nps_mapping)


#%%
# join master file with q1 to identify who responded or not
all_invited_w_nps = (ect_master_FY21.merge(nps_q1_fy21, how = "left", on = ["Contact_Key", "wave", "Fiscal_Year"])
                        .fillna('')
                        .drop(["Wave.Year"], axis = 'columns')
                    )
# all_invited_w_nps = (all_invited_w_nps.merge(nps_q3_fy21, how = "left", on = ["Contact_Name", "wave", "Target_Market_Name", "Fiscal_Year"])
#                         .fillna('')
#                         .drop(["Wave.Year"], axis = 'columns')
#                     )
print(all_invited_w_nps.shape)


outfilePath = outDir + "//fy21_all_invited_w_nps_20210705.xlsx"
(all_invited_w_nps.replace('\n',' ', regex=True)
    .to_excel(outfilePath,  index = False, header = True , encoding = "utf-8")
)
all_invited_w_nps.columns


# %%
#save output to file
exported_date = '20210302' #date of the latest snapshot

df_compared = pd.read_csv('data/cluster20210302.csv')
df_compared.shape

#add intensity level to the master file (including NPS q1)
all_invited_w_nps = all_invited_w_nps.rename(columns={"Organisation_Name": 'Organisation Legal Name_'})
nps_and_egm =  pd.merge(all_invited_w_nps, df_compared, how = "left", on = "Organisation Legal Name_")
print(nps_and_egm.shape)

#save output to file
outfilePath = outDir + "survey/w_nps_egm" + exported_date + ".csv"
(nps_and_egm.replace('\n',' ', regex=True)
    .to_csv(outfilePath, sep = ",",  index = False, header = True , encoding = "utf-8", line_terminator='\n')
)


# %%
# nps_egm = nps_egm.query("wave == 1")
cust_egm_chng = nps_and_egm.groupby(by = ["Focus_Group","Organisation Legal Name_", "Responses"])["cluster_prev", "cluster_aftr"].max().reset_index()

#map engagement intensity level
egm_mapping = {0:"Low", 1:'Medium', 2: 'High'}

cust_egm_chng['cluster_prev'] = cust_egm_chng['cluster_prev'].map(egm_mapping)
cust_egm_chng['cluster_aftr'] = cust_egm_chng['cluster_aftr'].map(egm_mapping)

#filter only customers that existing in both snapshots
cust_egm = cust_egm_chng.query("cluster_prev == cluster_prev & cluster_aftr == cluster_aftr")


# %%
#of those customers who responded, we want to see what their engagments look like
df = pd.DataFrame(cust_egm.query("Responses == 1"))
print("No. of customers responded (by Focus Group): ")
print(df.groupby(by = ["Focus_Group"])["Organisation Legal Name_"].nunique())

print("==========================================")
egm_chng  = df.pivot_table(index = ["Focus_Group","cluster_prev","cluster_aftr"], values = "Organisation Legal Name_",  aggfunc= "count" ).reset_index()
print(egm_chng)
#egm_chng = egm_chng.pivot(index = ["Focus_Group","cluster_prev"], values ="Organisation Legal Name_", columns = "cluster_aftr").reset_index()

# (egm_chng.replace('\n',' ', regex=True)
#     .to_excel("data//survey//egm_chng_FY21.xlsx",  index = False, header = True , encoding = "utf-8")
# )


# %%
df_egm_before = (
                    df.groupby(by= ["Focus_Group", "cluster_prev"])["Organisation Legal Name_"].nunique().reset_index()
                    .pivot(index = "Focus_Group", columns= "cluster_prev", values = "Organisation Legal Name_" ).reset_index()
                    .assign(cluster = "prev")
)
df_egm_aftr= (
                    df.groupby(by= ["Focus_Group", "cluster_aftr"])["Organisation Legal Name_"].nunique().reset_index()
                    .pivot(index = "Focus_Group", columns= "cluster_aftr", values = "Organisation Legal Name_" ).reset_index()
                    .assign(cluster = "aftr")
)
df_egm_change = pd.concat([df_egm_before,df_egm_aftr])
(df_egm_change.replace('\n',' ', regex=True)
    .to_excel("data//survey//egm_chng_FY21.xlsx",  index = False, header = True , encoding = "utf-8")
)

# %% [markdown]
# Comparing  FY20 (wave3) and FY21 (wave1+2)

# %%
#NPS_FY20 both q1 and q3 - contains al respondents for both q1 and q3
nps_q1q3_fy20    = pd.read_excel('C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave2_FY21//21-03-08 FY21_w2_data.xlsx',
                        sheet_name='FY20_Q1_Q3_AllWaves(src)')

print(nps_q1q3_fy20.shape)



# %%

nps_q1_fy20 = nps_q1q3_fy20.query("`NPS (Q1)`.notnull()")
print(nps_q1_fy20.shape)

nps_q1_fy20 =pd.DataFrame(
                nps_q1_fy20[["Responses","NPS (Q1)", "# NPS Raw Score", "Survey Tracker Answer Respondent Name"
                            , "Survey Tracker Answer Response", "Contact.Wave.Year","Wave.Year"]]
                .rename(columns= {'Survey Tracker Answer Respondent Name': "Contact_Name"})
                .fillna('')
                )

nps_q1_fy20['nps_category'] = nps_q1_fy20['NPS (Q1)'].map(nps_mapping)
nps_q1_fy20['Contact_Key'] = nps_q1_fy20['Contact.Wave.Year'].str.split("-wave").str[0]
nps_q1_fy20.shape


# %%
(df_chng.groupby(["nps_category_x", "nps_category_y"])["Contact_Name"].agg(contact_count ='count')
.reset_index()
.pivot(index = "nps_category_x", columns = "nps_category_y", values = "contact_count")
)


# %%
#### Of all contacts we have ever invited, what were their response across all waves
#####################################################################################
#### output:
#### contact_with_nps | wave 1 | wave 2| wave 3| wave 4| wave 5

#map wave number
w_order_mapping = {'wave 1 - 2020':"w_1", 'wave 2 - 2020':'w_2', 'wave 3 - 2020': 'w_3'
                    ,'wave 1 - 2021':"w_4", 'wave 2 - 2021':'w_5', 'wave 3 - 2021': 'w_6'}

nps_FY20 = pd.DataFrame(nps_q1_fy20[["Contact_Key", "Wave.Year", "nps_category"]])
nps_FY21  = pd.DataFrame(nps_q1_fy21[["Contact_Key", "Wave.Year", "nps_category"]])

nps_FY20_21 = pd.concat([nps_FY20, nps_FY21])
nps_FY20_21["w_"] = nps_FY20_21["Wave.Year"].map(w_order_mapping)
#1580 contacts ever responded at least one waves

nps_FY20_21_pivot = (nps_FY20_21.pivot_table(index = "Contact_Key"
                        , columns = "w_", values = "nps_category", aggfunc= "max" ).reset_index()
 #                       .fillna('')
)
#find columns starts with w_ to count number of total response waves
idx = nps_FY20_21_pivot.columns.str.startswith('w_')
nps_FY20_21_pivot['total_waves']  = nps_FY20_21_pivot.iloc[:, idx].count(axis = 1)

#combine w1 and w2 results for both years
nps_FY20_21_pivot['w_1or2'] = nps_FY20_21_pivot['w_2'].fillna(nps_FY20_21_pivot['w_1'])
nps_FY20_21_pivot['w_4or5'] = nps_FY20_21_pivot['w_5'].fillna(nps_FY20_21_pivot['w_4'])

## Add company details from latest invite (data source = ECT.ExportCustomerTracker)
ect_master_FY21w3 = ect_master.query("Fiscal_Year == 2021 & wave == 3")

ect_master_FY21w3_unq = (ect_master_FY21w3.drop(['Target_Market_Name', 'Market_Region', 'BDM', 'TC','Market_Stage', 'TM_start_Date'], axis = 1)
    .drop_duplicates()
)

nps_across_all_w_company_dtl = nps_FY20_21_pivot.merge(ect_master_FY21w3_unq, how = 'left', on = 'Contact_Key').fillna('')
nps_across_all_w_company_dtl.to_excel(outDir + "nps_6waves.xlsx", header = True)

# %%
# look into those 6 waves
nps_score_mapping = {"Passive":0 , 'Detractor': -100,  'Promoter':100}

nps_4waves = pd.DataFrame(nps_across_all_w_company_dtl).drop(['w_1','w_2','w_4','w_5'], axis =1)

#convert to numbers so we can sort and group for analysis
idx = nps_4waves.columns.str.startswith('w_')
nps_4waves.loc[:,idx] = nps_4waves.iloc[:,idx].replace(nps_score_mapping)

# wave_col  = [col for col in nps_4waves if col.startswith('w_')] ## it doesn't return the right order
wave_col = ['w_1or2', 'w_3','w_4or5','w_6']
nps_4waves[wave_col] = nps_4waves[wave_col].apply(pd.to_numeric)
nps_4waves['nps_sum'] = nps_4waves[wave_col].fillna(0).sum(axis = 1)

#%%
# Heatmap over 6 waves
from matplotlib.colors import LinearSegmentedColormap
def heatmap_contacts_vs_response(df):
        # get the tick label font size
        fontsize_pt = 16    # plt.rcParams['ytick.labelsize']
        dpi = 72.27

        # comput the matrix height in points and inches
        matrix_height_pt = fontsize_pt * df.shape[0]
        matrix_height_in = matrix_height_pt / dpi

        # compute the required figure height
        top_margin = 0.04  # in percentage of the figure height
        bottom_margin = 0.04 # in percentage of the figure height
        figure_height = matrix_height_in / (1 - top_margin - bottom_margin)

        # build the figure instance with the desired height
        fig, ax = plt.subplots(
                figsize=( figure_height,5),
                gridspec_kw=dict(top=1-top_margin, bottom=bottom_margin))


        # cmap = colors.ListedColormap(['black','blue','white','red'])

        #plot
        num_colors = 3
        colors = ['red', 'orange', 'green']
        cmap = LinearSegmentedColormap.from_list('', colors, num_colors)
        ax = sns.heatmap(df.transpose(), cmap=cmap, vmin=-100, vmax=100) #, annot=True, fmt='.0f', annot_kws={'rotation': 90})

        #ax = sns.heatmap( df.transpose(),  cmap="vlag_r" , vmin = -100, vmax = 100  ) # , cbar=True

        return
#call the heatmap
df_cols = wave_col + ['nps_sum', 'total_waves']
#%%
heatmap_df = nps_4waves.query("Focus_Group == 'New Focus'").sort_values(["nps_sum"], ascending = False)
heatmap_contacts_vs_response(heatmap_df[wave_col])


# %%
Old_focus_df = nps_4waves.query("Focus_Group == 'Established Focus'").sort_values(["nps_sum"], ascending = False)
heatmap_contacts_vs_response(Old_focus_df[wave_col])

# %%
h_intensity_df = nps_4waves.query("Contact_Type == 'Primary'").sort_values(["nps_sum"], ascending = False)
heatmap_contacts_vs_response(h_intensity_df[wave_col])


# %%
## 3.  Read CF Engagment Intensity
with open('scripts//ect_CMIntensityModel.sql', 'r') as sql_file:
        egm_intensity = pd.read_sql(sql_file.read(), cnxn)

egm_intensity =  egm_intensity.rename(columns = { 'Legal_Name':'Organisation_Name'
                                , 'Org_Key': 'Organisation_key' })
egm_intensity.columns.to_list()


#%%
##3.1 adding Intensity to the master_contacts_list
nps_4waves_egm = pd.merge(nps_4waves
                            , egm_intensity#[['Organisation_key', 'IntensityScore_Opt1']]
                            , how = 'left', on = 'Organisation_key')#.fillna("Undefined")


nps_4waves_egm.columns.to_list()
# %%
h_intensity_df = nps_4waves_egm.query("IntensityScore_Opt1 == 'High'").sort_values(["nps_sum"], ascending = False)
heatmap_contacts_vs_response(h_intensity_df[wave_col])

# %%
m_intensity_df = nps_4waves_egm.query("IntensityScore_Opt1 == 'Med'").sort_values(["nps_sum"], ascending = False)
heatmap_contacts_vs_response(m_intensity_df[wave_col])

# %%
l_intensity_df = nps_4waves_egm.query("IntensityScore_Opt1 == 'Light'").sort_values(["nps_sum"], ascending = False)
heatmap_contacts_vs_response(l_intensity_df[wave_col])

#%%
# %%
# prepare data for plotting movement between FYs
# Of those who were invited in w3fy21, 338 contacts have at least 1 response in both FYs
# # output:
# contact_Name | FY20 | FY21
import numpy as np
filter_col = ['Contact_Name', 'w_1or2', 'w_3', 'w_4or5', 'w_6', 'Focus_Group']

nps_mvmnt = (pd.DataFrame(nps_across_all_w_company_dtl).drop(['w_1','w_2','w_4','w_5'], axis =1)
                .query("wave != ''")[filter_col]
                .replace('',np.nan)
            )
nps_mvmnt['fy_21'] = nps_mvmnt['w_6'].fillna(nps_mvmnt['w_4or5'])

nps_mvmnt['fy_20'] = nps_mvmnt['w_3'].fillna( nps_mvmnt['w_1or2'])
nps_mvmnt_df = (nps_mvmnt.drop(['w_1or2', 'w_3'], axis = 1)[['Contact_Name', 'fy_20','fy_21']]
                    .dropna(axis = 0)
                )

csv_for_viz = nps_mvmnt_df.groupby(by = ['fy_20','fy_21']).size().reset_index(name = "value")
csv_for_viz['source'] = csv_for_viz['fy_20'] + "(FY20)"
csv_for_viz['target'] = csv_for_viz['fy_21'] + "(FY21)"
csv_for_viz[['source','target','value']].to_csv(outDir + "nps_movement_fy20_fy21.csv", sep = ",", index = False, header = True)

#%%
nps_mvmnt_cohort_df = (nps_mvmnt.drop(['w_1or2', 'w_3'], axis = 1)[['Contact_Name', 'Focus_Group', 'fy_20','fy_21']]
                    .dropna(axis = 0)
                )
nps_mvmnt_cohort_df.groupby(by = [ 'fy_20', 'fy_21', 'Focus_Group']).size().reset_index(name = "value")

#%%
## intensity vs. advocacy (NPS)
# src: contact_key | w3_FY21 nps | intensity

# look into those 6 waves
intensity_mapping = {"Light":0 , 'Med': 1,  'High':2}

# w_6_df = pd.DataFrame(nps_4waves_egm)
# nps_intensity = (pd.DataFrame(w_6_df)[['Contact_Name', 'w_6', 'IntensityScore_Opt1']]
#                 .query("w_6 == w_6")

#             )
w_6_df= (pd.DataFrame(nps_q1_fy21)
        .query("wave == 3")[['Contact_Name', 'Contact_Key', 'Survey Tracker Answer Response']]
        .merge(ect_master_FY21w3_unq[['Organisation_key','Contact_Key']], how = 'left', on = 'Contact_Key')
        .merge( egm_intensity[['Organisation_key', 'IntensityScore_Opt1']]
                             , how = 'left', on = 'Organisation_key')#.fillna("Undefined")
        .rename(columns= {'Survey Tracker Answer Response': 'w_6'})
         )
nps_intensity = (pd.DataFrame(w_6_df)[['Contact_Name', 'w_6', 'IntensityScore_Opt1']]
                .query("w_6 == w_6")

            )
nps_intensity['intensity'] = nps_intensity['IntensityScore_Opt1'].map(intensity_mapping)

print("correlation")
print(nps_intensity['w_6'].corr(nps_intensity['intensity']))

import seaborn as sns
import matplotlib.pyplot as plt
#sns.lmplot(y ="w_6", x="intensity", data=nps_intensity, x_estimator=np.mean)

sns.regplot(y ="w_6", x="intensity", data=nps_intensity)

#pivot output
nps_intensity_pvt = (nps_intensity.groupby(by = ["IntensityScore_Opt1",'w_6'])["Contact_Name"].size().reset_index(name = "count")
                    #.pivot_table(index = "IntensityScore_Opt1", columns = 'w_6', values = 'count')
)


#%%
w_6_nps_intensity = (pd.DataFrame(nps_4waves_egm)[['Contact_Name', 'Contact_Key','w_6'
                        , 'Final_Score'
                        , 'IntensityScore_Opt1', 'TargetMarket_Count', 'IGF']]
                .query("w_6 == w_6")
                .merge(nps_q1_fy21[['Contact_Key', 'Survey Tracker Answer Response', 'wave']]
                    , how = 'left',on = 'Contact_Key')
                .query("wave ==3")

            )
w_6_nps_intensity['intensity'] = w_6_nps_intensity['IntensityScore_Opt1'].map(intensity_mapping)

w_6_nps_intensity['has TM'] = (pd.to_numeric(w_6_nps_intensity['TargetMarket_Count'] + "").fillna(0)
                                .map(lambda x: 1 if x> 0 else 0)
)
w_6_nps_intensity['has igf'] = w_6_nps_intensity['IGF'] + ""


# %%
w_6_nps_intensity['has igf'] = (w_6_nps_intensity['has igf']
                    .isna()
                    .apply(lambda x: False if x== True else True)
                    .astype(int)
)
w_6_nps_intensity['igf_TM'] = w_6_nps_intensity['has igf'] + w_6_nps_intensity['has TM']
w_6_nps_intensity['has igf or TM'] = w_6_nps_intensity['igf_TM'].map(lambda x: 1 if x > 0 else 0)
w_6_nps_intensity['nps_cat'] = w_6_nps_intensity['w_6']/100
w_6_nps_intensity['Final_Score'] = pd.to_numeric(w_6_nps_intensity['Final_Score'])

print("correlation")
print(w_6_nps_intensity['w_6'].corr(w_6_nps_intensity['intensity']))
pd.set_option('float_format', '{:.2f}'.format)

corr = w_6_nps_intensity[['intensity','has igf','has TM','has igf or TM','nps_cat', 'Survey Tracker Answer Response']].corr()

#sns.heatmap(corr, annot= True)
sns.regplot(y ="Survey Tracker Answer Response", x="Final_Score", data=w_6_nps_intensity)

#%%
# output:
# 204 contacts have at least 1 response in wave 6 and fy20
# contact_Name | FY20 | wave_6

# *********not enough sample *******************
# # # nps_mvmnt = (pd.DataFrame(nps_across_all_w_company_dtl).drop(['w_1','w_2','w_4','w_5'], axis =1)
# # #                 .query("wave != '' and w_6 != '' ")[filter_col]
# # #                 .replace('',np.nan)
# # #             )

# # # nps_mvmnt['fy_20'] = nps_mvmnt['w_3'].fillna( nps_mvmnt['w_1or2'])
# # # nps_mvmnt_df = nps_mvmnt.drop(['w_1or2', 'w_3'], axis = 1)[['Contact_Name', 'fy_20','w_6']]

# # # nps_mvmnt_df[['Contact_Name', 'fy_20', 'w_6']].dropna(axis = 0).shape

#%%

# nps_chng =  pd.merge( nps_before, nps_after, how = "inner", on = "Contact_Key")

# #for those contacts who've got double invited, we pick the latest one
# df_chng = nps_chng.sort_values(["Contact_Key","Wave.Year_x", "Wave.Year_y"], ascending= True)
# df_chng["wave_index"] = df_chng.groupby(["Contact_Key","Wave.Year_x"])["Wave.Year_y"].transform('max')
# df_chng = df_chng.query("`Wave.Year_y` == wave_index").drop("wave_index", axis = 1)

# #save to file
# df_chng.to_excel(outDir +"//nps_chng_FY21w3.xlsx",  index = False, header = True , encoding = "utf-8")


# %%
