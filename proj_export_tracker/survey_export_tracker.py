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
outDir  = 'data//'

#connections
server = 'tcp:ausse-nzte-sqlrepp1.database.windows.net'
database = 'prdCRMReport'
driver = '{ODBC Driver 17 for SQL Server}'
username = 'prdCRMReport_Admin_Read'
password = '?ZvEHun3PE$E793G'


# %%
#set up db connection
cnxn = pyodbc.connect('DRIVER=' + driver +';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

####################################################################################
#Prepare investor profiles #
 
#Extract Investor list
## read from database
with open('scripts//ect_master.sql', 'r') as sql_file:
        ect_master= pd.read_sql(sql_file.read(), cnxn)

ect_master.shape


# %%
ect_master_FY21 = ect_master.query("Fiscal_Year == 2021")
ect_master_FY21.shape


# %%
#NPS_FY21 both q1 and q3 - contains al respondents for both q1 and q3
nps_q1q3_fy21    = pd.read_excel('C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave2_FY21//21-03-08 FY21_w2_data.xlsx',
                        sheet_name='FY21_q1_q3_w1w2(src)')
# nps_q3_fy21.dropna(axis = 0, how ="all", inplace = True)
nps_q1q3_fy21.shape


# %%
#question 1
nps_q1_fy21 = nps_q1q3_fy21.query("`NPS (Q1)`.notnull()")
print("q1: " , str(nps_q1_fy21.shape))
#question 3
nps_q3_fy21 = nps_q1q3_fy21.query("`NPS (Q3)`.notnull()")
print("q3: " , str(nps_q3_fy21.shape))

nps_q1_fy21 =pd.DataFrame(
                nps_q1_fy21[["Responses","NPS (Q1)", "# NPS Raw Score", "Survey Tracker Answer Respondent Name"
                            , "Survey Tracker Answer Response", "Wave.Year"]]
                .rename(columns= {'Survey Tracker Answer Respondent Name': "Contact_Name"})
                .fillna('')
                )
nps_q1_fy21["wave"] = pd.to_numeric(nps_q1_fy21["Wave.Year"].str[5:7])
nps_q1_fy21["Fiscal_Year"] = pd.to_numeric(nps_q1_fy21["Wave.Year"].str[-4:])


nps_q3_fy21 =pd.DataFrame(
                nps_q3_fy21[["Responses","NPS (Q3)", "# NPS Raw Score", "Target Market", "Survey Tracker Answer Respondent Name"
                            , "Survey Tracker Answer Response", "Wave.Year"]]
                .rename(columns= {'Survey Tracker Answer Respondent Name': "Contact_Name"
                                , 'Target Market': "Target_Market_Name"})
                .fillna('')
                )
nps_q3_fy21["wave"] = pd.to_numeric(nps_q3_fy21["Wave.Year"].str[5:7])
nps_q3_fy21["Fiscal_Year"] = pd.to_numeric(nps_q3_fy21["Wave.Year"].str[-4:])

# join master file with q1 to identify who responded or not
all_invited_w_nps = (ect_master_FY21.merge(nps_q1_fy21, how = "left", on = ["Contact_Name", "wave", "Fiscal_Year"])
                        .fillna('')                        
                        .drop(["Wave.Year"], axis = 'columns')
                    )
# all_invited_w_nps = (all_invited_w_nps.merge(nps_q3_fy21, how = "left", on = ["Contact_Name", "wave", "Target_Market_Name", "Fiscal_Year"])
#                         .fillna('')                        
#                         .drop(["Wave.Year"], axis = 'columns')
#                     )
print(all_invited_w_nps.shape)


outfilePath = "data//survey//fy21_all_invited_w_nps_20210310.xlsx"
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

print(egm_chng)
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
#NPS_FY21 both q1 and q3 - contains al respondents for both q1 and q3
nps_q1q3_fy20    = pd.read_excel('C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//Survey//Wave2_FY21//21-03-08 FY21_w2_data.xlsx',
                        sheet_name='FY20_Q1_Q3_AllWaves(src)')

print(nps_q1q3_fy20.shape)



# %%

#map engagement intensity level
nps_mapping = {0:"Passive", -100:'Detractor', 100: 'Promoter'}


nps_q1_fy20 = nps_q1q3_fy20.query("`Wave.Year` == 'wave 3 - 2020' & `NPS (Q1)`.notnull()")
print(nps_q1_fy20.shape)

nps_q1_fy20 =pd.DataFrame(
                nps_q1_fy20[["Responses","NPS (Q1)", "# NPS Raw Score", "Survey Tracker Answer Respondent Name"
                            , "Survey Tracker Answer Response", "Wave.Year"]]
                .rename(columns= {'Survey Tracker Answer Respondent Name': "Contact_Name"})
                .fillna('')
                )

nps_q1_fy20['nps_category'] = nps_q1_fy20['NPS (Q1)'].map(nps_mapping)
nps_q1_fy20                


# %%
nps_q1_fy21['nps_category'] = nps_q1_fy21['NPS (Q1)'].map(nps_mapping)

nps_before = pd.DataFrame(nps_q1_fy20[["Contact_Name", "Wave.Year", "nps_category"]])
nps_after = pd.DataFrame(nps_q1_fy21[["Contact_Name", "Wave.Year", "nps_category"]])


nps_chng =  pd.merge( nps_before, nps_after, how = "inner", on = "Contact_Name")

#for those contacts who've got double invited, we pick the latest one 
df_chng = nps_chng.sort_values(["Contact_Name","Wave.Year_x", "Wave.Year_y"], ascending= True)
df_chng["wave_index"] = df_chng.groupby(["Contact_Name","Wave.Year_x"])["Wave.Year_y"].transform('max')
df_chng = df_chng.query("`Wave.Year_y` == wave_index").drop("wave_index", axis = 1)

#save to file
df_chng.to_excel("data//survey//nps_chng_FY21.xlsx",  index = False, header = True , encoding = "utf-8")


# %%
(df_chng.groupby(["nps_category_x", "nps_category_y"])["Contact_Name"].agg(contact_count ='count')
.reset_index()
.pivot(index = "nps_category_x", columns = "nps_category_y", values = "contact_count")
)


# %%



