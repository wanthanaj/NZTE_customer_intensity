#%%
import os
path="C://Users//wanthana.j.app//OneDrive - New Zealand Trade and Enterprise//src//customer-intensity//"
os.chdir(path)
print(os.getcwd())

# %%
#Customer Intensification (overtime)
import pyodbc as pyodbc
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option('float_format', '{:.0f}'.format)

#connections
server = 'tcp:ausse-nzte-sqlrepp1.database.windows.net'
database = 'prdCRMReport'
driver = '{ODBC Driver 17 for SQL Server}'
username = 'prdCRMReport_Admin_Read'
password = '?ZvEHun3PE$E793G'

cnxn = pyodbc.connect('DRIVER=' + driver +';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
#%%
#READ DATA FROM SCRIPTS
snapshot_prev = "2020-07-02"
snapshot_mid = "2020-10-02"
snapshot_post = "2021-05-02"
#%%
with open('scripts/snapshot_traffic_light.sql', 'r') as sql_file:
        focus_snapshots = pd.read_sql(sql_file.read(), cnxn)

with open('scripts/snapshot_cashflow_clinic.sql', 'r') as sql_file:
        obj_cf_snapshots = pd.read_sql(sql_file.read(), cnxn)

with open('scripts/snapshot_igf.sql', 'r') as sql_file:        
        igf_snapshots = pd.read_sql(sql_file.read(), cnxn)

with open('scripts/snapshot_actions_by_type.sql', 'r') as sql_file:
        svc_action_snapshots = pd.read_sql(sql_file.read(), cnxn)


#%%
traffic_light_dic = {'Black':0 , 'Red':1 ,  'Orange': 2 , 'Green':3 , None: -1}
snapshot_dic = {snapshot_prev: "Pre", snapshot_post: "Post", snapshot_mid : "Mid"}


#add mapping feature
focus_snapshots['traffic_light_id'] = focus_snapshots["covid traffic light"].map(traffic_light_dic)
focus_snapshots['state'] = focus_snapshots["SnapshotNZDate"].map(snapshot_dic)

#%%
focus_2snapshots = focus_snapshots.query("state != 'Mid'").copy()
focus_2snapshots["snapshot_count"] = focus_2snapshots.groupby('Organisation Key')["state"].transform('count')
                                        

print(focus_2snapshots.info)


#%%

focus_w_2snapshots = focus_2snapshots.query("snapshot_count ==2")
print(focus_w_2snapshots["covid traffic light"].value_counts())

#%%
print(focus_snapshots["Organisation Key"].nunique())           #1458 customers
print(focus_w_2snapshots["Organisation Key"].nunique())        #1353 customers (July) , 1367 (August) , 1409 (Nov)

#%%
#pivot to before and after for comparison
#output: pivot table
focus_tl_chng = (focus_w_2snapshots.pivot(index = ['Organisation Key'], columns = 'state'
                                , values  = 'traffic_light_id')
                                .fillna(0)
                                .reset_index()
                )

bf_vs_aftr = focus_tl_chng.groupby(by = ["Pre", "Post"])["Organisation Key"].count().unstack()
bf_vs_aftr["sum"] = bf_vs_aftr.sum(axis = 1)
bf_vs_aftr


# %%
tl_bf = focus_w_2snapshots.query("SnapshotNZDate == '" + snapshot_prev + "'")
tl_aftr = focus_w_2snapshots.query("SnapshotNZDate == '" + snapshot_post + "'")

# %%
(
focus_snapshots.groupby(by = ["SnapshotNZDate", "traffic_light_id"])
        ["Organisation Key"].count()
        .reset_index()        
        #.unstack(-1)

)

#%%
################ add ALL SERVICE ACTIONS count by type to the original dataset ################ 
def service_actions_transform(df):
        #output: a summary total svc_actions by type (one column one service type) for each customers
        df["state"] = df["SnapshotNZDate"].map(snapshot_dic)
        service_actions = (df.query("`Action Service Type`.notnull()")
                        .groupby(by =  ["Organisation Key",  "state", "Action Service Type"]).sum().reset_index()
                        .pivot(index = ["Organisation Key",  "state"], columns= "Action Service Type", values = "actions_all") 
                        .reset_index()
                        .fillna(0)                                            
                        )   
        service_actions["action_total"] = service_actions.sum(axis = 1)

        #add prefix (svc_) to all services columns
        a = [ 'svc_'+str(col)  for col in service_actions.columns[2:]]
        service_actions.columns = service_actions.columns[:2].tolist() + a

        return service_actions

def non_svc_action_transform(df):
        df["state"] = df["SnapshotNZDate"].map(snapshot_dic)
        non_svc_actions = (df.query("`Action Service Type`.isnull()")
                        .groupby(by =  ["Organisation Key",  "state"]).sum().reset_index()
                        .reset_index()
                        .fillna(0)                                            
                        )           
        return non_svc_actions

non_svc_action_transform(svc_action_snapshots).shape

def igf_transform(df):
        df["state"] = df["SnapshotNZDate"].map(snapshot_dic)
        igf_all = (df.groupby(by = ["Organisation Key", "state"])
                        [["active IGFs", "Total_Fund_Amount_Approved"]].sum()
                        .reset_index()
                        .fillna(0)
                )
        
        return igf_all


# %% MERGE DATA
#objective cashflow clinic 
#output: a long format of customer traffic light info + cashflow clinic

#pivot df to show values side by side (pre v.s. post)
obj_cf_snapshots["state"] = obj_cf_snapshots["SnapshotNZDate"].map(snapshot_dic)
df_compared = pd.merge(focus_w_2snapshots
                , obj_cf_snapshots[["Organisation Key", "obj_cf_clinic_aftr", "obj_cf_clinic_all", "state"]]
                , how = "left"
                , on = ["Organisation Key", "state"])
print("add cashflow clinic: ")
print(df_compared.shape)

igf_summary = igf_transform(igf_snapshots)
df_compared = pd.merge(df_compared
                , igf_summary[["Organisation Key", "active IGFs", "Total_Fund_Amount_Approved", "state"]]
                , how = "left"
                , on = ["Organisation Key", "state"])
print("add IGF: ")
print(df_compared.shape)

non_svc_actions_summary =  non_svc_action_transform( svc_action_snapshots)
df_compared = (pd.merge(df_compared
                , non_svc_actions_summary
                , how = "left"
                , on = ["Organisation Key", "state"])
                .fillna(0)
        )
print("add non-service actions: ")
print(df_compared.shape)

svc_actions_summary = service_actions_transform( svc_action_snapshots)
df_compared = (pd.merge(df_compared
                , svc_actions_summary
                , how = "left"
                , on = ["Organisation Key", "state"])
                .fillna(0)
        )
print("add service actions: ")
print(df_compared.shape)

#%%

#%%
#PREPARE columns for traforming from long format to wide format
#output: (a wide format) comparing before and after for each metric e.g. traffic light, cashflow clinic

def long_to_wide(long_df):
        wanted_cols = pd.concat([long_df.iloc[:, long_df.columns.get_loc('traffic_light_id')],
                        long_df.iloc[:, long_df.columns.get_loc('obj_cf_clinic_all'):long_df.columns.get_loc('index')],
                        long_df.iloc[:, long_df.columns.get_loc('actions_all'):]]
                        , axis = 1).columns.tolist()

        wide_df = (long_df.pivot(index = ["Organisation Key", "Organisation Legal Name"]
                                        , values =  wanted_cols
                                        , columns = ["state"])                                
                                        .reset_index()
                                        .fillna(0)     # leave those without earlier snapshot blank
        )
        wide_df
        # #rename column names 
        wide_df.columns = [f'{i}_{j}' for i,j in wide_df.columns]
        wide_df.columns.to_list()
        wide_df = wide_df.rename(columns = {"Organisation Key_": "Organisation Key"})        
        return wide_df

df_compared_wide = long_to_wide(df_compared)
df_compared_wide.columns.tolist()
#%%
# cohort
# assign cohort by covid traffic light changes
def covid_cohort(x, y):
        if x == -1:           
                return "no pre data"
        elif x  > y :         
                return "worse"
        elif x  < y :         
                return "better"
        elif x  == y and x==3:         
                return "no change green"
        elif x  == y and x==2:         
                return "no change orange"
        elif x  == y and x==1:         
                return "no change red"
        elif x  == y and x==0:         
                return "no change black"

df_compared_wide["covid_cohort"] = df_compared_wide.apply(lambda x : covid_cohort(x["traffic_light_id_Pre"], x["traffic_light_id_Post"]), axis = 1)
df_compared_wide["covid_cohort"].value_counts()

# add the covid_cohort feature back to the original dataset too
df_compared = pd.merge(df_compared
                                , df_compared_wide[["Organisation Key", "covid_cohort"]]
                                , how = "left"
                                , on = ["Organisation Key"])
        
#df = df_with_covid_cohort.query("covid_cohort == 'no pre data'")
#df[["Organisation Legal Name", "state", "covid_cohort", 'covid traffic light', 'traffic_light_id']]


# %%
# output: a column showing changes over time by cohort type

import matplotlib.lines as mlines

def plot_chng_pre_post(df_wide, feature_pre, feature_post
                , feature_group_by, _xlabel
                , _xlim):

        df_raw = pd.DataFrame(df_wide)
        df_raw["_chng"] = df_raw[feature_pre] - df_raw[feature_post]

        df = (df_raw[['_chng'
                , feature_pre, feature_post
                , 'Organisation Key'
                , feature_group_by]]
                .groupby(feature_group_by)
                #["obj_cf_clinic_aftr_Pre", "obj_cf_clinic_aftr_Post", "cf_clinic_chng"].sum()
        )
        #print(df)

        # Import Data
        cust_count = df["Organisation Key"].agg(cust_count="count").reset_index().sort_values("cust_count")
        measures = df.apply(lambda x: x.mean()).reset_index()

        df = pd.merge(cust_count, measures, how = "left", on = feature_group_by).reset_index()
        #declare variable for the measure used in the visual
        vis_measure_from = feature_pre
        vis_measure_to =  feature_post

        # Func to draw line segment
        def newline(p1, p2, color='black'):
                ax = plt.gca()
                l = mlines.Line2D([p1[0],p2[0]], [p1[1],p2[1]], color='skyblue')
                ax.add_line(l)
                return l

        # Figure and Axes
        fig, ax = plt.subplots(1,1,figsize=(8,3), facecolor='#f7f7f7', dpi= 80)

        # Points
        ax.scatter(y=df['index'], x=df[vis_measure_from], s=50, color='#a3c4dc', alpha=0.7)
        ax.scatter(y=df['index'], x=df[vis_measure_to], s=70, color='#0e668b', alpha=0.7)

        # Line Segments
        for i, p1, p2 in zip(df['index'], df[vis_measure_from], df[vis_measure_to]):
                newline([p1, i], [p2, i])

        # Decoration
        ax.set_facecolor('#f7f7f7')
        #ax.set_title("Total actions Mar - Oct", fontdict={'size':18})
        ax.set(xlim=(0,_xlim), xlabel= _xlabel)
        ax.set_yticks(df.index)
        ax.set_yticklabels(df[feature_group_by], fontdict={'horizontalalignment': 'right'})
        plt.show()

plot_chng_pre_post( df_compared_wide,  'svc_action_total_Pre'
        ,  'svc_action_total_Post', 'covid_cohort', 'all svc_actions (Mean)', 10)
plot_chng_pre_post( df_compared_wide,  'actions_all_Pre'
        ,  'actions_all_Post', 'covid_cohort', 'all non-svc actions (Mean)',30)
plot_chng_pre_post( df_compared_wide,  'Total_Fund_Amount_Approved_Pre'
        ,  'Total_Fund_Amount_Approved_Post', 'covid_cohort', 'IGF approved $ (Mean)',50000)


#%%
#summary
df_Post = df_compared.query("state == 'Post'")
print(df_Post["covid_cohort"].value_counts())

# plot a Stacked Bar Chart by Sector
(df_Post.groupby('Sector')['covid_cohort'].value_counts(normalize = True)
    .unstack('covid_cohort').plot.bar(stacked=True)
    .legend(loc = 'center left',bbox_to_anchor=(1.0, 0.5))
)

#sns.barplot(x = "Sector", y = 'Organisation Key', data = df, )
#%%
w_pre_tl_cohort = df_compared.query("covid_cohort != 'no pre data'")
#%%
sns.displot(x =  "actions_all" , col = "covid_cohort", data = w_pre_tl_cohort)
# %%
sns.boxplot(x = "covid_cohort", y = "svc_Beachheads",  data = df_Post)

#%%
sns.displot(x = "actions_all", col = "covid_cohort"
        , row = "Sector", data = w_pre_tl_cohort.query('state == "Post"'))


# %%
sns.boxplot(x ="SnapshotNZDate", y = "actions_all", hue = "covid_cohort", data = w_pre_tl_cohort)



#%%
w_pre_tl_cohort.query('state == "Post"').boxplot(column= 'actions_all'
                                , by='covid_cohort', figsize=(7,6))
    
# %%
w_pre_tl_cohort.info()
# %%
df = (df_compared.groupby(by = ['state','covid_cohort', 'svc_action_total'])["Organisation Key"].count()
                .reset_index()
)

# %%
#IGF --- distribution of active IGFs in each cohort
df = (
df_compared.query("state == 'Post'")
        .groupby(by = ['state','covid_cohort','active IGFs'])["Organisation Key"].count().reset_index()
        .pivot_table(index = [ "active IGFs"], columns = "covid_cohort", values = "Organisation Key")
        .fillna(0)
)
df

#%%
#CF clinic --- distribution of CF Clinic in each cohort
df = (
df_compared.query("state == 'Post'")
        .groupby(by = ['state','covid_cohort','obj_cf_clinic_all'])["Organisation Key"].count().reset_index()
        .pivot_table(index = [ "obj_cf_clinic_all"], columns = "covid_cohort", values = "Organisation Key")
        .fillna(0)
)
df

#%%
def heatmap_cust_vs_feature(df):
        # get the tick label font size
        fontsize_pt = 16    # plt.rcParams['ytick.labelsize']
        dpi = 72.27

        # comput the matrix height in points and inches
        matrix_height_pt = fontsize_pt * df_raw.shape[0]
        matrix_height_in = matrix_height_pt / dpi

        # compute the required figure height 
        top_margin = 0.04  # in percentage of the figure height
        bottom_margin = 0.04 # in percentage of the figure height
        figure_height = matrix_height_in / (1 - top_margin - bottom_margin)

        # build the figure instance with the desired height
        fig, ax = plt.subplots(
                figsize=( figure_height,5), 
                gridspec_kw=dict(top=1-top_margin, bottom=bottom_margin))


        #remove customer with no any of these services
        #df = pd.DataFrame(df_raw[df_raw_col])
        #df = df[(df !=0).any(axis = 1)] #df[(df.T == 0).any(axist = 1)]

        #plot
        ax = sns.heatmap( df.transpose()) #, cmap = "Blues"  ) # , cbar=True

        return

#%%


# %%
#Top services used
#heatmap showing service used
sel_oc_cohort = "worse" #"better"

#filter only services they used: return service name
#prepare data
df_raw = pd.DataFrame(df_compared.query("state == 'Post'"))
df_raw = df_raw[df_raw["covid_cohort"] == sel_oc_cohort]

#get all services columns
col_start = df_raw.columns.get_loc('svc_Beachheads')
col_end = df_raw.columns.get_loc('svc_Sustainability')
val_cols = df_raw.columns[col_start:col_end+1]

#sum all services actions to find the most used services
svc_compared = df_raw[val_cols].agg("sum")
#convert to dataframe and add column names
svc_compared = pd.DataFrame({"svc_name": svc_compared.index , "svc_count": svc_compared.values})
#trim out the prev/aftr to find the name of service
svc_compared["svc_ori"] = svc_compared["svc_name"].str[4:]
#sort by most popular services
svc_compared = svc_compared.sort_values("svc_count", ascending = False)
svc_compared

#finding col_idx of top 5 services used
src_col  = pd.Series(df_raw.columns)
top5_svc_col = svc_compared.head(5)["svc_name"].tolist() #top 5 services
top5_svc_col

df_top5svc = pd.DataFrame(df_raw)
df_top5svc = df_top5svc.set_index("Organisation Legal Name")
df_top5svc[top5_svc_col]

heatmap_cust_vs_feature(df_top5svc[top5_svc_col]==0)

# %%
#heatmap showing service used
sel_oc_cohort = "no change orange" #"better"

#filter only services they used: return service name
#prepare data
df_raw = pd.DataFrame(df_compared.query("state == 'Post'"))
df_raw = df_raw[df_raw["covid_cohort"] == sel_oc_cohort]

wanted_cols = pd.concat([df_raw.iloc[:, df_raw.columns.get_loc('obj_cf_clinic_all')],
                        df_raw.iloc[:, df_raw.columns.get_loc('active IGFs')],
                        df_raw.iloc[:, df_raw.columns.get_loc('actions_all')]]
                        , axis = 1).columns.tolist()

df_othr_svc_usage = pd.DataFrame(df_raw)
df_othr_svc_usage = df_othr_svc_usage.set_index("Organisation Legal Name")
df_othr_svc_usage[wanted_cols] 

heatmap_cust_vs_feature(df_othr_svc_usage[wanted_cols]==0) #white means 0, otherwise black

#%%
#combined all services & IGFs & Cashflow clinics, etc.

#heatmap showing service used
sel_oc_cohort = "worse" #"no change orange" 

#prepare data
df_raw = pd.DataFrame(df_compared.query("state == 'Post'"))
df_raw = df_raw[df_raw["covid_cohort"] == sel_oc_cohort]

wanted_cols = pd.concat([df_raw.iloc[:, df_raw.columns.get_loc('obj_cf_clinic_all')],
                        df_raw.iloc[:, df_raw.columns.get_loc('active IGFs')],
                        df_raw.iloc[:, df_raw.columns.get_loc('actions_all')]]
                        , axis = 1).columns.tolist()
all_cols = wanted_cols + top5_svc_col

df_othr_svc_usage = pd.DataFrame(df_raw)
df_othr_svc_usage = df_othr_svc_usage.set_index("Organisation Legal Name")

df = (df_othr_svc_usage[all_cols] == 0)
df['sum'] = df.sum(axis = 1)
df = df.sort_values("sum", ascending = "false")

heatmap_cust_vs_feature(df[all_cols]) 

#%%
df = (df_othr_svc_usage[all_cols] == 0)
df['sum'] = df.sum(axis = 1)
#%%
df.isnull().sum()

# %%
# 3 all 3 snapshots

#%%
#output: pivot table
df_tfl_chng = (focus_snapshots.pivot(index = ['Organisation Key'], columns = 'state'
                                , values  = 'covid traffic light')
                               .reset_index()
                               .fillna("undefined")
                )

#generate sankey df
df_sankey12 = (df_tfl_chng.groupby(by = ['Pre', 'Mid'])['Organisation Key'].count().reset_index()
                        .rename(columns = {'Pre': 'Source', 'Mid': 'Target'})                        
                )
df_sankey12['Source']   = df_sankey12['Source'] + '(Pre)'
df_sankey12['Target']   = df_sankey12['Target'] + '(Mid)'
df_sankey12


df_sankey23 = (df_tfl_chng.groupby(by = ['Mid', 'Post'])['Organisation Key'].count().reset_index()
                        .rename(columns = {'Mid': 'Source', 'Post': 'Target'})
                )
df_sankey23['Source']   = df_sankey23['Source'] + '(Mid)'
df_sankey23['Target']   = df_sankey23['Target'] + '(Post)'
df_sankey23

df_sankey = pd.concat([df_sankey12, df_sankey23])
(df_sankey.replace('\n',' ', regex=True)
    .to_csv("data//covidFundFY22//tfl_chng.csv", sep = ",",  index = False, header = True , encoding = "utf-8", line_terminator='\n')
)  

# %%
# %%
from sklearn.decomposition import PCA

# %%
#%%
df_to_model = pd.DataFrame(df_compared.query("state == 'Post'"))
df_to_model = df_to_model.query("covid_cohort == 'no change green' or covid_cohort == 'no change orange' or covid_cohort == 'better'")

org_cols = ['Sector', 'Age in Focus (Month)', 'traffic_light_id']
df_to_model = df_to_model[top5_svc_col + wanted_cols + org_cols]
df_to_model.describe(include = 'all')

#%%
from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import PCA
#%%
# Normalize the numeric features so they're on the same scale
scaled_features = MinMaxScaler().fit_transform(df_to_model[df_to_model.columns[0:9]])

# Get two principal components
pca = PCA(n_components=2).fit(scaled_features)
features_2d = pca.transform(scaled_features)
features_2d[0:10]


# %%
df_to_model.info()

# %%
#map categorical to numeric
pd.Sector = pd.Categorical(df_to_model.Sector)
df_to_model['sector_code'] = pd.factorize(df_to_model['Sector'])[0] 
# %%
df_to_model.info()
# %%
