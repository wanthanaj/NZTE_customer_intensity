#%%
import os
os.chdir("..")

# %%
import pyodbc as pyodbc
import pandas as pd

pd.set_option('float_format', '{:.0f}'.format)
# Folder that hold outputs
outDir  = 'data//'
scriptDir = 'src//'


#%% create DB connection
from src.azure_db import get_connection_hardcode, get_sql_credential, get_connection
# username , password = get_sql_credential()
# cnxn = get_connection(username, password)
cnxn = get_connection_hardcode()

# %%

with open('scripts/snapshot_target_markets.sql', 'r') as sql_file:
        tm_snapshots = pd.read_sql(sql_file.read(), cnxn)

#%%  markets that have ever been turned on as target market:  2655 tms
tms_all = (tm_snapshots.groupby(by = ['customer_key', 'customer_name'
                        , 'customer_market_name'])['tm_start_date','tm_end_date'].nunique()
                .reset_index()
                )
tms_all

#%% about 7% (n= 175) have been turned back on e.g. more than one start date
tms_on_off =  tms_all.query("tm_end_date > 0 and tm_start_date > 1").copy()
tms_on_off['customer_market_name'].value_counts()

#next steps : how many of this are currently active?

#%% RETENTION

tms_all_list = ( tm_snapshots.query("customer_market_name == 'Thailand'")
                .drop(['SnapshotNZDate','cm_or_tm'], axis = 'columns')
                .drop_duplicates(subset = ['customer_key', 'customer_market_name', 'tm_start_date', 'tm_end_date'])
                        )
tms_all_list['tm_start_date'] = pd.to_datetime(tms_all_list['tm_start_date'])
tms_all_list['tm_end_date'] = pd.to_datetime(tms_all_list['tm_end_date'])

tms_all_list['tm_start_MY'] = pd.to_datetime(tms_all_list['tm_start_date']).dt.to_period('M')
tms_all_list['tm_end_MY'] = pd.to_datetime(tms_all_list['tm_end_date']).dt.to_period('M')

tms_all_list.sort_values('customer_name').head()

# tms_all_list.pivot_table( index = 'tm_start_MY'
#                 , columns = 'tm_end_MY', values = 'customer_key')

# %%
th_tms = tm_snapshots.query("customer_market_name == 'Thailand'").copy()

th_tms.sort_values(['w_end_date_count','customer_name'], ascending = False)

th_tms['w_start_date_count'] = th_tms.groupby(by = ['customer_market_name', 'customer_name'])['tm_start_date'].transform('nunique')
th_tms['w_end_date_count'] = th_tms.groupby(by = ['customer_market_name', 'customer_name'])['tm_end_date'].transform('nunique')

th_tms_w_end_date = th_tms.query("w_end_date_count > 0")
th_tms_w_end_date.sort_values('customer_name')

# %%

th = (th_tms_w_end_date.drop('SnapshotNZDate', axis = 'columns')
        .drop_duplicates(subset = ['customer_key', 'customer_market_name', 'tm_start_date', 'tm_end_date'])
        )
# , 'tm_start_date','tm_end_date', 'w_start_date_count', 'w_end_date_count'].unique()
th.sort_values('customer_name')

#%%
th.sort_values('customer_name')
# %%
