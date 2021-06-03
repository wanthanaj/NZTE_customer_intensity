#%%
import logging

import azure.functions as func
import pyodbc
import struct
import os 
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential,  ClientSecretCredential

# sql_server = environ.get("SQL_SERVER")
# db_name = environ.get("DB_NAME")

db_name = "prdCRMReport"
sql_server = "tcp:ausse-nzte-sqlrepp1.database.windows.net"
   
credential = DefaultAzureCredential()

#%%
def get_sql_credential():
    
    TENANT_ID = os.environ.get('AZURE_TENANT_ID')
    CLIENT_ID = os.environ.get('AZURE_CLIENT_ID')
    CLIENT_SECRET = os.environ.get('AZURE_CLIENT_SECRET')

    _credential = ClientSecretCredential(
                    tenant_id = TENANT_ID,
                    client_id = CLIENT_ID,
                    client_secret = CLIENT_SECRET
                    )
    
    KEYVAULT_URL = os.environ.get('VAULT_URL')
    _sc = SecretClient(vault_url= KEYVAULT_URL, credential = credential)

    print('Get Secret Client         |')
    sql_username = _sc.get_secret("sqlrepp1-prdCRMReport-username").value
    sql_password = _sc.get_secret("sqlrepp1-prdCRMReportpassword").value
        
    logging.info('SQL_username %s', sql_username)    

    return (sql_username, sql_password)
    
    # conn_string = f"Driver={{ODBC Driver 17 for SQL Server}};SERVER={sql_server}.database.windows.net;DATABASE={db_name}"
    
    # return pyodbc.connect(conn_string, attrs_before = { 1256:token_struct })#set up db connection
#%%
def get_connection(username, password):
    logging.info('Getting connection...        |')

    driver = '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER=' + driver +';SERVER='+sql_server+';DATABASE='+db_name+';UID='+username+';PWD='+ password)
    
    return cnxn
#%%

# %%

# %%
