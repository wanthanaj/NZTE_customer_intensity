#%%
import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

os.environ['AZURE_TENANT_ID'] = "209fe2ce-372c-489e-b3fd-066acdf69f09"
os.environ['AZURE_CLIENT_ID'] = "2b8951d7-9211-4e6d-8c01-03909073352d"
os.environ['AZURE_CLIENT_SECRET'] = "MzR-r0HMX4nP-ralw-jFw_7DWG~-U5-03T"
os.environ['VAULT_URL'] = "https://ausse-dev-keyvaulttest.vault.azure.net/"

TENANT_ID = os.environ.get('AZURE_TENANT_ID')
CLIENT_ID = os.environ.get('AZURE_CLIENT_ID')
CLIENT_SECRET = os.environ.get('AZURE_CLIENT_SECRET')
KEYVAULT_URL = os.environ.get('VAULT_URL')
_credential = ClientSecretCredential(
    tenant_id = TENANT_ID,
    client_id = CLIENT_ID,
    client_secret = CLIENT_SECRET
)
_sc = SecretClient(vault_url= KEYVAULT_URL, credential = _credential)

DEMO_ON_USERNAME = _sc.get_secret("Username").value
DEMO_ON_PASSWORD = _sc.get_secret("Password").value

print(DEMO_ON_USERNAME)
print(DEMO_ON_PASSWORD)

# %%
