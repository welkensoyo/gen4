import traceback
import requests
import msal
from API.config import az
from API.cache import sync, retrieve

class AzureSDB:
    def __init__(self):
        self.header = {}
        self.response = ''
        self.client = msal.ConfidentialClientApplication(az.client_id, authority=az.authority, client_credential=az.client_secret)
        self.azure_gen_key()

    def azure_gen_key(self):
        if not self.header:
            if (token_result := self.client.acquire_token_silent(az.scope, account=None) or self.client.acquire_token_for_client(scopes=az.scope)):
                self.header = {'Authorization': f'Bearer {token_result["access_token"]}', 'Content-Type': "application/json"}
                return True
            return False

    def next(self, link):
        return self.transmit(link)

    def get_users(self, refresh=False):
        if not refresh:
            if (users := retrieve('azusers')):
                return users

        url = 'https://graph.microsoft.com/v1.0/users?$select=displayName,userPrincipalName,givenName,surname,mail,jobTitle,department,officeLocation,mobilePhone,userType'
        users = []
        if self.transmit(url):
            users.extend(self.response.json()['value'])
            while self.response.json().get('@odata.nextLink'):
                self.next(self.response.json().get('@odata.nextLink'))
                users.extend(self.response.json()['value'] or [])
        sync('azusers', users)
        return users

    def delete_user(self, email):
        url = f'https://graph.microsoft.com/v1.0/users/{email}'
        if self.transmit(url, mode='delete'):
            return self.response.text
        return False


    def transmit(self, url, meta=None, mode='get'):
        if self.header:
            if mode =='get':
                self.response = requests.get(url, headers=self.header)
            elif mode =='post':
                self.response = requests.post(url, headers=self.header, json=meta)
            elif mode =='delete':
                self.response = requests.delete(url, headers=self.header)
            if self.response.status_code in (200, 204):
                return True
        return False

if __name__ == '__main__':
    from pprint import pprint
    azure = AzureSDB()
    x = azure.get_users()
    for each in x:
        if 'delete' in each['userPrincipalName']:
            print(each['userPrincipalName'])
