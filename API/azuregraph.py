from gevent import sleep
import msal, arrow
from msgraph.generated.models.on_premises_extension_attributes import OnPremisesExtensionAttributes
from API.config import az
from API.cache import sync, retrieve
from API.comms import upool
from API.njson import dc, jc, checkuuid
from pprint import pprint
import logging
logger = logging.getLogger('sdb_app')

APP_ID = '' #client id
OBJECT_ID = ''
DIRECTORY_ID = ''

class AzureSDB:
    def __init__(self):
        self.header = {}
        self.response = ''
        self.client = msal.ConfidentialClientApplication(az.client_id, authority=az.authority, client_credential=az.client_secret)
        self.azure_gen_key()
        self.proxy_aliases = {}

    def azure_gen_key(self):
        if not self.header:
            if token_result := self.client.acquire_token_silent(az.scope, account=None) or self.client.acquire_token_for_client(scopes=az.scope):
                if token_result.get("error"):
                    logger.info(token_result)
                    logger.info('AD Token Expired...')
                    return False
                self.header = {'Authorization': f'Bearer {token_result["access_token"]}', 'Content-Type': "application/json"}
                return True
            return False

    def next(self, link):
        return self.transmit(link)

    def get_user(self, email):
        url = f'https://graph.microsoft.com/beta/users/{email}?$select=id,employeeId,displayName,userPrincipalName,givenName,surname,mail,employeeHireDate,jobTitle,department,officeLocation,mobilePhone,userType,companyName,streetAddress,city,postalCode,state,onPremisesExtensionAttributes,accountEnabled,otherMails,proxyAddresses,'
        if checkuuid(email):
            url = url+'signInActivity,'
        return self.transmit(url)

    def get_users(self, refresh=False, field='userPrincipalName', include_disabled=False):
        if not refresh and field == 'userPrincipalName':
            if (users := retrieve('azusers')):
                return users
        url = 'https://graph.microsoft.com/beta/users?$select=id,employeeId,displayName,userPrincipalName,givenName,surname,mail,employeeHireDate,jobTitle,department,officeLocation,mobilePhone,userType,companyName,streetAddress,city,postalCode,state,onPremisesExtensionAttributes,accountEnabled,otherMails,proxyAddresses,signInActivity,'
        users = {}
        if response := self.transmit(url):
            for u in response['value']:
                if not u['accountEnabled']:
                    continue
                try:
                    users[u[field].lower()] = u
                except:
                    pass
            while response.get('@odata.nextLink'):
                response = self.next(response.get('@odata.nextLink'))
                for u in response.get('value', []):
                    if not u['accountEnabled'] and not include_disabled:
                        continue
                    try:
                        users[u[field].lower()] = u
                    except:
                        pass
        if field == 'userPrincipalName' and not include_disabled:
            sync('azusers', users)
        return users

    def get_ids(self, refresh=False):
        return self.get_users(refresh=refresh, field='employeeId')

    def backup_sync(self):
        sync('azusers_bak', retrieve('azusers'))
        return self

    def restore_sync(self):
        sync('azusers', retrieve('azusers_bak'))
        return self

    def add_email(self, userid, email):
        return self.transmit(f'https://graph.microsoft.com/beta/users/{userid}', meta={'proxyAddresses': [f'SMTP:{email}']}, mode='PATCH')

    def change_login(self, userid, email):
        return self.transmit(f'https://graph.microsoft.com/v1.0/users/{userid}', meta={'userPrincipalName': email}, mode='PATCH')

    def search(self, key, value):
        users = self.get_users()
        found = {}
        for email, v in users.items():
            if isinstance(key, (list, tuple)):
                if v.get(key[0],{}).get(key[1],False) == value:
                    found[email] = v
            else:
                if v.get(key) == value:
                    found[email] = v
        return found

    def delete_user(self, email):
        url = f'https://graph.microsoft.com/v1.0/users/{email}'
        if response := self.transmit(url, mode='delete'):
            return response
        return False

    def proxy_lookup(self):
        if not self.proxy_aliases:
            users = self.get_users()
            for user in users:
                for e in users[user].get('proxyAddresses', []):
                    self.proxy_aliases[e.lower().replace('smtp:', '')] = user
        return self.proxy_aliases

    def disable_user(self, email):
        meta = {'accountEnabled':False}
        url = f'https://graph.microsoft.com/v1.0/users/{email}'
        return self.transmit(url, meta=meta, mode='PATCH')

    def enable_user(self, email):
        meta = {'accountEnabled':True}
        url = f'https://graph.microsoft.com/v1.0/users/{email}'
        return self.transmit(url, meta=meta, mode='PATCH')

    def update_user(self, email, meta):
        url = f'https://graph.microsoft.com/v1.0/users/{email}'
        return self.transmit(url, meta=meta, mode='PATCH')

    def update_manager(self, email, managerid):
        url = f'https://graph.microsoft.com/v1.0/users/{email}/manager/$ref'
        meta = {
            '@odata.id' : f'https://graph.microsoft.com/v1.0/users/{managerid}'
        }
        return self.transmit(url, meta=meta, mode='PUT')

    def create_user(self, email, displayname):
        empl_id = email.split('@')[0]
        url = f'https://graph.microsoft.com/v1.0/users'
        meta = {
            "accountEnabled": True,
            "displayName": displayname,
            "mailNickname": empl_id,
            "userPrincipalName": email,
            "passwordProfile" : {
                "forceChangePasswordNextSignIn": True,
                "password": f"NewUser@SDB{arrow.get().year}"
                }
            }
        return self.transmit(url, meta=meta, mode='POST')

    def email_aliases(self):
        result = []
        for i in self.get_users(refresh=False).values():
            proxy = [e.lower().replace('smtp:','') for e in i.get('proxyAddresses', [])]
            result.append([i['givenName'],i['surname'], i['userPrincipalName'],', '.join(proxy), ', '.join(i['otherMails'])])
        return result

    def dynamic_groups(self):
        x = self.groups()
        result = []
        for each in x['value']:
            # logger.info(each['displayName'])
            if each['membershipRule']:
                # logger.info(Fore.BLUE+each['displayName'])
                # logger.info(Style.BRIGHT + Fore.WHITE + each['membershipRule'])
                result.append(each)
        return result

    def groups(self):
        url = f'https://graph.microsoft.com/v1.0/groups'
        return self.transmit(url)

    def attributes(self, keyname):
        results = {}
        for email, meta in self.get_users(refresh=True).items():
            if 'extensionAttribute' in keyname:
                if x := meta['onPremisesExtensionAttributes'][keyname]:
                    results[email] = x
            elif keyname in meta:
                results[email] = meta[keyname]
        return results

    def transmit(self, url, meta=None, mode='get'):
        if meta:
            response = upool.request(mode, url, headers=self.header, body=jc(meta))
        else:
            response = upool.request(mode, url, headers=self.header)
        if response.status == 200:
            return dc(response.data.decode())
        if response.status == 204:
            return response
        elif response.status == 429:
            # logger.info('waiting')
            sleep(5)
            return self.transmit(url, mode)
        # logger.info(response.data)
        return response

if __name__ == '__main__':
    from pprint import pprint
    a = AzureSDB()
    plogger.info(a.proxy_lookup())
    # plogger.info(a.get_user('4f1e8874-23b5-41f0-86f0-9cf72fa3e2d1'))



    # logger.info(a.disable_user('jobn.hust@sdbmail.com'))
    #
    # # a.update_user('aed8eadb-727f-478a-adb7-3576b0adf6c0', {'onPremisesExtensionAttributes':{'extensionAttribute2':'124606'}})
    # x = a.get_users(refresh=True)
    # logger.info(x.get('jobn.hust@sdbmail.com'))