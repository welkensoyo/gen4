from gevent import sleep
from API.comms import upool, urlencode
import API.njson as json
import API.dbpg as db
import API.cache as cache
import arrow

import unittest

company_list = 181663,179638,173664,173663,173662,173909,173908,163041,163042,163040,149697,144351,144350,94291,94290,124606,124605,124604,124602,124601
client_id = '3hlQ2wLHFk6UjupMGIoE8S04NTg1Mzk3OTUxOTk4Mjk3NTc1'
client_secret = 'nvTf/fwa/B2RUIRCe4hUf5FjFs/DCHiI+MXvqhx//gVSEIU3+T6ceGbOll+B0jJLo5NsUUpm3Jhyls0c1IbXuA=='
company_id = '181663'

url = 'https://api.paylocity.com/api/v2/companies/181663/employees'
production = 'https://api.paylocity.com/IdentityServer/connect/token'
sandbox =  'https://apisandbox.paylocity.com/IdentityServer/connect/token'
secret = json.b64e(client_id+':'+client_secret)

# print(secret.decode())
# headers = {
#     'Content-Type': "application/x-www-form-urlencoded",
#     'Authorization': 'Basic '+secret.decode()
# }
# body = urlencode({'grant_type': 'client_credentials',
# 'scope': 'WebLinkAPI'})
# data = upool.request('POST', production, headers=headers, body=body)
# print(data.status)
# x = json.dc(data.data.decode())
# '{"access_token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlladVR2c0xhLXNjZV9ib3N0Ym9TWnp4TTdFOCIsImtpZCI6IlladVR2c0xhLXNjZV9ib3N0Ym9TWnp4TTdFOCJ9.eyJpc3MiOiJodHRwczovL2FwaS5wYXlsb2NpdHkuY29tL0lkZW50aXR5U2VydmVyIiwiYXVkIjoiaHR0cHM6Ly9hcGkucGF5bG9jaXR5LmNvbS9JZGVudGl0eVNlcnZlci9yZXNvdXJjZXMiLCJleHAiOjE2Nzg5OTk5ODIsIm5iZiI6MTY3ODk5NjM4MiwiY2xpZW50X2lkIjoiM2hsUTJ3TEhGazZVanVwTUdJb0U4UzA0TlRnMU16azNPVFV4T1RrNE1qazNOVGMxIiwiY2xpZW50X25hbWUiOiJTcGVjaWFsdHkgRGVudGFsIEJyYW5kcyAtIENTMTI0NjAxIFtCUEsgVGVjaF0iLCJzY29wZSI6IldlYkxpbmtBUEkifQ.gfYvf8duhZqHjfwAh9anCOsCFmWDIn-feRkTe4kqU5LQZ6xzOPmn6bN3lh2bKHhQ0MBbX9e15l35-0LWnYYPn7APajTM1gYZG-vOKWau3VK1X-20FPEFjXcouL5kiqpKP6a6fDr5lNjXI4weClY0laM7fy8clR24myD9AIrZPSSrGDNUgrlbZ-06_q340qA8kPFSCqcfrV4CLvIt9SExstzCXsf1gFtS1TANmQkvDTNnLNFC-CFNzrad3dulVLTvi4AxZ9mNKgTOurUuQaJkqmKtNOzt26wgSmsfSaP8D0J7sMxr53WCJrdb9w7QGHXyAFKXyWd9MP-2JYLkz6zp0Mjx55EsIHBT1uuU4VybDqcbuU5Le5HIFRRVP0C3HOliF4xf6As3smXLlqKd9dJGF2ItkbPTahvVmdghklggsA6wYsu8B_WqXKVDUMb5vyLWOUHvH4IQdXXITcr8qW3XlyiB52tRdaApfOhpd_8OXx7c4VSJp0JAr5nrAj5Yqb2O79vQNYikl8ksiijelO_BmgaTkQD3tDxgEETV7kqdah7R-1wEyIfa1ozeqL3m5xC-ogNVDOp26ZqsUjvD4j4r7yLhKRahusZ9dllvfU9vEDoUt-nup2pHEBNS9sJyAwohDT_Bfly72rP0U0QbuEPKEkT9PAhhN880QaanizQEPHg","expires_in":3600,"token_type":"Bearer"}'
# access_token = x['access_token']


# headers = {
#     'Content-Type': "application/json",
#     'Authorization': 'Bearer '+access_token
# }


compids = (181663,179638,173664,173663,173662,173909,173908,163041,163042,163040,149697,144351,144350,94291,94290,124606,124605,124604,124602,124601)
class ConnectionError(Exception):
    pass


class Paylocity:
    def __init__(self):
        self.access_token = ''
        self.header = { 'Content-Type': "application/json" }
        self.token = {}

    def get_token(self, new=False):
        if not new:
            self.token = db.fetchreturn("SELECT token FROM cache.access_tokens WHERE id = 'PAYLOCITY'")
            if self.token:
                self.header['Authorization'] = self.token['token_type'] + ' ' + self.token['access_token']
                return True
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': 'Basic ' + secret.decode()
        }
        response = upool.request('POST', production, headers=headers, body=urlencode({'grant_type': 'client_credentials', 'scope': 'WebLinkAPI'}))
        if response.status == 200:
            self.token = json.dc(response.data.decode())
            self.header['Authorization'] = self.token['token_type']+' '+self.token['access_token']
            token = json.jc(self.token)
            db.execute('''INSERT INTO cache.access_tokens VALUES ('PAYLOCITY', %s) ON CONFLICT (id) DO UPDATE SET token = %s ''', token, token )
            return True
        return False

    def employees(self, refresh=False):
        self.empls = []
        if not refresh:
            self.empls = cache.retrieve('PAYLOCITY')
        if not self.empls:
            from time import process_time as ti
            start=ti()
            self.empls = []
            for company in compids:
                es = self.employee_all(company)
                if es:
                    for e in es:
                        self.empls.append(self.employee(company, e['employeeId']))
            print(ti()-start)
        if self.empls:
            cache.sync('PAYLOCITY', self.empls)
        return self.empls

    def employee_all(self, companyid):
        url = f'https://api.paylocity.com/api/v2/companies/{companyid}/employees?pagesize=5000'
        return json.lc(self.transmit(url, mode='GET') or [])

    def employee(self, companyid, emplid):
        url = f'https://api.paylocity.com/api/v2/companies/{companyid}/employees/{emplid}'
        return json.dc(self.transmit(url, mode='GET') or {})

    def attriticion(self, shift=2):
        date = arrow.now().shift(days=-shift)
        return [employee['workAddress'].get('emailAddress','').lower() for employee in self.employees() if 'status' in employee and employee['status']['statusType'] == 'T' and arrow.get(employee['status']['effectiveDate']) >= date]

    def missing(self):
        return [employee for employee in self.employees() if 'status' in employee and not employee['workAddress'].get('emailAddress')]

    def delete_terminated_users(self, shift=2):
        from API.toolkits import azure
        a = azure.AzureSDB()
        users = {u['userPrincipalName'].lower() for u in a.get_users(refresh=True)}
        x = set(self.attriticion(shift=shift)).intersection(users)
        for user in x:
            print(user)
            a.delete_user(user)
            cache.removed_ad_user(user)
        cache.log('REMOVE USERS', f"{len(x)} Users Succesfully Deleted")
        return x

    def transmit(self, url, meta=None, mode='POST'):
        if not self.access_token:
            if not self.get_token():
                raise ConnectionError('Access Token Not Granted!')
        if meta:
            response = upool.request(mode, url, headers=self.header, fields=json.dc(meta))
        else:
            response = upool.request(mode, url, headers=self.header)
        if response.status == 200:
            return response.data.decode()
        if response.status == 401:
            # print('token expired')
            self.get_token(new=True)
            self.transmit(url, mode)
        elif response.status == 429:
            # print('waiting')
            sleep(5)
            self.transmit(url, mode)

    def az_pay(self):
        from API.toolkits import azure
        self.employees()
        a = azure.AzureSDB()
        az_users = a.get_users()
        missing = {}
        names = {}
        pay_missing = {}
        eids = set()
        duplicates = {}
        titles = set()
        for a in az_users:
            if a.get('givenName') == 'test':
                continue
            if a.get('givenName') and a.get('jobTitle') != 'Service Account':
                titles.add(a.get('jobTitle'))
                missing[(a.get('mail','') or '').strip().lower()] = a
                names[f"{a.get('givenName','').split(' ')[0].lower()}_{(a.get('surname','') or '').lower()}"] = a
        for e in self.empls:
            found = False
            if not e:
                continue
            if e.get('firstName','').lower() == 'test':
                continue
            eid = e.get('employeeId')
            work, home= e['workAddress'].get('emailAddress','').strip().lower(), e['homeAddress'].get('emailAddress','').strip().lower()
            name = f"{e.get('firstName','').lower()}_{e.get('lastName','').lower()}"
            if eid not in eids:
                eids.add(eid)
            else:
                duplicates['name'] = e
            # jobTitle = e.get('departmentPosition',{}).get('jobTitle')
            if not name:
                continue
            if missing.pop(work,False):
                found = True
            # if missing.pop(home,False):
            #     found = True
            # if names.pop(name,False):
            #     found = True
            if not found and e['status']['employeeStatus'] == 'A':
                pay_missing[name] = e
        # print(duplicates)
        cache.sync('az_missing', missing)
        cache.sync('az_names', names)
        cache.sync('paylocity_missing', pay_missing)
        m = []
        names = cache.retrieve('az_names')
        for k, e in cache.retrieve('paylocity_missing').items():
            we = ''
            pn = f"{e.get('preferredName', '').lower()}_{e.get('lastName', '').lower()}"
            gn = f"{e.get('firstName', '').lower()}_{e.get('lastName', '').lower()}"
            if pn in names:
                we = names[pn]['mail']
            elif gn in names:
                we = names[gn]['mail']
            m.append(['PAYLOCITY', e['workAddress'].get('emailAddress', '').strip(), we, e.get('preferredName'), e.get('firstName', ''), e.get('lastName', ''), e.get('employeeId'), e.get('departmentPosition', {}).get('jobTitle'),
                      e.get('coEmpCode', '').split('-')[0]])
        for k, e in cache.retrieve('az_missing').items():
            try:
                m.append(['AZURE', e['mail'].strip(), '', '', e.get('givenName', ''), e.get('surname', ''), '', e.get('jobTitle'), ''])
            except:
                continue
        cache.sync('aztopay', m)
        return



if __name__=="__main__":
    p = Paylocity()
    # p.employees(refresh=True)
    p.delete_terminated_users(shift=30)
    # print(cache.retrieve('aztopay'))


    # print(x)
    # x = p.attriticion(shift=30)
    # # x = p.missing()
    # for each in x:
    #     print(each)
