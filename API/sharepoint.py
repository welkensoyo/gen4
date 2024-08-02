import traceback
from API.config import sharepoint_pages, sharepoint as cert_credentials
from office365.runtime.auth.authentication_context import AuthenticationContext
# from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
# from office365.sharepoint.files.file import File
from io import BytesIO
from API.files import Excel, get_filename
import API.dbms as db
import csv, arrow
from API.cache import sync, retrieve


# url_shrpt = 'https://sdbrands.sharepoint.com'
# auth_url = 'https://login.microsoftonline.com/sdbrands'
# folder_url_shrpt = '/personal/derek_bartron_sdbmail_com/'


class API:
    def __init__(self, url):
        aauth = AuthenticationContext(url=url)
        aauth.acquire_token_for_app(**cert_credentials)
        self.ctx = ClientContext(url, aauth)
        # cauth = ClientCredential(APPID, SECRET)
        # self.ctx = ClientContext(url_shrpt).with_credentials(ClientCredential(**cert_credentials))
        self.web = self.ctx.web
        self.ctx.load(self.web)
        self.ctx.execute_query()
        self.temproot = '\\home\\dataload\\'
        self.table = 'cached.rcm_weekly'
        self.current_year = arrow.get().format('YYYY')
        self.filepath = None
        self.cwalk = {}
        self.cols = {}
        self.filename = ''
        self.que = []
        self.list_name = ''
        # print(self.web.properties)
        # pprint(self.web.properties)
        # print('Authenticated into sharepoint as: ', self.web.properties['Title'])

    def overwrite_list(self, name, new_items):
        list_object = self.ctx.web.lists.get_by_title(name)
        self.cols = {field.properties['InternalName']: field.properties['Title'] for field in list_object.fields.get().execute_query() if
                     (not field.hidden and not field.read_only_field and field.can_be_deleted) or field.properties['InternalName'] == 'Title'}
        # Delete existing items in list
        items = list_object.get_items()
        self.ctx.load(items)
        self.ctx.execute_query()
        for item in items:
            item.delete_object()
        self.ctx.execute_query()

        # Add new items to the list
        for new_item in new_items:
            item_properties = {'Title': new_item}
            list_object.add_item(item_properties).execute_query()

    def delete_list(self, name):
        list_object = self.ctx.web.lists.get_by_title(name)
        self.cols = {field.properties['InternalName']: field.properties['Title'] for field in
                     list_object.fields.get().execute_query() if
                     (not field.hidden and not field.read_only_field and field.can_be_deleted) or field.properties[
                         'InternalName'] == 'Title'}
        # Delete existing items in list
        items = list_object.get_items()
        self.ctx.load(items)
        self.ctx.execute_query()
        for item in items:
            item.delete_object()
        self.ctx.execute_query()
        return self

    def add_list(self, list_title, new_item):
        list_object = self.ctx.web.lists.get_by_title(list_title)
        self.cols = {field.properties['InternalName']: field.properties['Title'] for field in list_object.fields.get().execute_query() if
                     (not field.hidden and not field.read_only_field and field.can_be_deleted) or field.properties['InternalName'] == 'Title'}
        if isinstance(new_item, list):
            for new in new_item:
                list_object.add_item(new)
            self.ctx.execute_query()
        else:
            list_object.add_item(new_item).execute_query()
        return self

    def get_list(self, name, internal_name=False):
        items = self.list_by_title(name)
        x = []
        if internal_name:
            for i in items:
                x.append({cn: i.properties.get(cn) for cn in self.cols.keys()})
        else:
            for i in items:
                x.append({self.cols[cn]: i.properties.get(cn) for cn in self.cols.keys()})
        return x

    def list_by_title(self, name):
        list_object = self.web.lists.get_by_title(name)
        self.cols = {field.properties['InternalName']: field.properties['Title'] for field in list_object.fields.get().execute_query() if
                     (not field.hidden and not field.read_only_field and field.can_be_deleted) or field.properties['InternalName'] == 'Title'}
        return list_object.get_items().execute_query()

    def root_folders(self, recursive=False):
        return self.web.default_document_library().root_folder.get_folders(recursive).execute_query().to_json()

    def folder_contents(self, folder_url, recursive=False):
        return self.web.get_folder_by_server_relative_url(folder_url).execute_query().get_files(recursive).execute_query().to_json()

    def file_ids(self, folder_url):
        return {x['Name']: x['UniqueId'] for x in self.folder_contents(folder_url)}

    def folder_path(self, folder_url, recursive=False):
        return self.web.get_folder_by_server_relative_path(folder_url).execute_query().get_files(recursive).execute_query().to_json()

    def folders(self, folder_url, recursive=False):
        return self.web.get_folder_by_server_relative_url(folder_url).get_folders(recursive).execute_query().to_json()

    def download(self, id):
        f = BytesIO()
        self.ctx.web.get_file_by_id(id).download(f).execute_query()
        f.seek(0)
        return f.read()

    def import_excel(self, id, check_field=None, data_only=True, header=True, sheetname=None):
        return Excel().open(self.download(id), data_only=data_only, filename=self.filename).ws(header=header, check_field=check_field, sheetname=sheetname)

    def check_exists(self):
        return

    def truncate_table(self):
        SQL = 'TRUNCATE TABLE rcm_weekly'
        db.bpkexecute(SQL)
        return self

    def rcm(self, id, filename):
        self.filename = filename
        self.filepath = self.temproot + id + '.csv'

        def _strip_lower(v):
            try:
                return v.strip().lower()
            except:
                return None
        def checkstrip(v):
            try:
                v = v.strip()
                if v:
                    if v != '-':
                        return v
                return ''
            except:
                return v

        def checkfloat(v):
            try:
                return float(v)
            except:
                return 0

        result = []
        headers = ['deposit date', 'bank total deposit', 'bank reference', 'bank name', 'bank account number', 'tin info', 'payment type', 'payer', 'bank transaction detail', 'post date', 'practice', 'amount posted']
        count = 0
        hi = {} # header index
        with open(self.filepath, 'w', newline='') as f:
            cw = csv.writer(f, delimiter='|')
            xlast = []
            for row in self.import_excel(id, check_field=11, data_only=True, sheetname='ReconSheet'):
                if count == 0:
                    hi = {_strip_lower(x):i for i, x in enumerate(row) if _strip_lower(x) in headers}
                    count +=1
                    continue
                x = ['',]
                x.extend([row[hi.get(_, 0)] for _ in headers])
                x[hi['bank total deposit']] = x[hi['bank total deposit']] if checkfloat(x[hi['bank total deposit']]) else None
                for i, v in enumerate(x):
                    x[i] = checkstrip(v)
                x[0] = count
                try:
                    abbrv = x[-2][0:4]
                except:
                    continue
                clinic = self.cwalk.get(abbrv, '')
                x.extend([abbrv, clinic, f'{filename}', id])
                count += 1
                if x[2]:
                    xlast = x[0:10]
                elif xlast:
                    for i in (1,3,4,5,6,7,8,9):
                        if not x[i]:
                            x[i] = xlast[i]
                # db.bpkexecute('INSERT INTO rcm_weekly VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', *x[0:12])
                cw.writerow(x[0:17])
        db.load_bcp_db(self.table, self.filepath, _async=True)
        return result

    def parse_reconciliation(self, folder, refresh=False):
        self.cwalk = crosswalk()
        import os
        errors = []
        if not refresh:
            contents = retrieve('RCON')
        else:
            print('Pulling Documents from Sharepoint Location...')
            contents = self.folder_contents(folder, recursive=True)
            sync('RCON', contents)
        count = 0
        self.truncate_table()
        for doc in contents:
            if doc['ServerRelativeUrl'].endswith('.xlsx') and self.current_year in doc['ServerRelativeUrl'] and doc['Exists'] and 'Zzz' not in doc['ServerRelativeUrl']:
                try:
                    print('Recon processing: ', doc['UniqueId'])
                    self.rcm(doc['UniqueId'], get_filename(doc['ServerRelativeUrl']))
                    count+=1
                except Exception as e:
                    traceback.print_exc()
                    x = traceback.format_exc()
                    errors.append([doc['UniqueId'], doc['ServerRelativeUrl'], x])
                    os.remove(self.filepath)
        sync('RCON_errors',errors)
        print(count)
        return self

    def errors(self):
        return retrieve('RCON_errors') or [['','','']]

    def error_report(self):
        print(self.errors())
        return [[x[0], get_filename(x[1]), x[2]] for x in self.errors()]

    def bulk_import(self):
        if self.list_name and self.que:
            return self.add_list(self.list_name, self.que)
        return False

    def terminated_user(self, name, email, office, terminated_date, que=False):
        self.list_name = 'Terminated Users'
        meta = {
            'Title': str(name),
            'TerminationDate': arrow.get(terminated_date).format('YYYY-MM-DD'),
            'Office': str(office),
            'EmailAddress': str(email),
            'Terminationemailsent': str(True),
            'UserterminatedinAD': str(False),
            'PMSAccessrevoked': str(False),
            'Equipmentreturnsent': '',
            'Equipmentreturned': str(False),
        }
        if not que:
            try:
                self.add_list('Terminated Users', meta)
            except:
                traceback.print_exc()
                return False
        self.que.append(meta)
        return self.que or meta

def crosswalk():
    try:
        data = Excel().open('C:\\Users\\DerekBartron\\OneDrive - Specialty Dental Brands\\Data Team\\2023\\DB Copy 2024 FP&A Location Mapping.xlsx').ws()
        header = data[0]
        ab = header.index('Abbreviation')
        primary = header.index('SDB Primary Reporting Name')
        secondary = header.index('SDB Secondary Reporting Name')
        result = {}
        for each in data[1:]:
            if each[ab]:
                result[each[ab]] = each[primary]
        return result
    except:
        return {}



if __name__ == '__main__':
    from time import time, perf_counter
    from pprint import pprint
    x = API(sharepoint_pages.rcm).folders('Shared Documents', recursive=True)
    pprint(x)
    # a = API(sharepoint_pages.terminated)
    # print(a.get_list('Terminated Users'))

    # print(crosswalk())
    # s = API(data_team_url)
    # x = s.get_list('SDB Corporate Directory')
    # print(s.cols)
    # pprint(x)
    # for each in x:
    #     print(each.properties['Title'])

    # x = s.error_report()
    # pprint(x)
    # print(len(x))
    # start = perf_counter()
    # x.parse_reconciliation('Shared Documents', refresh=False)
    # end = perf_counter()
    # print(end - start)
    # x.import_excel('3a3215af-21df-437f-90aa-4ede1a50c43b')
    # x = API(data_team_url)
    # print(x.download('50BF3F24-583A-4DC7-97F5-55F096E3E9D2'))
    # f = x.folder_contents('Shared Documents', recursive=True)
    # for d in f:
    #     print(d['Name'])
    # pprint(f)
