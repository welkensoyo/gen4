#!/usr/bin/env python3
import traceback

from API.config import curve as cv_config
from decimal import Decimal
from hashlib import md5
import API.dbpg as db
import API.njson as j
import API.cache as cache
import arrow
import requests
import csv
try:
    from gevent import sleep
except:
    from time import sleep


qry = {
    'update' : ''' INSERT INTO sdb.curve (name, meta) VALUES (%s, %s::jsonb)
                    ON CONFLICT (name) DO UPDATE SET meta = %s::jsonb RETURNING name ''',
    'insert': ''' INSERT INTO curve.{} (id, clinicid, meta, date) VALUES (%s, %s, %s::jsonb, %s)''',
    'get' : ''' SELECT meta FROM sdb.curve WHERE name = %s '''
}

class Configuration:
    def __init__(self):
        self.config = cv_config

        if "tenants" not in self.config:
            raise Exception("Config missing [tenants] section.")

        if "tenant_names" not in self.config["tenants"]:
            raise Exception("tenant_names missing from [tenants] section")


        # Parse the tenant names from the configuration file
        self.tenant_names = []
        tenant_names = self.config['tenants']['tenant_names']
        for tenant_name in tenant_names:
            self.tenant_names.append(tenant_name.strip())

        if "authentication" not in self.config:
            raise Exception("Config file missing [authentication] section.")

        authentication = self.config["authentication"]

        if "client_id" not in authentication:
            raise Exception("client_id missing from [authentication] section")
        if "client_secret" not in authentication:
            raise Exception("client_secret missing from [authentication] section")

        self.client_id = authentication["client_id"]
        self.client_secret = authentication["client_secret"]

        if "domain" in authentication:
            self.hero_domain = authentication['domain']
        else:
            self.hero_domain = 'curvehero.com'

    def get_tenant_names(self):
        return self.tenant_names

    def get_client_id(self):
        return self.client_id

    def get_client_secret(self):
        return self.client_secret

class HeroClient:
    def __init__(self, scopes):
        self.config = Configuration()
        self.tenant_names = self.config.get_tenant_names()
        self.access_tokens = {}
        self.client_id = self.config.get_client_id()
        self.client_secret = self.config.get_client_secret()
        self.scopes = scopes

    def get_base_url(self, tenant_name):
        return 'https://' + tenant_name + "." + self.config.hero_domain

    def clear_access_token(self, tenant_name):
        if tenant_name in self.access_tokens:
            del self.access_tokens[tenant_name]

    def check_access_token(self, tenant_name):
        if tenant_name in self.access_tokens:
            return

        scope = ' '.join(self.scopes)
        token_url = self.get_base_url(tenant_name) + '/cheetah/oauth2/token'
        token = requests.post(
            url= token_url,
            auth=(self.client_id, self.client_secret),
            data={
                'grant_type': "client_credentials",
                'scope': scope
            }
        )
        if token.status_code == 401:
            raise Exception("Invalid client_id and/or client_secret for " + token_url + "(" + token.text + ")")
        elif token.status_code == 404:
            raise Exception("Invalid tenant name.  Check the tenant name is correct. " + token_url + " not found (404).")
        elif token.status_code == 200:
            self.access_tokens[tenant_name] = token.json()["access_token"]
        elif token.status_code == 400:
            error = token.json()
            if error['error'] == 'unauthorized_client':
                raise Exception('The client_id ' + self.client_id + ' is not permitted to access tenant ' + tenant_name + '.  Contact customer support.')
            else:
                raise Exception('Unexpected response code ' + str(token.status_code))
        else:
            print('Unexpected response code ' + str(token.status_code) + ' from ' + token_url)
            raise Exception('Unexpected response code ' + str(token.status_code))

    def download(self, tenant_name, url, errors=0):
        self.check_access_token(tenant_name)
        full_url = self.get_base_url(tenant_name) + url
        response = requests.get(
            url= full_url,
            headers={
                "Authorization": "Bearer " + self.access_tokens[tenant_name]
            }
        )
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            self.clear_access_token(tenant_name)
            if errors == 2:
                return {'objects':[], 'meta':[]}
            return self.download(tenant_name, url, errors + 1)
        elif response.status_code == 429:
            # Implement primitive rate limiting by sleeping.
            duration = (errors ** 2) + 1
            print('Rate limited (429).  Sleeping for ' + str(duration) + 's.')
            sleep(duration)
            return self.download(tenant_name, url, errors + 1)
        else:
            print('Unexpected response code ' + str(response.status_code) + ' from ' + full_url)

            raise Exception('Unexpected status code ' + str(response.status_code) + ' from request ' + full_url + ':\n' + response.text)

    def download_collection(self, path):
        for tenant_name in self.tenant_names:
            current_path = path
            while True:
                result = self.download(tenant_name, current_path)
                yield (tenant_name, result['objects'])

                if 'next' in result['meta']:
                    current_path = result['meta']['next']

                    if current_path.startswith("cheetah/"):
                        current_path = "/" + current_path
                else:
                    break


class API:
    def __init__(self):
        self.start_date_str = arrow.now().floor('month').format('YYYY-MM-DD')
        self.save_folder = 'C:\Production\Data_API\data\\'
        self.monthly_account_journal_total = {}
        self.account_receivables_summaries = {}
        self.today = arrow.now().format('YYYY-MM-DD')
        self.hc = HeroClient(scopes=['ledger_entry:read', 'accounts_receivable.all:read','clinic:read','patient:read', 'schedule:read', 'appointment_summary:read', 'appointment.items:read'])

    def set_start_month(self, months):
        self.start_date_str = arrow.now().shift(months=months).ceil('month').format('YYYY-MM-DD')

    def set_start_day(self, date):
        self.start_date_str = arrow.get(date).format('YYYY-MM-DD')

    def hash(self, text):
        return md5(text.encode()).hexdigest()

    def accumulate_monthly_ledger_totals(self, tenant_name, clinic_id, posted_on_str, calculations):
        posted_on = arrow.get(posted_on_str)
        k = f'{tenant_name} {clinic_id} {str(posted_on.year).zfill(4) + "-" + str(posted_on.month).zfill(2)}'
        if k not in self.monthly_account_journal_total:
            values = {}
            for value_key in calculations.keys():
                values[value_key] = f'{float(calculations[value_key]):.2f}'

            self.monthly_account_journal_total[k] = values
        else:
            values = self.monthly_account_journal_total[k]
            for value_key in calculations.keys():
                values[value_key] = f'{Decimal(values[value_key]) + Decimal(calculations[value_key]):.2f}'
            self.monthly_account_journal_total[k] = values

    def patient(self):
        route = f'patient'
        filename = 'patient'
        return self.get(filename, route)

    def clinic(self, refresh=False):
        route = 'clinic'
        filename = 'clinic'
        return self.get(filename, route, refresh=refresh)

    def appointments(self, refresh=True):
        route=f'appointment_items?startDate={self.start_date_str}&endDate={self.today}'
        filename = 'appointments'
        return self.get(filename, route, refresh=refresh)

    def ledger(self, refresh=False):
        route = f'ledger_entry?startDate={self.start_date_str}&endDate={self.today}'
        table = 'ledger'
        for (tenant_name, result) in self.hc.download_collection(f"/cheetah/{route}"):
            for each in result:
                if not each:
                    continue
                m = j.jc(each)
                text = self.hash(m)
                db.fetchreturn(qry['insert'].format(table), text, tenant_name, m, each.get('postedOn'))
        cache.log('CURVE LEDGER',f"COMPLETED {route}")
        return self

    def ledger_summary(self):
        for k, obj in db.fetchall('SELECT clinicid, meta FROM curve.ledger'):
            self.accumulate_monthly_ledger_totals(k, obj['clinic']['id'], obj['postedOn'], obj['calculations'])
        return self

    def accounts_receivable(self, refresh=False):
        route = 'accounts_receivable'
        filename = 'accounts_receivable'
        return self.get(filename, route, refresh=refresh)

    def accounts_receivable_summary(self):
        for k, v in self.accounts_receivable().items():
            for obj in v:
                posted_on = arrow.get(self.today)
                key = f"{k} {obj['clinic']['id']} {str(posted_on.year).zfill(4) + '-' + str(posted_on.month).zfill(2)}"
                values = {
                    'responsibleParty0to30d': Decimal(obj['responsiblePartyAmount']['day0To30']),
                    'responsiblePartyday31To60': Decimal(obj['responsiblePartyAmount']['day31To60']),
                    'responsiblePartyday61To90': Decimal(obj['responsiblePartyAmount']['day61To90']),
                    'responsiblePartydayOver90': Decimal(obj['responsiblePartyAmount']['dayOver90']),
                    'insuranceEstimate0to30d': Decimal(obj['insuranceEstimate']['day0To30']),
                    'insuranceEstimate31To60': Decimal(obj['insuranceEstimate']['day31To60']),
                    'insuranceEstimate61To90': Decimal(obj['insuranceEstimate']['day61To90']),
                    'insuranceEstimateOver90d': Decimal(obj['insuranceEstimate']['dayOver90']),
                    'patientEstimate0to30d': Decimal(obj['patientEstimate']['day0To30']),
                    'patientEstimate31To60': Decimal(obj['patientEstimate']['day31To60']),
                    'patientEstimate61To90': Decimal(obj['patientEstimate']['day61To90']),
                    'patientEstimateOver90d': Decimal(obj['patientEstimate']['dayOver90'])
                }

                if key in self.account_receivables_summaries:
                    current_summary = self.account_receivables_summaries[key]
                    for value_key in values.keys():
                        current_summary[value_key] = str(Decimal(current_summary[value_key]) + values[value_key])
                else:
                    self.account_receivables_summaries[key] = values
        return self.account_receivables_summaries

    def get(self, tablename, route, refresh=False):
        if not refresh:
            x = db.fetchreturn(qry['get'], tablename)
            if x:
                return x
        meta = {}
        for (tenant_name, result) in self.hc.download_collection(f"/cheetah/{route}"):
            if tenant_name not in meta:
                meta[tenant_name] = []
            meta[tenant_name].extend(result)
        m = j.jc(meta)
        db.fetchreturn(qry['update'], tablename, m, m)
        return meta

    def get_from_file(self, filename, route, refresh=False):
        if not refresh:
            try:
                with open(self.save_folder+filename+'.json', 'r') as f:
                    x = f.read().rstrip()
                    return j.loads(x)
            except:
                traceback.print_exc()
        meta = {}
        for (tenant_name, result) in self.hc.download_collection(f"/cheetah/{route}"):
            if tenant_name not in meta:
                meta[tenant_name] = []
            meta[tenant_name].extend(result)
        with open(self.save_folder+filename+'.json', 'w') as f:
            f.write(j.jc(meta))
        return meta

    def build_csv(self):
        with open(self.save_folder+'ledger_entries.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                [
                    'tenantName',
                    'account',
                    'amount',
                    'patientId',
                    'providerId',
                    'clinicId',
                    'postedOn',
                    'insuranceCode',
                    'description',
                    'transactionId',
                    'transactionType',
                    'adjustmentType',
                    'adjustmentCategory',
                    'insurancePayer',
                    'customPayer',
                    'calcProduction',
                    'calcAdjustments',
                    'calcAdjustedProduction',
                    'calcPayments',
                    'calcPaymentsFromAccountCredit',
                    'calcTotalPayments',
                    'calcPaymentRefunds',
                    'calcOverPayments',
                    'calcDeposits',
                    'calcCreditRefunds'
                ]
            )

            total_ledger_entries = 0
            for (tenant_name, result) in self.hc.download_collection("/cheetah/ledger_entry?startDate=" + self.start_date_str + "&endDate=" + self.today):
                total_ledger_entries = total_ledger_entries + len(result)
                # print(f'ledger_entries.csv ({tenant_name}) {total_ledger_entries}')
                for obj in result:
                    self.accumulate_monthly_ledger_totals(tenant_name, obj['clinic']['id'], obj['postedOn'], obj['calculations'])
                    csv_writer.writerow(
                        [
                            tenant_name,
                            obj['account'],
                            obj['amount'],
                            obj['patientId'],
                            obj.get('provider',''),
                            obj['clinic']['id'],
                            obj['postedOn'],
                            obj['insuranceCode'],
                            obj['description'],
                            obj['transactionId'],
                            obj['transactionType'],
                            obj['adjustmentType'],
                            obj['adjustmentCategory'],
                            obj['insurancePayer'],
                            obj['customPayer'],
                            obj['calculations']['production'],
                            obj['calculations']['adjustments'],
                            obj['calculations']['adjustedProduction'],
                            obj['calculations']['payments'],
                            obj['calculations']['paymentsFromAccountCredit'],
                            obj['calculations']['totalPayments'],
                            obj['calculations']['paymentRefunds'],
                            obj['calculations']['overpayments'],
                            obj['calculations']['deposits'],
                            obj['calculations']['creditRefunds']
                        ]
                    )

        with open(self.save_folder+'ledger_entry_summaries.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                [
                    'tenantName',
                    'clinicId',
                    'yearMonth',
                    'production',
                    'adjustments',
                    'adjustedProduction',
                    'payments',
                    'paymentsFromAccountCredit',
                    'totalPayments',
                    'paymentRefunds',
                    'overPayments',
                    'deposits',
                    'creditRefunds'
                ]
            )

            total_ledger_entry_summaries = 0

            for key, value in self.monthly_account_journal_total.items():
                csv_writer.writerow(
                    [
                        key.tenant_name,
                        key.clinic_id,
                        key.year_month,
                        value['production'],
                        value['adjustments'],
                        value['adjustedProduction'],
                        value['payments'],
                        value['paymentsFromAccountCredit'],
                        value['totalPayments'],
                        value['paymentRefunds'],
                        value['overpayments'],
                        value['deposits'],
                        value['creditRefunds']
                    ]
                )

                total_ledger_entry_summaries = total_ledger_entry_summaries + 1

        print(f'ledger_entry_summaries.csv {total_ledger_entry_summaries}')

        account_receivables_summaries = {}

        with open(self.save_folder+'accounts_receivable.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                [
                    'tenantName',
                    'date',
                    'responsiblePartyId',
                    'providerId',
                    'clinicId',
                    'responsibleParty0to30d',
                    'responsibleParty31To60',
                    'responsibleParty61To90',
                    'responsiblePartyOver90',
                    'insuranceEstimate0to30d',
                    'insuranceEstimate31To60',
                    'insuranceEstimate61To90',
                    'insuranceEstimateOver90d',
                    'patientEstimate0to30d',
                    'patientEstimate31To60',
                    'patientEstimate61To90',
                    'patientEstimateOver90d'
                ]
            )

            total_account_receivables = 0
            for (tenant_name, result) in self.hc.download_collection("/cheetah/accounts_receivable"):
                total_account_receivables = total_account_receivables + len(result)

                print(f"accounts_receivables.csv ({tenant_name}) {total_account_receivables}")

                for obj in result:
                    csv_writer.writerow(
                        [
                            tenant_name,
                            self.today,
                            obj['responsibleParty']['id'],
                            obj['provider']['id'],
                            obj['clinic']['id'],
                            obj['responsiblePartyAmount']['day0To30'],
                            obj['responsiblePartyAmount']['day31To60'],
                            obj['responsiblePartyAmount']['day61To90'],
                            obj['responsiblePartyAmount']['dayOver90'],
                            obj['insuranceEstimate']['day0To30'],
                            obj['insuranceEstimate']['day31To60'],
                            obj['insuranceEstimate']['day61To90'],
                            obj['insuranceEstimate']['dayOver90'],
                            obj['patientEstimate']['day0To30'],
                            obj['patientEstimate']['day31To60'],
                            obj['patientEstimate']['day61To90'],
                            obj['patientEstimate']['dayOver90']
                        ]
                    )

                    posted_on = arrow.get(obj['postedOn'])
                    k = f"{tenant_name} {obj['clinic']['id']} {str(posted_on.year).zfill(4) + '-' + str(posted_on.month).zfill(2)}"
                    values = {
                        'responsibleParty0to30d': Decimal(obj['responsiblePartyAmount']['day0To30']),
                        'responsiblePartyday31To60': Decimal(obj['responsiblePartyAmount']['day31To60']),
                        'responsiblePartyday61To90': Decimal(obj['responsiblePartyAmount']['day61To90']),
                        'responsiblePartydayOver90': Decimal(obj['responsiblePartyAmount']['dayOver90']),
                        'insuranceEstimate0to30d': Decimal(obj['insuranceEstimate']['day0To30']),
                        'insuranceEstimate31To60': Decimal(obj['insuranceEstimate']['day31To60']),
                        'insuranceEstimate61To90': Decimal(obj['insuranceEstimate']['day61To90']),
                        'insuranceEstimateOver90d': Decimal(obj['insuranceEstimate']['dayOver90']),
                        'patientEstimate0to30d': Decimal(obj['patientEstimate']['day0To30']),
                        'patientEstimate31To60': Decimal(obj['patientEstimate']['day31To60']),
                        'patientEstimate61To90': Decimal(obj['patientEstimate']['day61To90']),
                        'patientEstimateOver90d': Decimal(obj['patientEstimate']['dayOver90'])
                    }

                    if key in account_receivables_summaries:
                        current_summary = account_receivables_summaries[key]
                        for value_key in values.keys():
                            current_summary[value_key] = current_summary[value_key] + \
                                values[value_key]
                    else:
                        account_receivables_summaries[key] = values

        with open(self.save_folder+'accounts_receivable_summaries.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                [
                    'tenantName',
                    'date',
                    'clinicId',
                    'responsibleParty0to30d',
                    'responsiblePartyday31To60',
                    'responsiblePartyday61To90',
                    'responsiblePartydayOver90',
                    'insuranceEstimate0to30d',
                    'insuranceEstimate31To60',
                    'insuranceEstimate61To90',
                    'insuranceEstimateOver90d',
                    'patientEstimate0to30d',
                    'patientEstimate31To60',
                    'patientEstimate61To90',
                    'patientEstimateOver90d'
                ]
            )

            for key, value in account_receivables_summaries.items():
                csv_writer.writerow(
                    [
                        key.tenant_name,
                        key.year_month,
                        key.clinic_id,
                        value['responsibleParty0to30d'],
                        value['responsiblePartyday31To60'],
                        value['responsiblePartyday61To90'],
                        value['responsiblePartydayOver90'],
                        value['insuranceEstimate0to30d'],
                        value['insuranceEstimate31To60'],
                        value['insuranceEstimate61To90'],
                        value['insuranceEstimateOver90d'],
                        value['patientEstimate0to30d'],
                        value['patientEstimate31To60'],
                        value['patientEstimate61To90'],
                        value['patientEstimateOver90d']
                    ]
                )

    def appointments_csv(self):
        import datetime
        save_folder = 'data\\'
        today = datetime.datetime.today().date()
        thirtyone_days_ago = today - datetime.timedelta(days=31)

        two_years = datetime.timedelta(days=365 * 2)

        today_str = today.isoformat()
        start_date_str = (today - two_years).isoformat()
        end_date_str = (today + two_years).isoformat()

        # Build Curve Hero Client
        hc = HeroClient(
            scopes=['clinic:read',
                    'schedule:read',
                    'appointment_summary:read',
                    'appointment.items:read'])


        # Gather metadata (clinics)
        tenant_clinics = {}

        print("Gathering metadata ... ", end="")
        for (tenant_name, result) in hc.download_collection('/cheetah/clinic'):
            print(tenant_name + " ", end="", flush=True)
            tenant_clinics[tenant_name] = []
            for clinic in result:
                if clinic['isActive']:
                    tenant_clinics[tenant_name].append(clinic)

        total_daily_summaries = 0
        with open(save_folder+'daily_appointment_summary.csv', 'w') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                [
                    'tenantName',
                    "date",
                    'clinicId',
                    'clinicName',
                    'appointmentsCompleted',
                    'appointmentsCancelled',
                    'appointmentsMissed',
                    'appointmentsDeleted',
                    'appointmentsQueued',
                    'appointmentsIncomplete',
                    'appointmentsRescheduled',
                    'appointmentsRelocated'
                ]
            )

            for tenant_name, clinics in tenant_clinics.items():
                for clinic in clinics:

                    clinic_id = str(clinic["id"])
                    clinic_name = clinic["name"]

                    current = thirtyone_days_ago

                    while current < today:
                        current_date_str = current.isoformat()

                        result = hc.download(tenant_name, "/cheetah/clinic/" + clinic_id + "/daily_appointment_summary?date=" + current_date_str)
                        csv_writer.writerow(
                            [
                                tenant_name,
                                current_date_str,
                                clinic_id,
                                clinic_name,
                                result['appointmentsCompleted'],
                                result['appointmentsCancelled'],
                                result['appointmentsMissed'],
                                result['appointmentsDeleted'],
                                result['appointmentsQueued'],
                                result['appointmentsIncomplete'],
                                result['appointmentsRescheduled'],
                                result['appointmentsRelocated']
                            ]
                        )

                        current = current + datetime.timedelta(days=1)
                        total_daily_summaries = total_daily_summaries + 1

                print("daily_appointment_summary.csv (" + tenant_name + ") " + str(total_daily_summaries))


        with open(save_folder+'appointments.csv', 'w') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                [
                    'tenantName',
                    'appointmentId',
                    'clinicId',
                    'patientId',
                    'providerId',
                    'appointmentStartTime',
                    'status'
                ]
            )

            total_calendar_events = 0
            for (tenant_name, objects) in hc.download_collection("/cheetah/calendar_event/schedule?startDate=" + start_date_str + "&endDate=" + end_date_str + "&paginated=true&pageSize=2000"):
                for object in objects:
                    if object["kind"] == "Appointment":
                        csv_writer.writerow(
                                [
                                    tenant_name,
                                    object["id"],
                                    object["clinic_id"],
                                    object["patient_id"],
                                    object["provider_id"],
                                    object["startTime"],
                                    object['status']
                                ]
                            )

                        total_calendar_events = total_calendar_events + 1



                print("appointments.csv (" + tenant_name + ") " + str(total_calendar_events))

        with open(save_folder+'appointment_items.csv', 'w') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                [
                    'tenantName',
                    "appointmentItemId",
                    "kind",
                    "status",
                    "procedureCode",
                    "units",
                    'appointmentId'
                ]
            )

            total_appointment_items = 0
            for (tenant_name, objects) in hc.download_collection("/cheetah/appointment_items?startDate=" + start_date_str + "&endDate=" + end_date_str):
                total_appointment_items = total_appointment_items + len(objects)

                for object in objects:
                    csv_writer.writerow(
                            [
                                tenant_name,
                                object["id"],
                                object["kind"],
                                object["status"],
                                object["procedureCode"],
                                object["units"],
                                object["appointment"]["id"]
                            ]
                        )

                print("appointment_items.csv (" + tenant_name + ") " + str(total_appointment_items))

if __name__=='__main__':
    h = API()
    print(len(h.ledger(refresh=True)))
    print(len(h.accounts_receivable(refresh=True)))
    print(len(h.clinic(refresh=True)))
    print(len(h.appointments(refresh=True)))

    # h.accounts_receivable()
    # for k,v in h.ledger(refresh=True).items():
    #     print(k)
    #     for each in v:
    #         print(each['amount'])
    # print(h.monthly_account_journal_total)
    # x = h.ledger()
    # for k,v in x.items():
    #     print(k)
    #     for each in v:
    #         print(each)

