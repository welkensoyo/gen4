from gevent import monkey
monkey.patch_all()
from API.cache import sync, retrieve
from pprint import pprint
def cloud9():
    from gevent import monkey
    monkey.patch_all()
    import API.PMS.cloud9 as cloud9
    from pprint import pprint

    monkey.patch_all()
    c = cloud9.API('f1943667-c6a2-45c8-9cce-842c4187966e')
    x = c.patientlist().aging().result
    # x = c.patientlist('03f9863f-6695-4d68-9a16-a3cf1ae712e4')
    # pprint(x)
    # x = c.patientlist('19a7436b-f760-4814-8f32-6e42fb28556e')
    # pprint(x)
#
# cloud9()

def curve():
    import API.PMS.curve as cv
    hero = cv.API()
    return hero.clinic()


def az_pay():
    from API import paylocity
    from API.toolkits import azure

    p = paylocity.Paylocity()
    p.employees()
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
    for e in p.empls:
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
    print(duplicates)
    sync('az_missing', missing)
    sync('az_names', names)
    sync('paylocity_missing', pay_missing)
    return missing, names, pay_missing

# az_pay()
m = []
names = retrieve('az_names')
for k,e in retrieve('paylocity_missing').items():
    we = ''
    pn = f"{e.get('preferredName','').lower()}_{e.get('lastName','').lower()}"
    gn = f"{e.get('firstName','').lower()}_{e.get('lastName','').lower()}"
    if pn in names:
        we = names[pn]['mail']
    elif gn in names:
        we = names[gn]['mail']
    m.append(['PAYLOCITY', e['workAddress'].get('emailAddress','').strip(), we, e.get('preferredName'), e.get('firstName',''),e.get('lastName',''),e.get('employeeId'), e.get('departmentPosition',{}).get('jobTitle'), e.get('coEmpCode','').split('-')[0] ])

for k,e in retrieve('az_missing').items():
    try:
        m.append(['AZURE', e['mail'].strip(),'','', e.get('givenName',''), e.get('surname',''), '', e.get('jobTitle'), ''])
    except:
        continue
import csv
with open('missing.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['source','email', 'corrected','preferred', 'first', 'last', 'id', 'title', 'companyid'])
    for line in m:
        try:
            writer.writerow(line)
        except:
            pass





# missing, names, pay_missing = az_pay()
# # pprint(missing)
# # pprint(names)
# # for each, v in names.items():
# #     print(each, v)
# print(len(names))
# print(len(missing))
# print(len(pay_missing))
# # print(pay_missing)
# for each,v in pay_missing.items():
#     print(each, v)

# az = {'mail': 'Zulema.Garcia@reddirtorthomail.com', 'jobTitle': 'Treatment Coordinator', 'userType': 'Member', 'givenName': 'Zulema', 'department': None, 'displayName': 'Zulema Garcia', 'mobilePhone': None, 'officeLocation': 'Red Dirt Orthodontics', 'userPrincipalName': 'Zulema.Garcia@reddirtorthomail.com'}

# x = {'birthDate': '1963-03-20T00:00:00',
#  'coEmpCode': '124601-890',
#  'companyFEIN': '84-1764495',
#  'companyName': 'GA Specialty Dental Services, PC',
#  'currency': 'USD',
#  'departmentPosition': {'costCenter1': 'GOMS',
#                         'costCenter2': '1',
#                         'costCenter3': 'ORAL',
#                         'effectiveDate': '2023-01-19T00:00:00',
#                         'employeeType': 'RFT',
#                         'equalEmploymentOpportunityClass': '5',
#                         'isMinimumWageExempt': False,
#                         'isOvertimeExempt': False,
#                         'isSupervisorReviewer': True,
#                         'isUnionDuesCollected': False,
#                         'isUnionInitiationCollected': False,
#                         'jobTitle': 'Community Marketing Specialist',
#                         'positionCode': '324',
#                         'reviewerCompanyNumber': '124606',
#                         'reviewerEmployeeId': '10010',
#                         'supervisorCompanyNumber': '124606',
#                         'supervisorEmployeeId': '10010'},
#  'emergencyContacts': [{'firstName': 'Jaime',
#                         'lastName': 'Johnson',
#                         'mobilePhone': '(678) 595-2003',
#                         'primaryPhone': 'M',
#                         'priority': 'P',
#                         'relationship': 'Daughter'},
#                        {'firstName': 'Mark',
#                         'lastName': 'Willett',
#                         'mobilePhone': '(704) 930-6026',
#                         'primaryPhone': 'M',
#                         'priority': 'P',
#                         'relationship': 'Husband'}],
#  'employeeId': '890',
#  'ethnicity': 'W',
#  'federalTax': {'amount': 0.0,
#                 'filingStatus': 'M',
#                 'percentage': 0.0,
#                 'taxCalculationCode': 'D',
#                 'w4FormYear': 2020},
#  'firstName': 'Christin',
#  'gender': 'F',
#  'homeAddress': {'address1': '6559 Tahiti Way',
#                  'city': 'Flowery Branch',
#                  'country': 'USA',
#                  'emailAddress': 'christin_willett@yahoo.com',
#                  'mobilePhone': '6785954141',
#                  'phone': '6785954141',
#                  'postalCode': '30542',
#                  'state': 'GA'},
#  'lastName': 'Willett',
#  'maritalStatus': 'M',
#  'middleName': 'L',
#  'primaryStateTax': {'amount': 0.0,
#                      'exemptions': 1.0,
#                      'exemptions2': 0.0,
#                      'filingStatus': 'MJ2',
#                      'percentage': 0.0,
#                      'specialCheckCalc': 'Supp',
#                      'taxCalculationCode': 'D',
#                      'taxCode': 'GA',
#                      'w4FormYear': 2019},
#  'priorLastName': 'Christin',
#  'status': {'changeReason': 'Hire',
#             'effectiveDate': '2021-09-16T00:00:00',
#             'employeeStatus': 'A',
#             'hireDate': '2021-09-16T00:00:00',
#             'isEligibleForRehire': False,
#             'statusType': 'A'},
#  'taxSetup': {'suiState': 'GA', 'taxForm': 'W2'},
#  'veteranDescription': 'My husband is',
#  'webTime': {'chargeRate': 0.0, 'isTimeLaborEnabled': True},
#  'workAddress': {'address1': '1380 Peachtree Industrial Blvd',
#                  'address2': 'Ste 150',
#                  'city': 'Suwanee',
#                  'country': 'USA',
#                  'emailAddress': 'Christin.Willett@drrhondahoganmail.com',
#                  'location': 'GPED',
#                  'postalCode': '30024',
#                  'state': 'GA'},
#  'workEligibility': {'isI9Verified': False, 'isSsnVerified': False}}

def eod():
    import API.toolkits.eod.eod_attachments as eod
    eod.run()


eod()