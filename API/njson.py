import base64
import json as _json
import traceback
from uuid import UUID, uuid4
import arrow
import ujson as json
import xmltodict
from dict2xml import dict2xml

class JsonError(Exception):
    pass

def ctnow():
    return arrow.get().to('US/Central')

def monthlast(xdate=arrow.utcnow()):
    return arrow.get(xdate).ceil('month').format('YYYY-MM-DD')

def monthfirst(xdate=arrow.utcnow()):
    return arrow.get(xdate).floor('month').format('YYYY-MM-DD')

def monthrange(xdate=arrow.utcnow()):
    return arrow.get(xdate).floor('month').format('YYYY-MM-DD'), arrow.get(xdate).ceil('month').format('YYYY-MM-DD')

def b64e(meta):
    return base64.urlsafe_b64encode(str(meta).encode())

def b64d(meta):
    return base64.urlsafe_b64decode(str(meta).encode())

def jc(data, indent=None): #convert to json
    if isinstance(data, (tuple, list, set, dict)):
        try:
            if indent:
                return json.dumps(data, indent=indent)
            return json.dumps(data)
        except TypeError:
            return _json.dumps(data, default=str)
        except Exception as exc:
            traceback.print_exc()
            return '{}'
    # elif data and data[0:] == '{' and data[:0] == '}': #assume properly formatted json string
    elif isinstance(data, (bytes)):
        print('IS BYTES')
        return data.decode()
    elif data:  # assume properly formatted json string
        return data
    else:  # or if there isn't any data for some reason
        return '{}'

def jsonprint(data):
    return json.dumps(data, indent=4)

def jsonhtml(data):
    return f'<pre>{json.dumps(data, indent=4)}</pre>'

def lc(data): #convert to list
    if isinstance(data, list):
        return data
    elif isinstance(data, (str, bytes)):
        try:
            return json.loads(data)
        except Exception as exc:
            traceback.print_exc()
            return []

def dc(data): #convert to dict
    if isinstance(data, (dict,)):
        return data
    elif isinstance(data, (tuple, list, set)):
        try:
            return dict(data)
        except:
            return {}
    elif isinstance(data, (str, bytes)):
        try:
            try:
                return json.loads(data)
            except:
                return _json.loads(data)
        except:
            return {}

#convert csv to json
def csj(result):
    if not result: return None
    d = []
    if len(result) > 1:
        for each in result:
            jstring = each[0].replace("'", "\"")
            j = json.loads(jstring)
            j.update({'id':'%s' % each[1]})
            d.append(j)
        return json.dumps(d)
    else:
        each = result[0][0]
        jstring = each.replace("'", "\"")
        j = json.loads(jstring)
        j.update({'id':'%s' % result[0][1]})
        return json.dumps(j)

def merge_dicts(*args):
    result = {}
    for dictionary in args:
        result.update(dictionary)
    return result

def merge_request(request):
    if isinstance(request, (dict)):
        return request
    return dc(request.json) or merge_dicts(dict(request.forms), dict(request.query.decode()))

def checkuuid(u, version=4):
    try:
        UUID(u, version=version)
        return u
    except:
        return ''

def newid():
    return str(uuid4())

def xml(meta):
    return json.loads(json.dumps(xmltodict.parse(meta)))

def toxml(meta, indent=None):
    return dict2xml(dc(meta), indent=indent)


loads = json.loads
dumps = json.dumps