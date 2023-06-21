from gevent import spawn_later

import gateway.db as db
from gateway.config import systemoptions
from gateway.njson import jc, dc, checkuuid

e_table = systemoptions['events']
t_table = systemoptions['transaction']

qry = {
    'id' : f'''SELECT id, meta, created FROM {e_table} WHERE id = %s ''',
    'get' : f'''  with READY as ( 
                    UPDATE cache.events SET state='RETRIEVED' WHERE id IN ( 
                    SELECT id FROM cache.events 
                    WHERE "state" = 'READY' AND (id::text=%s OR meta @> %s)          
                    ORDER BY created FOR UPDATE SKIP LOCKED LIMIT 1
                 ) RETURNING id, meta, created) 
                 SELECT * FROM READY ORDER BY created ASC ''',
    'ready' : f''' with READY as ( UPDATE {e_table} SET state='RETRIEVED'
            WHERE id IN ( SELECT id FROM {e_table} WHERE "state" = 'READY' and clientid = %s
            ORDER BY created FOR UPDATE SKIP LOCKED) RETURNING id, meta, created)
            SELECT * FROM READY ORDER BY created ASC ''',
    'all' : f''' SELECT id, meta, created, state FROM {e_table} WHERE clientid = %s  ''',
    'new' : f''' INSERT INTO {e_table} (clientid, meta) VALUES (%s, %s) RETURNING id''',
    'tranmerge' : f''' UPDATE {t_table} SET meta = jsonb_update(meta, %s) WHERE meta @> %s RETURNING meta->>'id' ''',
    'tranupdate' : f''' UPDATE {t_table} SET meta = meta||%s WHERE meta @> %s RETURNING meta->>'id' ''',
    'next' : f''' UPDATE {e_table} SET "state"='RETRIEVED' 
            WHERE id IN ( SELECT id FROM {e_table} WHERE "state" = 'READY' and clientid = %s 
            ORDER BY created ASC FOR UPDATE SKIP LOCKED limit 1) RETURNING clientid, meta, created ''',
    'flush' : f'''DELETE FROM {e_table} WHERE state = 'RETRIEVED' and clientid=%s RETURNING clientid ''',
    'clean' : f'''DELETE FROM {e_table} WHERE meta@>%s RETURNING id ''',
}


def callback(clientid, option):
    if not option:
        return next(clientid)
    elif option == 'ready':
        return ready(clientid)
    elif option == 'all':
        return all(clientid)
    elif option in ('delete','flush','clean'):
        return flush(clientid)
    else:
        return get(option)

def get(id):
    def checkjson(id):
        if dc(id):
            return jc(id)
        return '{"":""}'
    return db.fetchone(qry['get'], checkuuid(id), checkjson(id))

def ready(clientid):
    return db.fetchone(qry['ready'], clientid)

def all(clientid):
    return db.fetchall(qry['all'], clientid)

def clean(meta):
    db.fetchall(qry['clean'], jc(meta))

def new(clientid, meta):
    if 'externaltransactionid' in meta: #curpay update
        id = meta['externaltransactionid']
        txstatus = meta['txstatus'].strip().lower()
        msg = txstatus
        result = meta['iscanceled']
        status = True
        if result in (True, 'true'):
            status = False
            msg = meta['canceledreason']
            spawn_later(600, clean, {'externaltransactionid': id})
        if txstatus == 'completed' :
            spawn_later(600, clean, {'externaltransactionid':id})
        coin = meta.get('currencycode','')
        db.execute(qry['tranupdate'], jc({'message':f'{coin}: {msg}', 'status': status}), f'{{"id" : "{id}"}}')
        db.execute(qry['tranmerge'], jc({'details': [meta]}), f'{{"id" : "{id}"}}')
    return db.fetchreturn(qry['new'], clientid, jc(meta))

def next(clientid):
    return db.fetchone(qry['next'], clientid)

def flush(clientid):
    return [each[0] for each in db.fetchall(qry['flush'], clientid)]
