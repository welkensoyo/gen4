import API.dbpg as db
from API.njson import jc, dc
from genson import SchemaBuilder
qry = {
    'report': ''' SELECT meta FROM sdb.report WHERE name = %s ''',
    'upsert': ''' INSERT INTO sdb.report (name, meta) VALUES (%s, %s)  
                    ON CONFLICT (name) 
                    DO UPDATE set meta = %s RETURNING meta'''
}

class Report:
    def __init__(self, name, build_schema=True):
        self.build_schema = build_schema
        self.name = name
        self.meta = {}
        self.schema = {}
        self.sb = SchemaBuilder()
        self.get()

    def get(self):
        self.meta = db.fetchreturn(qry['report'], self.name)
        self.check_schema()
        return self.meta

    def check_schema(self):
        if self.meta:
            self.schema = self.meta.pop('_schema', '')
            if not self.schema and self.build_schema:
                self.sb.add_schema({"type": "object", "properties": {}, "title": self.name.upper(), "format": "grid"})
                self.sb.add_object(self.meta)
                self.schema = self.sb.to_schema()

    def columns(self):
        return self.meta.get('columns')

    def data(self):
        return self.meta.get('data')

    def callback(self):
        return self.meta.get('callback')

    def grid(self):
        return self.meta.get('grid')

    def new(self, meta):
        return self.update(meta)
    def update(self, meta):
        self.meta = dc(meta)
        self.check_schema()
        self.meta['_schema'] = self.schema
        meta = jc(meta)
        return db.fetchreturn(qry['upsert'], self.name, meta, meta)
