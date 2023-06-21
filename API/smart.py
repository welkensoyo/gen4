import smartsheet
import os.path

_dir = os.path.dirname(os.path.abspath(__file__))

class API():

    access_token = '8QG48BPqXExoBp6Ebm9xODcKNzjLP7CClwkWG'

    def __init__(self, filename, logs=True):
        self.filename = filename
        self.column_map = {}
        self.smart = smartsheet.Smartsheet(self.access_token)
        self.smart.errors_as_exceptions(logs)
        self.sheet = None
        self.emails = {}

    def open(self):
        action = self.smart.Sheets.list_sheets(include_all=True)
        sheets = action.data
        for sheet in sheets:
            if sheet.name == self.filename:
                self.sheet = sheet

    def get_cell_by_column_name(self, row, column_name):
        column_id = self.column_map[column_name]
        return row.get_column(column_id)

    def sheet2dict(self):
        s = self.smart.Sheets.get_sheet(self.sheet.id)
        rx = {}
        rx['columns'] = self.columns()
        for row in s.rows:
            rx[row.id]=[row.cells[c].value for c in range(0, len(s.columns))]
        return rx

    def sheet2list(self):
        s = self.smart.Sheets.get_sheet(self.sheet.id)
        x = []
        for row in s.rows:
            rx = []
            for c in range(0, len(s.columns)):
                 rx.append(row.cells[c].value)
            x.append(rx)
        return x

    def update(self, rowid, columnid, value):
        new_cell = self.smart.models.Cell()
        new_cell.column_id = int(columnid)
        new_cell.value = value
        new_cell.strict = False
        # Build the row to update
        new_row = self.smart.models.Row()
        new_row.id = int(rowid)
        new_row.cells.append(new_cell)
        self.smart.Sheets.update_rows(self.sheet.id, [new_row])

    def updatemany(self, data):
        rows = []
        old_rowid = None
        nrow = None
        for rowid,columnid,value in data:
            if rowid != old_rowid:
                if old_rowid:
                    rows.append(nrow)
                nrow = self.smart.models.Row()
                nrow.id = int(rowid)
            cell = self.smart.models.Cell()
            cell.column_id = int(columnid)
            cell.value = str(value)
            cell.strict = False
            nrow.cells.append(cell)
            old_rowid = rowid
        if nrow:
            rows.append(nrow)
        self.smart.Sheets.update_rows(self.sheet.id, rows)

    def add(self, data, skip=None):
        if not skip: skip = []
        cols = self.columns()
        rows = []
        for l in data:
            nrow = self.smart.models.Row()
            nrow.to_bottom = True
            for index, v in enumerate(l):
                if index in skip: continue
                nrow.cells.append({
                    'column_id' : int(cols[index]),
                    'value' : v or ''
                })
            rows.append(nrow)
        self.smart.Sheets.add_rows(self.sheet.id, rows)
        return data

    def columns(self):
        s = self.smart.Sheets.get_sheet(self.sheet.id)
        cols = self.smart.Sheets.get_columns(self.sheet.id)
        return [str(i.id) for i in cols.result]
