import pandas as pd


class GetExcel:
    def __init__(self, filename, sheet):
        self.path = 'JacksonShacklett'
        self.filename = filename
        self.sheet = sheet
        self.table = {pd.set_option('display.max_columns', None),
                      pd.set_option('display.max_rows', None),
                      pd.set_option('display.width', 1000)}

    def excel_fd(self):
        df = pd.read_excel('C:\\Users\\'+self.path+'\\OneDrive - Specialty Dental Brands\\Finance and Data Team\\Location Mapping\\'+self.filename, sheet_name=self.sheet)
        return df
