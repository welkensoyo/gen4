import xlrd, traceback, uuid, os, hashlib
from tabula import read_pdf, convert_into
from bottle import request
import API.njson as json
from PIL import Image

savedimagepath = db.config_opts['savedimagepath']
imageurlpath = '/{}/%s/%s/%simages/%s'.format(db.config_opts['picsurl'])


def md5(text):
    x = hashlib.md5()
    x.update(text)
    return x.hexdigest()


class UploadInfo(object):
    def __init__(self):
        self.file = request.files.get('fileUpload')
        if self.file:
            self.filename = self.file.filename.lower()
            print(self.filename)
            if os.path.exists(self.filename):
                os.remove(self.filename)
            self.file.save(self.filename)
            self.result = None

    def read(self, json=False, mtables=False, pages='all'):
        if not json:
            try:
                return read_pdf(self.filename, pages=pages, multiple_tables=mtables, error_bad_lines=False,
                                spreadsheet=mtables, silent=True)
            except:
                return read_pdf(self.filename, encoding='latin1', pages=pages, multiple_tables=mtables,
                                error_bad_lines=False, spreadsheet=mtables, silent=True)
        else:
            try:
                return read_pdf(self.filename, pages=pages, output_format="json", multiple_tables=mtables,
                                spreadsheet=mtables, silent=True)
            except:
                return read_pdf(self.filename, encoding='latin1', pages=pages, output_format="json",
                                multiple_tables=mtables, spreadsheet=mtables, silent=True)

    def csv(self, dest, mtables=True):
        try:
            convert_into(self.filename, dest, output_format="csv", pages='all', multiple_tables=mtables)
        except:
            convert_into(self.filename, dest, output_format="csv", encoding='latin1', pages='all',
                         multiple_tables=mtables)
        return True

    def exel(self):
        try:
            if not self.filename:
                return xlrd.open_workbook(file_contents=self.file.file.read())
            else:
                return xlrd.open_workbook(self.filename)
        except:
            traceback.print_exc()
            return False

    def analyze(self, prx=1.40):
        tag = self.max('info', 'TAG')

