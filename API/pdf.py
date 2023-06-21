from tabula import read_pdf, convert_into
from pathlib import Path
def save_csv(file):
    # file = "C:\\Users\\DerekBartron\\OneDrive - Specialty Dental Brands\\Documents\\Orthobanc Active Listings.pdf"
    p = Path(file)
    return convert_into(file, f'{p.parent}\\{p.stem}.csv', output_format="csv", pages='all')

