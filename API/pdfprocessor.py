# import fitz  # PyMuPDF
import pandas as pd
from pprint import pprint
import os
import glob
import re
from API.files import PDFProcess


class PDF:
    def __init__(self, folder, output):
        self.folder = folder
        self.output = output
        self.data = None

    def extract_pdf(self, pdf_path):
        # Extract the data
        tables = PDFProcess(filename=pdf_path).read(multiple_tables=True, output_format='dataframe')
        for table in tables:
            for row in table.values.tolist():
                print(row)
        # Ensure tables are a list of dataframes
        if isinstance(tables, list) and all(isinstance(tbl, pd.DataFrame) for tbl in tables):
            df = pd.concat(tables, ignore_index=True)  # Concatenate all tables into one DataFrame
        else:
            raise ValueError("The extracted data is not in the expected format.")

        # print(type(df))
        # for line in df.iterrows():
        #     print(line)



    def extract_text_from_pdf(self, pdf_path):
        doc = pd.DataFrame(PDFProcess(filename=pdf_path).read(multiple_tables=True))
        data = []
        for page_num in range(len(doc)):
           page = doc.load_page(page_num)
           text = page.get_text("text")
           lines = text.split('\n')
           data.extend(lines)
        self.data = data
        return self

    def parse_data(self, raw_data, pdf_filename):
       headers = ["PDF Source", "Date", "Patient Name", "Th", "Code", "Description", "OS", "Charges", "Payments", "BT", "Prov", "Phone #"]
       records = []
       date_pattern = re.compile(r"\d{2}/\d{2}/\d{4}")
       phone_pattern = re.compile(r"\(\d{3}\)\d{3}-\d{4}")
       charges_pattern = re.compile(r"(?<!-)\b\d+\.\d{2}\b")  # Matches numbers with two decimals
       payments_pattern = re.compile(r"-\d+\.\d{2}")  # Matches -prefaced numbers with two decimals
       provider_pattern = re.compile(r"\b\w{4}\(\)")  # Matches a 4-character provider code followed by ()
       code_pattern = re.compile(r"D\d{4}")
       th_pattern = re.compile(r"\b\d{1,2}\b")  # Matches 1-2 digit numbers (often used for Th)
       current_record = None
       for line in raw_data:
           line = line.strip()
           if not line or "Date Patient Name" in line or "DAY SHEET" in line:
               continue
           if date_pattern.match(line):  # Match the date at the start
               if current_record:
                   records.append(current_record)
               parts = line.split(maxsplit=1)
               current_record = {
                   "PDF Source": pdf_filename,
                   "Date": parts[0].strip(),
                   "Patient Name": parts[1].strip() if len(parts) > 1 else "",
                   "Th": "",
                   "Code": "",
                   "Description": "",
                   "OS": "",
                   "Charges": "",
                   "Payments": "",
                   "BT": "",
                   "Prov": "",
                   "Phone #": ""
               }
           elif phone_pattern.search(line):  # Match phone #
               phone_number = phone_pattern.search(line).group()
               current_record["Phone #"] = phone_number
               line = line.replace(phone_number, "").strip()
               current_record["Description"] += " " + line.strip()
           elif code_pattern.search(line):
               # Match Th (1-2 digit number)
               th_match = th_pattern.search(line)
               if th_match:
                   current_record["Th"] = th_match.group()
                   line = line.replace(th_match.group(), "").strip()
               # Match the Code
               match = code_pattern.search(line)
               if match:
                   current_record["Code"] = match.group()
                   remaining = line.replace(match.group(), "").strip()
                   # Extract Charges if existing or showing up?
                   charges_match = charges_pattern.search(remaining)
                   if charges_match:
                       current_record["Charges"] = charges_match.group()
                       remaining = remaining.replace(charges_match.group(), "").strip()
                   # Extract Payments if showing up
                   payments_match = payments_pattern.search(remaining)
                   if payments_match:
                       current_record["Payments"] = payments_match.group()
                       remaining = remaining.replace(payments_match.group(), "").strip()
                   # Extract Provider if showing up
                   provider_match = provider_pattern.search(remaining)
                   if provider_match:
                       current_record["Prov"] = provider_match.group()
                       remaining = remaining.replace(provider_match.group(), "").strip()
                   # Anything left SHOULD BE considered description, but check indiv parse later in case
                   current_record["Description"] = remaining.strip()
           else:
               if current_record:
                   if current_record["Description"]:
                       current_record["Description"] += " " + line.strip()
                   else:
                       current_record["Description"] = line.strip()
       if current_record:  # Add the last record
           records.append(current_record)
       return headers, records

    def process_pdfs_in_folder(self):
       all_data = []
       headers = None
       for pdf_file in glob.glob(os.path.join(self.folder, "*.pdf")):
           print(f"Processing {pdf_file}...")
           raw_data = self.extract_text_from_pdf(pdf_file)
           pdf_filename = os.path.basename(pdf_file)
           headers, records = self.parse_data(raw_data, pdf_filename)
           all_data.extend(records)
       if headers and all_data:
           df = pd.DataFrame(all_data, columns=headers)
           df.to_excel(self.output, index=False)
           print(f"All data has been successfully saved to {self.output}")
       return self


if __name__=='__main__':
    # Customize these lines with the correct paths
    # pdf_folder_path = r"C:/Users/AmandaLevonitis/OneDrive - Gen4 Dental Partners/Desktop/Test Updated/All Dentrix PDF Source"  # Path to the folder containing PDFs
    # output_path = r"C:/Users/AmandaLevonitis/OneDrive - Gen4 Dental Partners/Desktop/Test Updated/Combined Output.xlsx"  # Path to the output Excel file
    pdf_folder_path = "/home/nfty/Downloads/gen4/pdfs/"  # Path to the folder containing PDFs
    output_path = "/home/nfty/Downloads/gen4/pdfs/Combined Output.xlsx"  # Path to the output Excel file
    x = PDF(folder=pdf_folder_path, output= output_path)
    x.extract_pdf(pdf_folder_path+'ACD Emporia.KSACD0004.1396.0 1.pdf')