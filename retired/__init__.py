import os
import logging
import requests
import json
from datetime import datetime
import pandas as pd
from lxml import html
from azure.storage.blob import BlockBlobService
import azure.functions as func

# Azure Blob Storage
ACCOUNT_NAME = os.environ.get('CERT_COMP_STORAGE_NAME')
ACCOUNT_KEY = os.environ.get('CERT_COMP_STORAGE_KEY')
CONTAINER_NAME = os.environ.get('CERT_COMP_STORAGE_CONTAINER')

# Output File
FILE_NAME = 'retired'
COLUMNS = ['EXAM_ID', 'RETIREMENT_DATE']

# Microsoft Retired Exams
URL = 'https://www.microsoft.com/en-us/learning/retired-certification-exams.aspx'
MONTHS = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE',
    'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']

def main(req: func.HttpRequest) -> func.HttpResponse:

    # 1. Init
    data = []
    response = {}

    # 2. HTTP Request
    page = requests.get(URL)
    tree = html.fromstring(page.content)

    # 3. Get Data
    for section in tree.xpath('//*[@id="content"]/div/div/div/div[3]/div/table'):
        retirement_date_raw = section.xpath('.//tbody/tr/td[1]/text()')
        logging.info('RETIRE: ' + retirement_date_raw[0])

        if retirement_date_raw:
            retirement_date_raw = retirement_date_raw[0].strip()
            retirement_date_raw = retirement_date_raw.replace('Retiring on ', '')

            retirement_month = None
            retirement_date = None
            for month in MONTHS:
                if month in retirement_date_raw.upper():
                    retirement_month = month
                    retirement_year = retirement_date_raw[-4:]
                    retirement_day = retirement_date_raw.upper().replace(retirement_month, '').replace(retirement_year, '').replace(',', '').replace(' ', '')
                    retirement_date_text = '{0} {1} {2}'.format(retirement_day, retirement_month, retirement_year)
                    retirement_date = datetime.strptime(retirement_date_text, '%d %B %Y')
            
            for exam in section.xpath('.//tbody/tr/td[2]/p/a'):
                exam_id = exam.xpath('./text()')[0].strip().replace(':', '')
                row = [exam_id, retirement_date]
                response[exam_id] = str(retirement_date)
                logging.info(exam_id)
                data.append(row)
      
    # 4. Write to Azure Blob Storage
    block_blob_service = BlockBlobService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)     
    write_to_blob(block_blob_service, data, COLUMNS, FILE_NAME)

    # 5. HTTP Response
    return func.HttpResponse(json.dumps(response),headers={'Content-Type':'application/json'})

def write_to_blob(block_blob_service, data, columns, filename):
    today = datetime.today()
    folderpath = '{0}/{1}'.format(filename, today.strftime('%Y/%m/%d'))
    filename = '{0}/{1}.csv'.format(folderpath, filename)
    df = pd.DataFrame(data, columns=columns)
    csv = df.to_csv(index=False, encoding='utf-8')
    block_blob_service.create_blob_from_text(container_name=CONTAINER_NAME, blob_name=filename, text=csv, encoding='utf-8')
