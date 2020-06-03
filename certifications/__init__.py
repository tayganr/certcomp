import os
import logging
import requests
import json
import math
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlockBlobService
import azure.functions as func

# Azure Blob Storage
ACCOUNT_NAME = os.environ.get('CERT_COMP_STORAGE_NAME')
ACCOUNT_KEY = os.environ.get('CERT_COMP_STORAGE_KEY')
CONTAINER_NAME = os.environ.get('CERT_COMP_STORAGE_CONTAINER')

# Output Files
FILE_NAME_CERTS = 'certifications'
FILE_NAME_CERTEXAMS = 'cert_exams'
COLUMNS_CERTS = ['CERT_ID', 'LEVEL', 'CERTIFICATION', 'LINK', 'REQUIREMENTS']
COLUMNS_CERTEXAMS = ['CERT_ID', 'EXAM_ID']

# API
API = 'https://docs.microsoft.com/api/contentbrowser/search/certifications'
BATCH = 30

def main(req: func.HttpRequest) -> func.HttpResponse:

    # 1. Init
    data_certs = []
    data_certexams = []
    response = {}
    response = {}

    # Calculate number of pages
    number_of_items = getResponse(0)['count']
    number_of_pages = math.ceil(number_of_items/BATCH)

    # Get Exam Data
    for page in range(0,number_of_pages):
        skip = page * 30
        data = getResponse(skip)
        for result in data['results']:
            cert_id = result['uid']
            cert_type = result['type']
            cert_title = result['title']
            cert_url = 'https://docs.microsoft.com/en-us{0}'.format(result['url'])

            # JSON Response
            response[cert_id] = {}
            response[cert_id]['title'] = cert_title
            response[cert_id]['type'] = cert_type
            response[cert_id]['url'] = cert_url
            response[cert_id]['exams'] = []

            cert_req = 'Exam '
            for exam in result['exams']:
                exam_id = exam['display_name']
                cert_req += '{0};'.format(exam_id)
                exam_row = [cert_id, exam_id]
                data_certexams.append(exam_row)
                response[cert_id]['exams'].append(exam_id)
            
            cert_row = [cert_id, cert_type, cert_title, cert_url, cert_req]
            data_certs.append(cert_row)
    
    # 4. Write to Azure Blob Storage
    block_blob_service = BlockBlobService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)     
    write_to_blob(block_blob_service, data_certs, COLUMNS_CERTS, FILE_NAME_CERTS)
    write_to_blob(block_blob_service, data_certexams, COLUMNS_CERTEXAMS, FILE_NAME_CERTEXAMS)

    # 5. HTTP Response
    return func.HttpResponse(json.dumps(response),headers={'Content-Type':'application/json'})

def write_to_blob(block_blob_service, data, columns, filename):
    today = datetime.today()
    folderpath = '{0}/{1}'.format(filename, today.strftime('%Y/%m/%d'))
    filename = '{0}/{1}.csv'.format(folderpath, filename)
    df = pd.DataFrame(data, columns=columns)
    csv = df.to_csv(index=False, encoding='utf-8')
    block_blob_service.create_blob_from_text(container_name=CONTAINER_NAME, blob_name=filename, text=csv, encoding='utf-8')

def getResponse(skip):
    params = {
        'environment':'prod',
        'locale':'en-us',
        'facet':'levels',
        'facet':'products',
        'facet':'resource_type',
        'facet':'roles',
        'facet':'type',
        '$filter':"((resource_type eq 'certification'))",
        '$orderBy':"last_modified desc",
        '$skip':skip,
        '%$top':BATCH
    }
    response = requests.get(API, params=params)
    data = json.loads(response.content)
    return data
