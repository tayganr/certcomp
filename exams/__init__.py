import os
import logging
import requests
import json
import math
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlockBlobService
import azure.functions as func
from .mslearn import *

# Azure Blob Storage
ACCOUNT_NAME = os.environ.get('CERT_COMP_STORAGE_NAME')
ACCOUNT_KEY = os.environ.get('CERT_COMP_STORAGE_KEY')
CONTAINER_NAME = os.environ.get('CERT_COMP_STORAGE_CONTAINER')

# Output Files
FILE_NAME_EXAMS = 'exams'
FILE_NAME_PREP = 'preparation'
COLUMNS_EXAMS = ['EXAM_ID', 'EXAM', 'LINK', 'PUBLISHED', 'BETA']
COLUMNS_PREP = ['EXAM_ID', 'PREP_TYPE', 'PREP_TEXT', 'LINK']

# API
API = 'https://docs.microsoft.com/api/contentbrowser/search/certifications'
BATCH = 30

def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1. Init
    data_exam = []
    data_prep = []
    response = {}

    # Calculate number of pages
    number_of_items = getResponse(0)['count']
    number_of_pages = math.ceil(number_of_items/BATCH)
    
    # Get Exam Data
    for page in range(0,number_of_pages):
        skip = page * 30
        data = getResponse(skip)
        for result in data['results']:
            exam_id = result['exam_display_name']
            exam_title = result['title']
            exam_url = 'https://docs.microsoft.com/en-us{0}'.format(result['url'])
            exam_modified = result['last_modified']
            exam_beta = isbeta(exam_title)
            exam_row = [exam_id, exam_title, exam_url, exam_modified, exam_beta]
            data_exam.append(exam_row)

            # JSON Response
            response[exam_id] = {}
            response[exam_id]['title'] = exam_title
            response[exam_id]['url'] = exam_url
            response[exam_id]['published'] = exam_modified
            response[exam_id]['beta'] = exam_beta

    # data_prep = add_ms_learn_entries(data_prep)
    data_prep = append_learn(data_prep)

    # 4. Write to Azure Blob Storage
    block_blob_service = BlockBlobService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)     
    write_to_blob(block_blob_service, data_exam, COLUMNS_EXAMS, FILE_NAME_EXAMS)
    write_to_blob(block_blob_service, data_prep, COLUMNS_PREP, FILE_NAME_PREP)

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
        '$filter':"((resource_type eq 'examination'))",
        '$orderBy':"last_modified desc",
        '$skip':skip,
        '%$top':BATCH
    }
    response = requests.get(API, params=params)
    data = json.loads(response.content)
    return data

def isbeta(title):
    title_suffix = title[-5:][:4]
    if title_suffix == 'beta':
        return True
    else:
        return False