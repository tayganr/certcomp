import os
import json
import logging
from io import StringIO
import pandas as pd
from azure.storage.blob import BlockBlobService
import azure.functions as func

# Azure Blob Storage
ACCOUNT_NAME = os.environ.get('CERT_COMP_STORAGE_NAME')
ACCOUNT_KEY = os.environ.get('CERT_COMP_STORAGE_KEY')
CONTAINER_NAME = os.environ.get('CERT_COMP_STORAGE_CONTAINER2')

def main(req: func.HttpRequest) -> func.HttpResponse:

    # 1. Init
    response = {}
    response['status'] = 'OK'

    # Azure Blob Storage
    block_blob_service = BlockBlobService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)

    # Exams
    df0 = get_df(block_blob_service, 'exams.csv', ['EXAM_ID'])
    df0['SEARCH'] = df0['EXAM_ID']

    df1 = get_df(block_blob_service, 'exams.csv', ['EXAM_ID','EXAM'])
    df1.rename(columns={'EXAM_ID': 'EXAM_ID', 'EXAM': 'SEARCH'}, inplace=True)

    # Competencies
    df2 = get_df(block_blob_service, 'comp_exams.csv', ['EXAM','COMPETENCY'])
    df2.rename(columns={'EXAM': 'EXAM_ID', 'COMPETENCY': 'SEARCH'}, inplace=True)

    # Certifications
    df3 = get_df(block_blob_service, 'cert_exams.csv', ['EXAM_ID', 'CERT_ID'])
    df3.rename(columns={'EXAM_ID': 'EXAM_ID', 'CERT_ID': 'SEARCH'}, inplace=True)

    # Preparation
    df4 = get_df(block_blob_service, 'preparation.csv', ['EXAM_ID','PREP_TEXT'])
    df4.rename(columns={'EXAM_ID': 'EXAM_ID', 'PREP_TEXT': 'SEARCH'}, inplace=True)

    frames = [df0, df1, df2, df3, df4]
    result = pd.concat(frames)
    result.drop_duplicates(inplace=True)
    
    write_to_blob(block_blob_service, result, 'search.csv')   


    # 5. HTTP Response
    return func.HttpResponse(json.dumps(response),headers={'Content-Type':'application/json'}) 

def get_df(block_blob_service, filename, columns):
    data = block_blob_service.get_blob_to_text(container_name=CONTAINER_NAME, blob_name=filename, encoding='utf-8')
    df = pd.read_csv(StringIO(data.content))
    return df[columns]

def write_to_blob(block_blob_service, df, filename):
    csv = df.to_csv(index=False, encoding='utf-8')
    block_blob_service.create_blob_from_text(container_name=CONTAINER_NAME, blob_name=filename, text=csv, encoding='utf-8')
