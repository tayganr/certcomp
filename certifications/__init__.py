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

# Output Files
FILE_NAME_CERTS = 'certifications'
FILE_NAME_CERTEXAMS = 'cert_exams'
COLUMNS_CERTS = ['CERT_ID', 'LEVEL', 'CERTIFICATION', 'LINK', 'REQUIREMENTS']
COLUMNS_CERTEXAMS = ['CERT_ID', 'EXAM_ID']

# Microsoft Certifications
URL_CERTS = 'https://www.microsoft.com/learning/proxy2/LocAPIPROD/api/values/GetContent?localeCode=en-us&property=mslCertifications'

def main(req: func.HttpRequest) -> func.HttpResponse:

    # 1. Init
    data_certs = []
    data_certexams = []
    response = {}
    response['certifications'] = {}

    # CERTIFICATIONS ##################################################
    http_response = requests.get(URL_CERTS)
    http_data = json.loads(http_response.content)

    for level in http_data['mslCertifications']:
        response['certifications'][level] = {}
        for cert in http_data['mslCertifications'][level]:
            # Certification Name and URL
            cert_id = cert['ID']
            cert_title = cert['name']
            cert_url = 'https://www.microsoft.com/en-us/learning/{0}'.format(cert['url'])
            logging.info(cert_title)

            cert_response = requests.get(cert_url)
            cert_tree = html.fromstring(cert_response.content)
            
            if(level == 'Role-based'):
                requirement = 'Required Exams:'
                response['certifications'][level][cert_title] = []
                for exam in cert_tree.xpath('//*[@id="msl-certification-azure"]/div[1]/section/div/div/p[1]/a'):
                    exam_id = exam.xpath('./text()')[0].replace('Exam ','').replace('Transition ','').strip()
                    requirement += ' {0};'.format(exam_id)
                    exam_row = [cert_id, exam_id]
                    data_certexams.append(exam_row)
                    response['certifications'][level][cert_title].append(exam_id)
                
                logging.info(requirement)
                
                cert_row = [cert_id, level, cert_title, cert_url, requirement]
                data_certs.append(cert_row)
            elif(level != 'MCE'):
                script_text = cert_tree.xpath('//*[@id="content"]/div/div/script/text()')[0]
                api_key = script_text.split('fnLoadCertificationPage')[1].split('"')[1]
                cert_api = 'https://www.microsoft.com/learning/proxy2/LocAPIPROD/api/values/GetContent?localeCode=en-us&property={0}'.format(api_key)
                cert_response = requests.get(cert_api)
                cert_data = json.loads(cert_response.content)
                requirement = getRequirement(cert_data[api_key], level)
                cert_row = [cert_id, level, cert_title, cert_url, requirement]
                data_certs.append(cert_row)
                response['certifications'][level][cert_title] = []

                # Exam - Certification 
                for exam in cert_data[api_key]['cert_page_details']['steps']['step2']['exams']:
                    exam_id = exam['exam_code'].replace('Exam ', '')
                    exam_row = [cert_id, exam_id]
                    data_certexams.append(exam_row)
                    response['certifications'][level][cert_title].append(exam_id)
        
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

def getRequirement(cert, level):
    step_tagline = cert['cert_page_details']['steps']['step2']['step_tagline'].replace(' Be sure to explore the exam prep resources.', '')
    whats_involved = cert['cert_page_details']['what_is_involved']['step2_exams'].replace(' Be sure to explore the exam prep resources.', '')

    skills_reqd = None
    if level == 'MCSD' or level == 'MCSE':
        skills_reqd = cert['cert_page_details']['what_is_involved']['step1_skills']

    if '70-' in step_tagline:
        requirement = step_tagline
    else:
        requirement = whats_involved

    if skills_reqd:
        requirement = 'Step 1: ' + skills_reqd + ' Step 2: ' + requirement
    
    return requirement

def transformCertId(link):
    link = link.replace('.aspx','')
    link = link.replace('https://www.microsoft.com/en-us/learning/', '')
    cert_id = ''
    for part in link.split('-'):
        cert_id += part.title()
    return cert_id[0].lower() + cert_id[1:]

def getCertId(url):
    cert_id = None
    page = requests.get(url)
    tree = html.fromstring(page.content)
    js = tree.xpath('//*[@id="content"]/div/div/script/text()')
    if len(js):
        cert_id = js[0].split('"')[1]
    return cert_id
