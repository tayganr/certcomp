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
URL_CERTS_NEW = 'https://www.microsoft.com/en-us/learning/browse-new-certification.aspx'
URL_CERTS_LEGACY = 'https://www.microsoft.com/learning/proxy2/LocAPIPROD/api/values/GetContent?localeCode=en-us&property=certificationCards'

def main(req: func.HttpRequest) -> func.HttpResponse:

    # 1. Init
    data_certs = []
    data_certexams = []
    response = {}
    response['certifications'] = {}

    # 2. LEGACY CERTIFICATIONS ##################################################
    http_response = requests.get(URL_CERTS_LEGACY)
    http_data = json.loads(http_response.content)

    for level in http_data['certificationCards']:
        response['certifications'][level] = {}
        for cert in http_data['certificationCards'][level]:
            # Certification Name and URL
            cert_title = cert['name']
            cert_url = 'https://www.microsoft.com/en-us/learning/{0}'.format(cert['url'])
            cert_id = getCertId(cert_url)
            logging.info(cert_title)

            if(cert_id):
                # Certification Details
                cert_api = 'https://www.microsoft.com/learning/proxy2/LocAPIPROD/api/values/GetContent?localeCode=en-us&property={0}'.format(cert_id)
                cert_response = requests.get(cert_api)
                cert_data = json.loads(cert_response.content)
                requirement = getRequirement(cert_data[cert_id], level)

                cert_row = [cert_id, level, cert_title, cert_url, requirement]
                data_certs.append(cert_row)
                response['certifications'][level][cert_title] = []

                # Exam - Certification 
                for exam in cert_data[cert_id]['cert_page_details']['steps']['step2']['exams']:
                    exam_id = exam['exam_code'].replace('Exam ', '')
                    exam_row = [cert_id, exam_id]
                    data_certexams.append(exam_row)
                    response['certifications'][level][cert_title].append(exam_id)

    # 3. ROLE BASED CERTIFICATIONS ##################################################
    http_response2 = requests.get(URL_CERTS_NEW)
    tree2 = html.fromstring(http_response2.content)

    for card2 in tree2.xpath('//section[@class="msl-certification-card"]'):
        role_based_cert_title = card2.xpath('./div/h3/text()')
        if len(role_based_cert_title) > 0:
            role_based_cert_title = role_based_cert_title[0]
            role_based_cert_link = 'https://www.microsoft.com/en-us/learning/' + card2.xpath('.//a')[0].attrib["href"]
            role_based_cert_id = transformCertId(role_based_cert_link)

            role_based_cert_level = None
            role_based_cert_img = card2.xpath('.//picture/img/@src')[0]

            if 'associate' in role_based_cert_img:
                role_based_cert_level = 'Associate'
            elif 'expert' in role_based_cert_img:
                role_based_cert_level = 'Expert'
            else:
                role_based_cert_level = 'Fundamentals'

            logging.info(role_based_cert_title)

            rbc_page = requests.get(role_based_cert_link)
            rbc_tree = html.fromstring(rbc_page.content)

            role_based_cert_reqs = 'Pass the following exam(s):'
            for link in rbc_tree.xpath('//a[@class="msl-body-regular msl-cp-hyperlink"]'):
                if len(link.xpath('./text()')):
                    link_text = link.xpath('./text()')[0]
                    exam_text = link_text.replace('Schedule to take Exam ','')
                    if len(exam_text) < len(link_text):
                        role_based_cert_reqs += ' {0};'.format(exam_text)
                        rbc_exam_row = [role_based_cert_id, exam_text]
                        data_certexams.append(rbc_exam_row)
            
            rbc_cert_row = [role_based_cert_id, role_based_cert_level, role_based_cert_title, role_based_cert_link, role_based_cert_reqs]
            data_certs.append(rbc_cert_row)
        
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
