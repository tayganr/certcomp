import os
import logging
import requests
import json
import math
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
FILE_NAME_COMPS = 'competencies'
FILE_NAME_COMPEXAMS = 'comp_exams'
COLUMNS_COMPS = ['AREA', 'COMPETENCY', 'LINK']
COLUMNS_COMPEXAMS = ['COMPETENCY', 'EXAM']

# Microsoft Competencies
URL = 'https://partner.microsoft.com/en-us/membership/competencies'
XPATH_COMPETENCY_AREAS = '//div[@class="panel-group hidden-md-x hidden-lg simple-tabs-accordion-sm"]/div'
XPATH_COMPETENCIES = './/div[@class="col-xs-12 col-sm-6 col-md-x-4 clickable-panel column-content-item"]'

# CERT API
CERT_API = 'https://docs.microsoft.com/api/contentbrowser/search/certifications'
CERT_BATCH = 30
MATCH = 'microsoft.com/en-us/learn'

def main(req: func.HttpRequest) -> func.HttpResponse:

    # 1. Init
    data_comps = []
    data_compexams = []
    response = {}
    response['competencies'] = {}
    cert_dict = get_cert_dict()

    # 2. HTTP Request
    page = requests.get(URL)
    tree = html.fromstring(page.content)

    # 3a. Get Data - Competencies
    for area in tree.xpath(XPATH_COMPETENCY_AREAS):
        competency_area = area.xpath('.//span[@class="accordion-heading-text"]/text()')[0]
        response['competencies'][competency_area] = []
        logging.info(competency_area)

        for comp in area.xpath(XPATH_COMPETENCIES):
            competency = comp.xpath('.//h3[@class="subhead2 headline-hoverable"]/text()')[0]
            logging.info(competency)
            competency_link = comp.xpath('.//a[@class="cta cta-x cta-x-secondary"]')[0].attrib["href"]
            row = [competency_area, competency, competency_link]
            data_comps.append(row)
            response['competencies'][competency_area].append({"competency":competency,"link":competency_link})

    # 3b. Get Data - Competency-Exams
    for area in response['competencies']:
        for competency in response['competencies'][area]:
            logging.info('Competency: ' + competency['competency'])
            comp_page = requests.get(competency['link'])
            comp_tree = html.fromstring(comp_page.content)
            
            link_dict = {}
            for links in comp_tree.xpath('.//a'):
                if 'href' in links.attrib:
                    link = links.attrib["href"]
                    if MATCH in link and 'exam-70-' not in link:
                        link_dict[link] = None
            
            uid_dict = {}
            for filtered_link in link_dict:
                link_response = requests.get(filtered_link)
                link_tree = html.fromstring(link_response.content)
                meta = link_tree.xpath('//meta[@name="uid"]')
                if len(meta) > 0:
                    if 'content' in meta[0].attrib:
                        uid = meta[0].attrib["content"]
                        uid_dict[uid] = None

            for filtered_uid in uid_dict:
                if filtered_uid[:4] == 'exam':
                    exam_id = filtered_uid.replace('exam.','')
                    compexam_row = [competency['competency'], exam_id.upper()]
                    data_compexams.append(compexam_row)
                
                if filtered_uid[:13] == 'certification':
                    for exam in cert_dict[filtered_uid]:
                        compexam_row = [competency['competency'], exam.upper()]
                        data_compexams.append(compexam_row)

    # 4. Write Data to Blob Storage
    block_blob_service = BlockBlobService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)     
    write_to_blob(block_blob_service, data_comps, COLUMNS_COMPS, FILE_NAME_COMPS)
    write_to_blob(block_blob_service, data_compexams, COLUMNS_COMPEXAMS, FILE_NAME_COMPEXAMS)

    # 5. HTTP Response
    return func.HttpResponse(json.dumps(response),headers={'Content-Type':'application/json'})

def get_cert_dict():
    # Initialise Dictionary
    cert_dict = {}

    # Calculate number of pages
    number_of_items = get_cert_data(0)['count']
    number_of_pages = math.ceil(number_of_items/CERT_BATCH)

    # Get Certification Data
    for page in range(0,number_of_pages):
        skip = page * 30
        data = get_cert_data(skip)
        for result in data['results']:
            cert_id = result['uid']
            cert_dict[cert_id] = []
            for exam in result['exams']:
                exam_id = exam['display_name']
                cert_dict[cert_id].append(exam_id)
    
    # Return Dictionary
    return cert_dict

def get_cert_data(skip):
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
        '%$top':CERT_BATCH
    }
    response = requests.get(CERT_API, params=params)
    data = json.loads(response.content)
    return data

def write_to_blob(block_blob_service, data, columns, filename):
    today = datetime.today()
    folderpath = '{0}/{1}'.format(filename, today.strftime('%Y/%m/%d'))
    filename = '{0}/{1}.csv'.format(folderpath, filename)
    df = pd.DataFrame(data, columns=columns)
    csv = df.to_csv(index=False, encoding='utf-8')
    block_blob_service.create_blob_from_text(container_name=CONTAINER_NAME, blob_name=filename, text=csv, encoding='utf-8')
