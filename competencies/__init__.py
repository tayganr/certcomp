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
FILE_NAME_COMPS = 'competencies'
FILE_NAME_COMPEXAMS = 'comp_exams'
COLUMNS_COMPS = ['AREA', 'COMPETENCY', 'LINK']
COLUMNS_COMPEXAMS = ['COMPETENCY','OPTION','LEVEL','EXAM']

# Microsoft Competencies
URL = 'https://partner.microsoft.com/en-us/membership/competencies'
XPATH_COMPETENCY_AREAS = '//div[@class="panel-group hidden-md-x hidden-lg simple-tabs-accordion-sm"]/div'
XPATH_COMPETENCIES = './/div[@class="col-xs-12 col-sm-6 col-md-x-4 clickable-panel column-content-item"]'
# COMPETENCY_URL_PREFIX = 'https://partner.microsoft.com'
XPATH_COMPETENCY_OPTIONS = '//div[@class="complex-table-accordion-device"]//div[@class="panel panel-default"]'
XPATH_COMPETENCY_TIERS = './/div[@class="panel-body"]//div[@class="col-md-12"]'

def main(req: func.HttpRequest) -> func.HttpResponse:

    # 1. Init
    data_comps = []
    data_compexams = []
    response = {}
    response['competencies'] = {}

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
            page_comp = requests.get(competency['link'])
            tree_comp = html.fromstring(page_comp.content)

            for section in tree_comp.xpath(XPATH_COMPETENCY_OPTIONS):
                option = section.xpath('.//span[@class="accordion-heading-text"]/text()')[0]
                logging.info('Option: ' + option)
               
                toggle = {'Gold':'Silver','Silver':'Gold'}
                level = 'Gold'
                for segment in section.xpath(XPATH_COMPETENCY_TIERS):
                    level = toggle[level]
                    logging.info('-- ' + level)
                    for links in segment.xpath('.//a'):
                        data_compexams = populate_table(competency['competency'], option, level, links, data_compexams)

    # 4. Write Data to Blob Storage
    block_blob_service = BlockBlobService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)     
    write_to_blob(block_blob_service, data_comps, COLUMNS_COMPS, FILE_NAME_COMPS)
    write_to_blob(block_blob_service, data_compexams, COLUMNS_COMPEXAMS, FILE_NAME_COMPEXAMS)

    # 5. HTTP Response
    return func.HttpResponse(json.dumps(response),headers={'Content-Type':'application/json'})

def write_to_blob(block_blob_service, data, columns, filename):
    today = datetime.today()
    folderpath = '{0}/{1}'.format(filename, today.strftime('%Y/%m/%d'))
    filename = '{0}/{1}.csv'.format(folderpath, filename)
    df = pd.DataFrame(data, columns=columns)
    csv = df.to_csv(index=False, encoding='utf-8')
    block_blob_service.create_blob_from_text(container_name=CONTAINER_NAME, blob_name=filename, text=csv, encoding='utf-8')

def populate_table(competency_name, option, level, links, table):
    if 'href' in links.attrib:
        link = links.attrib["href"] 
        link_title = extract(links.xpath('./text()'))
        prefix = 'https://www.microsoft.com/en-us/learning/exam-'

        if link_title is None:
            link_title = extract(links.xpath('./span/text()'))

        if link_title is not None:
            if prefix in link or link_title.startswith('Exam'):
                exam_id = get_exam_id(link_title)
                row = (competency_name, option, level, exam_id)
                table.append(row)
            elif 'https://www.microsoft.com/en-us/learning/' in link:
                link_response = requests.get(link)
                link_tree = html.fromstring(link_response.content)

                for exam in link_tree.xpath('//*[@id="msl-certification-azure"]/div[1]/section/div/div/p[1]/a'):
                    exam_id = exam.xpath('./text()')[0].replace('Exam ','').replace('Transition ','')
                    row = (competency_name, option, level, exam_id)
                    table.append(row)

    return table

def extract(data):
    if data:
        return data[0].strip()
    else:
        return None

def get_link(href):
    if href is not None:
        return href.attrib['href']
    else:
        return None

def get_exam_id(text):
    exam_id = None

    if text[:5] == 'Exam ':
        exam_id = text.replace('Exam ', '')

    if exam_id is None:
        exam_id = text
    
    if ':' in exam_id:
        exam_id = exam_id.split(':')[0]

    if exam_id == '532':
        exam_id = '70-532'

    if len(exam_id) > 7:
        exam_id = exam_id.split(' ')[0]

    return exam_id
