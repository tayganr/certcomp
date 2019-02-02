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
FILE_NAME_EXAMS = 'exams'
FILE_NAME_PREP = 'preparation'
COLUMNS_EXAMS = ['EXAM_ID', 'EXAM', 'LINK', 'PUBLISHED']
COLUMNS_PREP = ['EXAM_ID', 'PREP_TYPE', 'PREP_TEXT', 'LINK']

# Microsoft Exams
URL = 'https://www.microsoft.com/en-us/learning/exam-list.aspx'

def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1. Init
    data_exam = []
    data_prep = []
    response = {}

    # 2. HTTP Request
    page = requests.get(URL)
    tree = html.fromstring(page.content)

    # 3. Get Data
    for exam in tree.xpath('//a[@class="mscom-link"]'):
        # EXAM
        link_text = exam.xpath('./text()')[0]
        exam_id = link_text.split(':')[0]
        exam_title = link_text.split(':')[1]
        exam_url = exam.attrib['href']

        published = None
        if 'release' in exam_title:
            published = exam_title.replace('(beta)','')
            published = published.split('(')[1].split(')')[0].replace('releases ','').replace('released ','')
            exam_title = exam_title.split(' (')[0]

        exam_title = exam_title.strip()

        logging.info('{0}: {1}'.format(exam_id, exam_title))
        
        # DETAIL
        try:
            detail = requests.get(exam_url, timeout=5)
            logging.info(detail.status_code)
            detail_tree = html.fromstring(detail.content)

            if published is None:
                published = extract(detail_tree.xpath('//*[@id="msl2ExamHero-details"]/ul/li[1]/text()'))

            # PREP ATTRIBUTES
            for prep in detail_tree.xpath('//div[@id="preparation-options"]/dl/dd'):
                prep_type = prep.attrib['id']

                # (community, elearning, onlinetraining, practice-test, selflearning, studyguide, training)
                for item in prep.xpath('./ul/li/a'):
                    prep_href = get_link(item)
                    prep_text = extract(item.xpath('./text()'))
                    prep_row = [exam_id, prep_type, prep_text, prep_href]
                    data_prep.append(prep_row)

                # (books)
                if prep_type == 'books':
                    for book_div in prep.xpath('./div'):
                        prep_text = extract(book_div.xpath('./div/p/strong[1]/text()'))
                        for book in book_div.xpath('./div/p/a'):
                            prep_href = get_link(book)
                            prep_row = [exam_id, prep_type, prep_text, prep_href]
                            data_prep.append(prep_row)
        except requests.exceptions.Timeout:
            logging.exception("Timeout occurred")

        # ROW - EXAM
        exam_row = [exam_id, exam_title, exam_url, published]
        response[exam_id] = {}
        response[exam_id]['title'] = exam_title
        response[exam_id]['url'] = exam_url
        response[exam_id]['published'] = published
        data_exam.append(exam_row)

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
