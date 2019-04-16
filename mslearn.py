import math
import json
import requests
import csv

# Microsoft Learn
LEARN_API = 'https://docs.microsoft.com/api/contentbrowser/search'

# Exam Mapping
# ROLES = {
#     "functional-consultant": [],
#     "business-user": [],
#     "developer", c: [],
#     "solution-architect": [],
#     "administrator", c: [],
#     "data-engineer": [],
#     "ai-engineer": [],
#     "data-scientist": [],
#     "business-analyst": []
# }

def handler():
    # Init Table
    table = []
    headers = ('EXAM_ID', 'PREP_TYPE', 'PREP_TEXT', 'LINK')
    # headers = ('uid', 'title', 'resource_type', 'last_modified', 'duration_in_minutes', 'number_of_children', 'url')
    table.append(headers)

    # Iterate each MS Learn page
    total_pages = get_ms_learn_page_count()
    skip = 0
    for x in range(0,total_pages):
        print('Processing Page: {0} of {1}'.format((x+1), total_pages))
        data = get_ms_learn_response(skip)

        for result in data['results']:
            # uid = result['uid']
            products = ';'.join(result['products'])
            title = result['title']
            resource_type = result['resource_type']
            # last_modified = result['last_modified'].split(' ')[0]
            # lm_year = last_modified.split('/')[2]
            # lm_month = last_modified.split('/')[0]
            # lm_day = last_modified.split('/')[1]
            # last_modified = '{0}-{1}-{2}'.format(lm_year,lm_month,lm_day)
            # duration_in_minutes = result['duration_in_minutes']
            # number_of_children = result['number_of_children']
            url = 'https://docs.microsoft.com/en-us{0}'.format(result['url'])
            # row = (uid, title, resource_type, last_modified, duration_in_minutes, number_of_children, url)
            row = (products, resource_type, title, url)
            table.append(row)

        skip += 30
    
    write_table_to_csv(table)

# levels: ["intermediate"]
# products: ["dynamics", "dynamics-finance-operations"]
# roles: ["functional-consultant", "business-user"]

def write_table_to_csv(table):
    filename = 'learn.csv'
    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerows(table)

def get_ms_learn_page_count():
    data = get_ms_learn_response(0)
    total_count = data['count']
    total_pages = math.ceil(total_count / 30)
    return total_pages

def get_ms_learn_response(skip):
    payload = {
        'environment': 'prod',
        'locale': 'en-us',
        '$orderBy': 'last_modified desc',
        '$skip': skip,
        '$top': 30
    }
    response = requests.get(LEARN_API, params=payload)
    data = json.loads(response.content)
    return data

if __name__ == '__main__':
    handler()