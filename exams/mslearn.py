import math
import json
import requests
import csv

# Microsoft Learn
LEARN_API = 'https://docs.microsoft.com/api/contentbrowser/search'

# Exam Mapping
ROLES_DICT = {
    "ai-engineer": ['AI-100'],
    "data-engineer": ['DP-200','DP-201'],
    "data-scientist": ['DP-100'],
    "developer": ['AZ-203'],
    "functional-consultant": ['MB-300','MB-310','MB-320','MB-330'],
    "solution-architect": ['AZ-300','AZ-301'],
    "admin-azure": ['AZ-103'],
    "admin-m365": ['MS-100','MS-101']
}

def append_learn(table):
    # Iterate each MS Learn page
    total_pages = get_ms_learn_page_count()
    skip = 0
    for x in range(0,total_pages):
        data = get_ms_learn_response(skip)

        for result in data['results']:
            roles = result['roles']
            title = result['title']
            resource_type = result['resource_type']
            products = result['products']
            url = 'https://docs.microsoft.com/en-us{0}'.format(result['url'])

            # Append rows where 1:1 mapping between ROLE and EXAM
            for role in ROLES_DICT:
                if role in roles:
                    for exam in ROLES_DICT[role]:
                        table.append([exam, resource_type, title, url])

            # Administrator role could be M365: Security Administrator (MS-500); Azure: Administrator (AZ-100;AZ-101)
            if 'administrator' in roles and 'azure' in products:
                for exam in ROLES_DICT['admin-azure']:
                    table.append([exam, resource_type, title, url])

            if 'administrator' in roles and 'm365' in products:
                for exam in ROLES_DICT['admin-m365']:
                    table.append([exam, resource_type, title, url])
            
        skip += 30
    return table

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
