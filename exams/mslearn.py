import logging
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

MAPPING = [
    {
        "$filter": "((roles/any(t: t eq 'ai-engineer'))) and ((products/any(t: t eq 'azure')))",
        "exams": ["AI-100"]
    },
    {
        "$filter": "((roles/any(t: t eq 'administrator'))) and ((products/any(t: t eq 'azure')))",
        "exams": ["AZ-102", "AZ-103"]
    },
    {
        "$filter": "((roles/any(t: t eq 'solution-architect'))) and ((products/any(t: t eq 'azure')))",
        "exams": ["AZ-300","AZ-301","AZ-302"]
    },
    {
        "$filter": "((roles/any(t: t eq 'developer'))) and ((products/any(t: t eq 'azure')))",
        "exams": ["AZ-203"]
    },
    {
        "$filter": "((products/any(t: t eq 'azure')))",
        "terms": "devops",
        "exams": ["AZ-403"]
    },
    {
        "$filter": "((products/any(t: t eq 'azure')))",
        "terms": "security",
        "exams": ["AZ-500"]
    },
    {
        "$filter": "((products/any(t: t eq 'azure')))",
        "terms": "fundamentals",
        "exams": ["AZ-900"]
    },
    {
        "$filter": "((roles/any(t: t eq 'data-scientist'))) and ((products/any(t: t eq 'azure')))",
        "exams": ["DP-100"]
    },
    {
        "$filter": "((roles/any(t: t eq 'data-engineer'))) and ((products/any(t: t eq 'azure')))",
        "exams": ["DP-200","DP-201"]
    },
    {
        "$filter": "((products/any(t: t eq 'dynamics-customer-engagement')))",
        "exams": ["MB-200"]
    },
    {
        "$filter": "((products/any(t: t eq 'dynamics-sales')))",
        "exams": ["MB-210"]
    },
    {
        "$filter": "((products/any(t: t eq 'dynamics-marketing')))",
        "exams": ["MB-220"]
    },
    {
        "$filter": "((products/any(t: t eq 'dynamics-customer-service')))",
        "exams": ["MB-230"]
    },
    {
        "$filter": "((products/any(t: t eq 'dynamics-field-service')))",
        "exams": ["MB-240"]
    },
    {
        "$filter": "((products/any(t: t eq 'dynamics')))",
        "terms": "unified operations",
        "exams": ["MB-300"]
    },
    {
        "$filter": "((products/any(t: t eq 'dynamics-finance-operations')))",
        "exams": ["MB-310","MB-320","MB-330"]
    },
    {
        "$filter": "((products/any(t: t eq 'dynamics')))",
        "terms": "fundamentals",
        "exams": ["MB-900"]
    },
    {
        "$filter": "((products/any(t: t eq 'm365')))",
        "terms": "desktop",
        "exams": ["MD-100","MD-101"]
    },
    {
        "$filter": "((roles/any(t: t eq 'administrator'))) and ((products/any(t: t eq 'm365')))",
        "exams": ["MS-100","MS-101"]
    },
    {
        "$filter": "((products/any(t: t eq 'm365')))",
        "terms": "messaging",
        "exams": ["MS-200","MS-201","MS-202"]
    },
    {
        "$filter": "((products/any(t: t eq 'm365')))",
        "terms": "teamwork",
        "exams": ["MS-300","MS-301","MS-302"]
    },
    {
        "$filter": "((products/any(t: t eq 'm365')))",
        "terms": "security",
        "exams": ["MS-500"]
    },
    {
        "$filter": "((products/any(t: t eq 'm365')))",
        "terms": "fundamentals",
        "exams": ["MS-900"]
    }
]

def append_learn(table):
    # Iterate through each MS Learn request (MAPPING)
    for item in MAPPING:
        filters = item['$filter']
        terms = None
        if 'terms' in item:
            terms = item['terms']

        total_pages = get_ms_learn_page_count(filters, terms)
        skip = 0
        # Iterate each MS Learn page
        for x in range(0,total_pages):
            data = get_ms_learn_response(skip, filters, terms)
            for result in data['results']:
                title = result['title']
                resource_type = result['resource_type']
                url = 'https://docs.microsoft.com/en-us{0}'.format(result['url'])

                for exam in item['exams']:
                    table.append([exam, resource_type, title, url])
                
            skip += 30
    return table

def get_ms_learn_page_count(filters, terms):
    data = get_ms_learn_response(0, filters, terms)
    total_count = data['count']
    total_pages = math.ceil(total_count / 30)
    return total_pages

def get_ms_learn_response(skip, filters, terms):
    payload = {
        'environment': 'prod',
        'locale': 'en-us',
        '$orderBy': 'last_modified desc',
        '$skip': skip,
        '$filter': filters,
        '$top': 30
    }
    if terms:
        payload['terms'] = terms
    response = requests.get(LEARN_API, params=payload)
    data = json.loads(response.content)
    return data
