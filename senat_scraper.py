#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 20:42:33 2023

@author: hrspx
"""

import requests
from unidecode import unidecode
from bs4 import BeautifulSoup
from itertools import pairwise
from tqdm import tqdm
import time
import re
import scraping_functions as scf
from collections import namedtuple, Counter, defaultdict
import shelve
from joblib import Parallel, delayed
import logging
import datetime	
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


#%%

def secure_get_text(row):
    if tmp:=row.findAll('td')[3].find('p'):
        return tmp.get_text().strip()
    return None

def get_voting_dict(voting_link):
    driver.get(senat_page+voting_link)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    voting_dict = {}

    for row in soup.find('table', {'class': 'tabela-aktywnosc-szczegoly-glosowania-dzien'}).tbody.findAll('tr'):
        vote_dict = {
            'vote': row.findAll('td')[2].get_text().strip(),
            'title': row.findAll('td')[3].find(string=True,recursive=False).get_text().strip(),
            'sub': secure_get_text(row)
            }
        
        voting_dict[row.findAll('td')[0].get_text().strip()] = vote_dict
        
    return voting_dict


def process_personal_votings(voting_id):
    driver.get(voting_page_template.format(voting_id))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    voting_links_dict = {}

    for row in soup.find('table', {'class': 'voting-table'}).tbody.findAll('tr'):
        voting_links_dict[row.findAll('td')[0].get_text().strip()] = row.findAll('td')[1].find('a')['href']
        
    votings = {key: get_voting_dict(val) for key,val in voting_links_dict.items()}

    return votings

#%%

senat_page = 'https://www.senat.gov.pl'
voting_page_template = 'https://www.senat.gov.pl/sklad/senatorowie/aktywnosc-glosowania,{}.html'

project_root = "/home/hrspx/Documents/GitHub/scraper_parlament/"

#%%

kadencja = 9

with open("senat_strony/senatorowie_IX.html") as html_page:
    soup = BeautifulSoup(html_page, 'html.parser')
    links = [ii.find('a')['href'] for ii in soup.find_all('div', {'class': 'senator-kontener'})]     
    
#%%

senator_id = [ii.split(',')[1] for ii in links]


process_personal_votings(senator_id[1]+ ',' + str(kadencja))

#%%

options = Options()
options.add_argument('--headless')
driver = webdriver.Firefox(options=options)

res_dict = {}

for id in tqdm(senator_id, position=0, leave=True):
    print(id)
    res_dict[id] = process_personal_votings(id + ',' + str(kadencja))


#%%



#%%

single_batch_size = 5
n_jobs = 4
chunk_size = n_jobs * single_batch_size
parallel = Parallel(n_jobs=n_jobs)

with shelve.open(project_root + 'data/senat_votes') as db:
    remaining_keys = [ii for ii in senator_id if repr(ii) not in db.keys()]

#%%

while remaining_keys:
    chunks = [
        remaining_keys[x : x + chunk_size]
        for x in range(0, len(remaining_keys), chunk_size)
    ]
    
    for ii in tqdm(chunks):
        try:
            output = parallel(
                delayed(process_personal_votings)(senator_id + ',' + str(kadencja)) for senator_id in ii
            )

            with shelve.open(project_root + "data/process_db") as db:
                for key, value in zip(ii, output):
                    db[repr(key)] = value
        except ConnectionError as e:
            print(e)
            pass

    with shelve.open(project_root + "data/senat_votes") as db:
        remaining_keys = [ii for ii in senator_id if repr(ii) not in db.keys()]


#%% debug

voting_id = senator_id[1]+ ',' + str(kadencja)

driver.get(voting_page_template.format(voting_id))
soup = BeautifulSoup(driver.page_source, "html.parser")
voting_links_dict = {}

for row in soup.find('table', {'class': 'voting-table'}).tbody.findAll('tr'):
    voting_links_dict[row.findAll('td')[0].get_text().strip()] = row.findAll('td')[1].find('a')['href']
        

#%%

voting_link = voting_links_dict['22']

driver.get(senat_page+voting_link)
soup = BeautifulSoup(driver.page_source, "html.parser")

voting_dict = {}

#%%

for row in soup.find('table', {'class': 'tabela-aktywnosc-szczegoly-glosowania-dzien'}).tbody.findAll('tr'):
    vote_dict = {
        'vote': row.findAll('td')[2].get_text().strip(),
        'title': row.findAll('td')[3].find(string=True,recursive=False).get_text().strip(),
        'sub': secure_get_text(row)
        }
        
    voting_dict[row.findAll('td')[0].get_text().strip()] = vote_dict
        




