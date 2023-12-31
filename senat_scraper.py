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
import shelve
import pandas as pd

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

voting_date_dict = {}

def process_personal_votings(voting_id):
    driver.get(voting_page_template.format(voting_id))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    voting_links_dict = {}

    for row in soup.find('table', {'class': 'voting-table'}).tbody.findAll('tr'):
        voting_links_dict[row.findAll('td')[0].get_text().strip()] = row.findAll('td')[1].find('a')['href']
        voting_date_dict[row.findAll('td')[0].get_text().strip()] = row.findAll('td')[1].get_text().strip()
        
    votings = {key: get_voting_dict(val) for key,val in voting_links_dict.items()}

    return votings

#%%

options = Options()
options.add_argument('--headless')
driver = webdriver.Firefox(options=options)

senat_page = 'https://www.senat.gov.pl'
voting_page_template = 'https://www.senat.gov.pl/sklad/senatorowie/aktywnosc-glosowania,{}.html'

project_root = "/home/hrspx/Documents/GitHub/scraper_parlament/"

#%%

kadencja = 10

with open("senat_strony/senatorowie_X.html") as html_page:
    soup = BeautifulSoup(html_page, 'html.parser')
    links = [ii.find('a')['href'] for ii in soup.find_all('div', {'class': 'senator-kontener'})]     
    
#%%
senator_id = [ii.split(',')[1] for ii in links]
res_dict = {}

for id in tqdm(senator_id, position=0, leave=True):
    res_dict[id] = process_personal_votings(id + ',' + str(kadencja))

#%%

klub = {}
senator = {}

for id, ii in tqdm(zip(senator_id, links), position=0, leave=True):
    time.sleep(1)
    driver.get(ii)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    senator[id] = soup.find('div', {'class': 'informacje'}).find('h2').get_text()
    klub[id] = soup.find('div', {'class': 'kluby'}).find('a').get_text()
    

#%%

# unpac dict

res_list = []

for key,val in res_dict.items():
    for ik, iv in val.items():
        tmp_df = pd.DataFrame.from_dict(iv, orient='index')
        tmp_df['senator'] = senator[key]
        tmp_df['nr_posiedzenia'] = ik
        tmp_df['klub'] = klub[key]
        tmp_df['kedencja'] = kadencja
        tmp_df['data'] = voting_date_dict[ik]
        res_list.append(tmp_df)

df = pd.concat(res_list)
df.index.name = 'nr_glosowania'
df.to_excel("test_senat.xlsx")
