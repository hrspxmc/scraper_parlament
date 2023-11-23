#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 22:11:15 2023

@author: hrspx
"""

#%%

import requests
from unidecode import unidecode
from bs4 import BeautifulSoup
from itertools import pairwise
from tqdm import tqdm
import time
import re
import scraping_functions as scf
from collections import namedtuple, Counter
import shelve
from joblib import Parallel, delayed
import logging


#%% 

project_root = '/home/hrspx/Documents/GitHub/scraper_parlament/'

base_site = 'https://www.sejm.gov.pl/Sejm9.nsf/'

process_list_adress = 'https://www.sejm.gov.pl/Sejm9.nsf/page.xsp/przeglad_projust'
process_page = 'https://www.sejm.gov.pl/Sejm9.nsf/PrzebiegProc.xsp?nr={}'

#%% głosowania

adress_voting = 'https://www.sejm.gov.pl/Sejm9.nsf/agent.xsp?symbol=posglos&NrKadencji=9'

webpage = requests.get(adress_voting)
soup = BeautifulSoup(webpage.content, "html.parser")
voting_table = soup.find('tbody')
all_votes_list = voting_table.find_all('tr')

votings_dict = [scf.assemble_voting_dict(ii.find_all('td')) for  ii in all_votes_list]

for ii,jj in pairwise(votings_dict):
    if jj['nr_pos_sejmu'] == ' ':
        jj['nr_pos_sejmu'] = ii['nr_pos_sejmu']
        
        
day_votings_pages = [ii['strona_z_głosami'] for ii in votings_dict]
 
all_votings_pages = []

for pages in tqdm(day_votings_pages):
    
    time.sleep(1)
    
    webpage = requests.get(base_site + pages)
    soup = BeautifulSoup(webpage.content, "html.parser")
    voting_time_table = soup.find_all('td', {'class': 'bold'})
    voting_time_table = [ii.a['href'] for ii in voting_time_table]
    all_votings_pages = [*all_votings_pages, *voting_time_table]


voting_id = namedtuple('voting_id', ['nr_kadencji', 'nr_posiedzenia',
                                     'nr_glosowania'])

votes_list = [voting_id(
    int(re.search('NrKadencji=(.+?)&', ii).group(1)),
    int(re.search('NrPosiedzenia=(.+?)&', ii).group(1)),
    int(re.search('NrGlosowania=(.+)', ii).group(1))) for ii in all_votings_pages]

#%% downloading params

single_batch_size = 5
n_jobs = 4
chunk_size = n_jobs*single_batch_size
parallel = Parallel(n_jobs=n_jobs)

#%%

with shelve.open(project_root+'data/votes_db') as db:
    remaining_keys = [ii for ii in votes_list if repr(ii) not in db.keys()]

while remaining_keys:

    chunks = [remaining_keys[x:x+chunk_size] for x in range(0, len(remaining_keys), chunk_size)]

    for ii in tqdm(chunks):
        try:
            output = parallel(delayed(scf.all_votings)(scf.vote_id_to_link(vote_id)) for vote_id in ii)
        
            with shelve.open(project_root+'data/votes_db') as db:
                for key, value in zip(ii, output):
                    db[repr(key)] = value
        except ConnectionError as e:
            print(e)
            pass
        
    with shelve.open(project_root+'data/votes_db') as db:
        remaining_keys = [ii for ii in votes_list if repr(ii) not in db.keys()]


#%%

with shelve.open(project_root+'data/votes_db') as db:
    print((db.keys()))

#%%

with shelve.open(project_root+'data/votes_db') as db:
      voting_results = dict(db)
      
      
#%%

party_aggregated_votes = {}

for key, value in voting_results.items():
    party_aggregated_votes[key] = {k:dict(Counter(v.values())) for k,v in value.items()}
