#%% import bibliotek

import requests
from unidecode import unidecode
from bs4 import BeautifulSoup
from itertools import pairwise
from tqdm import tqdm
import time
import re

#%% 

base_site = 'https://www.sejm.gov.pl/Sejm9.nsf/'
page_adress = 'https://www.sejm.gov.pl/Sejm9.nsf/page.xsp/przeglad_projust'
process_page = 'https://www.sejm.gov.pl/Sejm9.nsf/PrzebiegProc.xsp?nr={}'

webpage = requests.get(page_adress)
soup = BeautifulSoup(webpage.content, "html.parser")

#%% 
projects_row_list = soup.findAll('tbody')[0].findAll('tr')
row = projects_row_list[181].find_all('td')

#%%

def get_project_id(row_cells):
    return row[0].text.strip().replace(".","")

def get_project_for_eu(row_cells):
    ue_image = row[0].find("img", alt = 'Projekt zawierający przepisy wykonujące prawo UE')
    return True if ue_image else False

def get_project_title(row_cells):
    return unidecode(row[1].text.strip().replace('Treść projektu', ''))

def get_project_combined(row_cells):
    return True if "rozpatr.wspólnie" in row[3].text.strip() else False
    
def get_project_priority(row_cells):
    return True if "*" in row[3].text.strip() else False

def get_project_number(row_cells):
    return [int(ii) for ii in row[3].text.strip().split() if ii.isdigit()][0]

def acquire_row_dict(row_cells):
    return {'lp': get_project_id(row_cells),
            'proj_number': get_project_number(row_cells),
            'ue': get_project_for_eu(row_cells),
            'tytul': get_project_title(row_cells),
            'wspolny': get_project_combined(row_cells),
            'priorytet': get_project_priority(row_cells)}


#%%
project_rows_results = [acquire_row_dict(ii.find_all('td')) for ii in projects_row_list]
    
#%%
webpage = requests.get(process_page.format(210))

soup = BeautifulSoup(webpage.content, "html.parser")

str_to_replace = 'Ekspertyzy i opinie Biura Analiz SejmowychPosiedzenia komisji i podkomisji'

descreption = unidecode(soup.find('div', {"class": "left"}).text.replace(str_to_replace, ''))
title = unidecode(soup.find('div', {"class": "h2"}).text.replace(str_to_replace, ''))


#%%

process_list = soup.find('ul', {'class': 'proces zakonczony'}).find_all('li', {'class': 'krok'})

#%%

[ii.span.text for ii in process_list]

process_list[4].h3.text

process_list[4].find('a', {'class':'vote'})


#%% głosowania

adress_voting = 'https://www.sejm.gov.pl/Sejm9.nsf/agent.xsp?symbol=posglos&NrKadencji=9'

webpage = requests.get(adress_voting)
soup = BeautifulSoup(webpage.content, "html.parser")


# %%

def assemble_voting_dict(votings_row_cells):
    return {'nr_pos_sejmu': unidecode(votings_row_cells[0].text),
            'data_pos_sejmu': unidecode(votings_row_cells[1].text),
            'strona_z_głosami': votings_row_cells[1].find('a')['href'],
            'liczba_głosowań': unidecode(votings_row_cells[2].text)}

#%% 
voting_table = soup.find('tbody')

all_votes_list = voting_table.find_all('tr')


#%% 
votings_dict = [assemble_voting_dict(ii.find_all('td')) for  ii in all_votes_list]

for ii,jj in pairwise(votings_dict):
    if jj['nr_pos_sejmu'] == ' ':
        jj['nr_pos_sejmu'] = ii['nr_pos_sejmu']
        
        
day_votings_pages = [ii['strona_z_głosami'] for ii in votings_dict]

#%% 
all_votings_pages = []

for pages in tqdm(day_votings_pages):
    
    time.sleep(1)
    
    webpage = requests.get(base_site + pages)
    soup = BeautifulSoup(webpage.content, "html.parser")
    voting_time_table = soup.find_all('td', {'class': 'bold'})
    voting_time_table = [ii.a['href'] for ii in voting_time_table]
    all_votings_pages = [*all_votings_pages, *voting_time_table]




re.search('NrGlosowania=(.+)', 'agent.xsp?symbol=glosowania&NrKadencji=9&NrPosiedzenia=76&NrGlosowania=118').group(1)
re.search('NrKadencji=(.+?)&', 'agent.xsp?symbol=glosowania&NrKadencji=9&NrPosiedzenia=76&NrGlosowania=118').group(1)
re.search('NrPosiedzenia=(.+?)&', 'agent.xsp?symbol=glosowania&NrKadencji=9&NrPosiedzenia=76&NrGlosowania=118').group(1)

#%%  extract votes

webpage = requests.get(base_site + all_votings_pages[1])
soup = BeautifulSoup(webpage.content, "html.parser")

clubs_voting_pages = {ii.text:ii.find('a')['href'] for ii in soup.find_all('td', {'class': 'left'})}

#%%

def extract_personal_votes(voting_page_adress):
    webpage = requests.get(base_site + voting_page_adress)
    soup = BeautifulSoup(webpage.content, "html.parser")
    return dict(
        zip([ii.text for ii in soup.find_all('td', {'class': 'left'})[0::2]],
            [ii.text for ii in soup.find_all('td', {'class': 'left'})[1::2]])
        )


personal_votes_dict = {k:extract_personal_votes(v) for k,v in clubs_voting_pages.items()}
