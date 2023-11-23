#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 21:33:35 2023

@author: hrspx
"""

from unidecode import unidecode
from bs4 import BeautifulSoup
import requests

base_site = 'https://www.sejm.gov.pl/Sejm9.nsf/'

def get_project_id(row_cells):
    return row_cells[0].text.strip().replace(".","")

def get_project_for_eu(row_cells):
    ue_image = row_cells[0].find("img", alt = 'Projekt zawierający przepisy wykonujące prawo UE')
    return True if ue_image else False

def get_project_title(row_cells):
    return unidecode(row_cells[1].text.strip().replace('Treść projektu', ''))

def get_project_combined(row_cells):
    return True if "rozpatr.wspólnie" in row_cells[3].text.strip() else False
    
def get_project_priority(row_cells):
    return True if "*" in row_cells[3].text.strip() else False

def get_project_number(row_cells):
    return [int(ii) for ii in row_cells[3].text.strip().split() if ii.isdigit()][0]

def acquire_project_row_dict(row_cells):
    return {'lp': get_project_id(row_cells),
            'proj_number': get_project_number(row_cells),
            'ue': get_project_for_eu(row_cells),
            'tytul': get_project_title(row_cells),
            'wspolny': get_project_combined(row_cells),
            'priorytet': get_project_priority(row_cells)}


# individual process functions

# votings

def assemble_voting_dict(votings_row_cells):
    return {'nr_pos_sejmu': unidecode(votings_row_cells[0].text),
            'data_pos_sejmu': unidecode(votings_row_cells[1].text),
            'strona_z_głosami': votings_row_cells[1].find('a')['href'],
            'liczba_głosowań': unidecode(votings_row_cells[2].text)}


def vote_id_to_link(vote_id):
    return 'agent.xsp?symbol=glosowania&NrKadencji={}&NrPosiedzenia={}&NrGlosowania={}'.format(
        vote_id.nr_kadencji,
        vote_id.nr_posiedzenia,
        vote_id.nr_glosowania)
       

def extract_personal_votes(club_voting_page_adress):
    webpage = requests.get(base_site + club_voting_page_adress)
    soup = BeautifulSoup(webpage.content, "html.parser")
    
    all_td = soup.find_all('td', {'class': 'left'})
    
    return dict(
        zip([ii.text for ii in all_td[0::2]],
            [ii.text for ii in all_td[1::2]])
        )

def all_votings(voting_page_adress):
    #time.sleep(0.25)
    webpage = requests.get(base_site + voting_page_adress)
    soup = BeautifulSoup(webpage.content, "html.parser")
    
    clubs_voting_pages = {ii.text:ii.find('a')['href'] for ii in soup.find_all('td', {'class': 'left'})}
    personal_votes_dict = {k:extract_personal_votes(v) for k,v in clubs_voting_pages.items()}
    return personal_votes_dict