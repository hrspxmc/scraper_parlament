# %%

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

# %%

kadencja = 8

project_root = "/home/hrspx/Documents/GitHub/scraper_parlament/"
base_site = "https://www.sejm.gov.pl/Sejm{}.nsf/".format(kadencja)
process_list_adress = base_site + "page.xsp/przeglad_projust"
process_page = base_site + "PrzebiegProc.xsp?nr={}"

# %% scrape projects descreption

webpage = requests.get(process_list_adress)
soup = BeautifulSoup(webpage.content, "html.parser")
projects_row_list = soup.find("tbody").findAll("tr")

project_rows_results = [
    scf.acquire_project_row_dict(ii.find_all("td")) for ii in projects_row_list
]

# %%


def safe_text_field_access(bs_find_res, str_to_replace=""):
    return (
        unidecode(val.getText(separator=u'; ').replace(str_to_replace, "") + ';')
        if (val := bs_find_res)
        else None
    )


def acquire_project_process(project_number):
    time.sleep(0.5)
    webpage = requests.get(process_page.format(project_number))
    soup = BeautifulSoup(webpage.content, "html.parser")
    str_to_replace = (
        "Ekspertyzy i opinie Biura Analiz SejmowychPosiedzenia komisji i podkomisji"
    )
    descreption = (
        safe_text_field_access(soup.find("div", {"class": "left"}), str_to_replace),
    )
    title = safe_text_field_access(soup.find("div", {"class": "h2"}))
    raw_events_list = soup.select('ul[class*="proces"]')[0].select('li[class="krok"]')
    events_descreption = [
        {
            "date": safe_text_field_access(ii.find("span")),
            "event": safe_text_field_access(ii.find("h3")),
            'descreption': safe_text_field_access(ii.find("div"))
        }
        for ii in raw_events_list
    ]

    return {
        "proj_num": project_number,
        "title": title,
        "descreption": descreption,
        "events_descreption": events_descreption,
    }


# %%


proj_nums = [ii["proj_number"] for ii in project_rows_results[-100:-1]]


res = {
    ix: acquire_project_process(ix) for ix in tqdm(proj_nums, position=0, leave=True)
}

# %%


def get_event_field(project_descreption, event_descreption, field="date"):
    if res := [
        ii[field]
        for ii in project_descreption["events_descreption"]
        if event_descreption in ii["event"]
    ]:
        return res[0]
    else:
        return None


# %%

res_dict1 = {}

for key, val in res.items():
    res_dict = defaultdict()
    res_dict["data_pisma"] = get_event_field(val, "Projekt wplynal do Sejmu")
    res_dict["data_skierowania_do_1_czytania"] = get_event_field(
        val, "Skierowano do I czytania"
    )
    res_dict["miejsce_1_czytania"] = (
        "Sejm"
        if "Sejmu" in get_event_field(val, "Skierowano do I czytania", "event")
        else "Komisje"
    )
    res_dict["data_3_czytania"] = get_event_field(val, "III czytanie")
    res_dict["data_decyzji_senatu"] = get_event_field(val, "Stanowisko Senatu")
    
    if text_val:=get_event_field(val, "Stanowisko Senatu", 'descreption'):
        if 'Nie wniosl poprawek;' in text_val:
            res_dict['decyzja_senatu'] = 'Nie wniosl poprawek'
        elif 'Wniosl poprawki' in text_val:
         res_dict['decyzja_senatu'] = 'Wniosl poprawki'
        else:
         res_dict['decyzja_senatu'] = None
    else:
        res_dict['decyzja_senatu'] = None
    
    res_dict["data_przekazania_prezydentowi"] = get_event_field(
        val, "przekazano Prezydentowi"
    )
    res_dict["data_podpisania_prezydenta"] = get_event_field(val, "Prezydent podpisal")

    res_dict1[key] = res_dict

# %%

webpage = requests.get(process_page.format(1013))
soup = BeautifulSoup(webpage.content, "html.parser")



final_step = soup.find("li", {"class": "krok koniec"}).find_all("p")

final_step[1].get_text()
final_step[2].get_text()

#%%



# %%
p_str = "Projekty ustaw zawarte w drukach"
if combined_tag := soup.find(lambda tag: tag.name == "p" and p_str in tag.text):
    list_res = [ii.get_text() for ii in combined_tag.find_all("a")]
else:
    list_res = None
