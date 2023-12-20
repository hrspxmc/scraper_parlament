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
import datetime

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
    status_dict = defaultdict(None)
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

    p_str = 'Projekty ustaw zawarte w drukach'
    if combined_tag := soup.find(lambda tag: tag.name == "p" and p_str in tag.text):
        processed_with = [ii.get_text() for ii in combined_tag.find_all("a")]
    else:
        processed_with = None

    status_dict['proj_num'] = project_number
    status_dict['title'] = title
    status_dict['descreption'] = descreption
    status_dict['events_descreption'] = events_descreption
    status_dict['processed_with'] = processed_with
    
    # STATUS

    not_closed_step = val.find_all('p') if (val:=soup.find("li", {"class": "niezamknieta"})) else None
    
    if not_closed_step:
        status_dict['not_closed'] = True
        status_dict['not_closed_comment'] = not_closed_step[1].get_text()
        status_dict['not_closed_comment2'] = soup.find_all("p")[-1].get_text()
    
    final_step = val.find_all('p') if (val:=soup.find("li", {"class": "krok koniec"})) else None
    
    if final_step:
        status_dict['closed_status'] = final_step[1].get_text() if (len(final_step) >= 2) else None
        status_dict['dz_ust'] = final_step[2].get_text() if (len(final_step) >= 3) else None
    
    final_step_div = val.find_all('div') if (val:=soup.find("li", {"class": "krok koniec"})) else None
    
    if final_step_div:
        status_dict['div_desc'] = final_step_div[0].get_text() if (len(final_step) >= 1) else None

    return status_dict

# %%


proj_nums = [ii["proj_number"] for ii in project_rows_results]


res = {
    ix: acquire_project_process(ix) for ix in tqdm(proj_nums, position=0, leave=True)
}


#%%

proj_nums = [ii["proj_number"] for ii in project_rows_results]

single_batch_size = 5
n_jobs = 4
chunk_size = n_jobs * single_batch_size
parallel = Parallel(n_jobs=n_jobs)

with shelve.open(project_root + "data/process_db") as db:
    remaining_keys = [ii for ii in proj_nums if repr(ii) not in db.keys()]

while remaining_keys:
    chunks = [
        remaining_keys[x : x + chunk_size]
        for x in range(0, len(remaining_keys), chunk_size)
    ]
    
    for ii in tqdm(chunks):
        try:
            output = parallel(
                delayed(acquire_project_process)(proj_id) for proj_id in ii
            )

            with shelve.open(project_root + "data/process_db") as db:
                for key, value in zip(ii, output):
                    db[repr(key)] = value
        except ConnectionError as e:
            print(e)
            pass

    with shelve.open(project_root + "data/process_db") as db:
        remaining_keys = [ii for ii in proj_nums if repr(ii) not in db.keys()]

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
#9 2263
for key, val in res.items():
    
    if key in []:
        continue
    
    res_dict = defaultdict()
    res_dict["data_pisma"] = get_event_field(val, "Projekt wplynal do Sejmu")
    res_dict["data_skierowania_do_1_czytania"] = get_event_field(
        val, "Skierowano do I czytania"
    )
    
    if get_event_field(val, "Skierowano do I czytania", "event"):
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
        elif 'wniósł poprawkę' in text_val:
            res_dict['decyzja_senatu'] = 'Wniósł poprawkę'
        elif 'odrzucenie ustawy' in text_val:
            res_dict['decyzja_senatu'] = 'Wnosi o odrzucenie ustawy'
        else:
            res_dict['decyzja_senatu'] = None
    else:
        res_dict['decyzja_senatu'] = None
    
    res_dict["data_przekazania_prezydentowi"] = get_event_field(
        val, "przekazano Prezydentowi"
    )
    res_dict["data_podpisania_prezydenta"] = get_event_field(val, "Prezydent podpisal")

    res_dict['procedowany_z'] =  val['processed_with']

    if 'closed_status' in val and val['closed_status']:
        if 'Uchwalono' in val['closed_status']:
            res_dict['status'] =  'Uchwalono'
    if 'div_desc' in val and val['div_desc']:
        if 'wycofany' in val['div_desc']:
            res_dict['status'] =  'Wycofano'
    elif 'not_closed' in val and val['not_closed']:
        res_dict['status'] =  'Niezamknięty'

    if 'Poselski' in val['title']:
        res_dict['wnioskodawca'] = 'Poselski'
    elif 'Rządowy' in val['title']  or 'Rzadowy' in val['title']:
        res_dict['wnioskodawca'] = 'Rządowy'
    elif 'Obywatelski' in val['title']:
        res_dict['wnioskodawca'] = 'Obywatelski'
    elif 'Prezydenta' in  val['title']:
        res_dict['wnioskodawca'] = 'Prezydent'
    elif 'Senacki' in  val['title']:
        res_dict['wnioskodawca'] = 'Senacki'
    elif 'Komisyjny' in  val['title']:
        res_dict['wnioskodawca'] = 'Komisja'
    else:
        res_dict['wnioskodawca'] = None

    res_dict['UE'] = [ii['ue'] for ii in project_rows_results if ii['proj_number'] == key][0]
    res_dict['wspolny'] = [ii['wspolny'] for ii in project_rows_results if ii['proj_number'] == key][0]
    res_dict['priorytet'] = [ii['priorytet'] for ii in project_rows_results if ii['proj_number'] == key][0]

    res_dict1[key] = res_dict

#%% 

import pandas as pd

df = pd.DataFrame.from_dict(res_dict1, orient='index')
print(df)

#%%

months_dict = {
    'czerwca': '06',
     'grudnia': '12',
     'kwietnia': '04',
     'lipca': '07',
     'listopada': '11',
     'lutego': '02',
     'maja': '05',
     'marca': '03',
     'pazdziernika': '10',
     'sierpnia': '08',
     'stycznia': '01',
     'wrzesnia': '09'
    }



# %%
mutate_col = [col for col in df if col.startswith('data')]

#%%

def assemble_date(ii):
    if ii:
        return(pd.Timestamp(ii.split(" ")[2][:-1] + "-" + months_dict[ii.split(" ")[1]] + "-" + ii.split(" ")[0]))
    else:
        return None

for ii in mutate_col:
    df[ii] = df[ii].map(assemble_date)
    
#%%

df.to_excel("test.xlsx")