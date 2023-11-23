#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 21:33:35 2023

@author: hrspx
"""

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

def acquire_row_dict(row_cells):
    return {'lp': get_project_id(row_cells),
            'proj_number': get_project_number(row_cells),
            'ue': get_project_for_eu(row_cells),
            'tytul': get_project_title(row_cells),
            'wspolny': get_project_combined(row_cells),
            'priorytet': get_project_priority(row_cells)}