# -*- coding: utf-8 -*-
#%% import modules
import pandas as pd
import os
from ast import literal_eval
from tqdm import tqdm
import numpy as np
import regex as re
from langdetect import detect

#%%
# directory = 'czasopisma_metadata'

# dfs = []

# for filename in tqdm(os.listdir(directory)):
#     if filename.endswith('.xlsx'):
#         f = os.path.join(directory, filename)
#         temp_df = pd.read_excel(f)
#         dfs.append(temp_df)
        
# df = pd.concat(dfs)
# df = df.dropna(axis=1, how='all')
# df.to_excel('czasopisma_metadata.xlsx', index=False)

#%% load simplyfied data

df = pd.read_excel('czasopisma_metadata_full.xlsx')
df = df.fillna('')

#%%

def get_identifier(row):
    for header in row.index:
        if header in ('pdf_name', 'file_name'):
            if row[header]:
                if row[header].startswith('['):
                    row_ast = literal_eval(row[header])
                else: row_ast = row[header]
                
                if isinstance(row_ast, list):
                    return row_ast[0]
                else:
                    row_ast = row_ast.replace('.xml', '.pdf')
                    return row_ast

def get_title(row):
    for header in row.index:
        if header == 'title:pl-PL' and row[header]:
            if row[header].startswith('['):
                row_ast = literal_eval(row[header])
                return ' | '.join(row_ast)
            else: 
                return row[header]
        elif header == 'title' and row[header]:
            if row[header].startswith('['):
                row_ast = literal_eval(row[header])
                return ' | '.join(row_ast)
            else:
                return row[header]
        elif header == 'title:pl' and row[header]:
            if row[header] not in ("['Autobiografia']", "['Autobiografia Literatura Kultura Media']", "['Meluzyna']", "['Meluzyna : dawna literatura i kultura']", "['Rocznik Komparatystyczny']"):
                if row[header].startswith('['):
                    row_ast = literal_eval(row[header])
                    return ' | '.join(row_ast)
                else: 
                    return row[header]
            elif row[header].startswith('['):
                row_ast = literal_eval(row[header])
                if len(row_ast) > 1:
                    return ' | '.join(row_ast)
                else:
                    if row['source:pl']:
                        if row['source:pl'].startswith('['):
                            return literal_eval(row['source:pl'])[0]
            else:
                return row[header]

def get_authors(row):
    contributors = []
    for header in row.index:
        if header == 'creator' and row[header]:
            if row[header].startswith('['):
                row_ast = literal_eval(row[header])
                return ' | '.join(row_ast)
            else:
                return row[header]
        elif header == 'creator:pl' and row[header]:
            if row[header].startswith('['):
                row_ast = literal_eval(row[header])
                return ' | '.join(row_ast)
            else:
                return row[header]
        elif header.startswith('contributor_') and row[header]:
            contributors.append(row[header])
    if contributors:
        return ' | '.join(contributors)

def get_source(row):
    for header in row.index:
        if header in ('source:pl-PL', 'source:pl', 'source', 'relation') and row[header]:
            if row[header].startswith('['):
                row_ast = literal_eval(row[header])
                return ' | '.join(row_ast)
            else:
                return row[header]   
    
def get_date(row):
    for header in row.index:
        if header in ('date', 'year', 'date:pl'):
            if row[header]:
                if isinstance(row[header], float):
                    row_ast = str(int(row[header]))
                else:
                    if row[header].startswith('['):
                        row_ast = literal_eval(row[header])
                    else:
                        row_ast = row[header]
                if isinstance(row_ast, list):
                    return sorted(row_ast, key=lambda x: len(x))[0]
                else:
                    return row_ast

def get_number(row):
    for header in row.index:
        if header in ('number'):
            if row[header]:
                return row[header]
        elif header == 'identifier' and row['title:pl'] in ("['Autobiografia']", "['Autobiografia Literatura Kultura Media']", "['Meluzyna']", "['Meluzyna : dawna literatura i kultura']"):
            if row[header]:
               if row[header].startswith('['):
                   row_ast = literal_eval(row[header])
                   row_ast = [e for e in row_ast if e.startswith('10.')]
                   if row_ast:
                       return row_ast[0]
                
def get_pages(row):
    for header in row.index:
        if header in ('page'):
            if row[header]:
                return row[header]
            
cols = df.columns
new_df = pd.DataFrame()
for _,row in tqdm(df.iterrows()):
    identifier = get_identifier(row)
    resource_type = 'article'
    title = get_title(row)
    author = get_authors(row)
    author_gender = ''
    source = get_source(row)
    if not source:
        source = re.search('.+?(?= \d)', identifier).group(0) if re.search('.+?(?= \d)', identifier) else None
    source_num = get_number(row)
    source_place = None
    source_date = get_date(row)
    pub_date = None
    pub_place = None
    pages = get_pages(row)
    open_acces = False
    temp_df = pd.DataFrame([[identifier, resource_type, title, author, author_gender, source, source_num, source_place, source_date, pub_date, pub_place, pages, open_acces]], columns=['identifier', 'type', 'title', 'author', 'author_gender', 'source', 'source_number', 'source_place', 'source_date', 'publication_date', 'publication_place', 'pages', 'open_access'])
    new_df = pd.concat([new_df,temp_df], axis=0)

new_df = new_df.reset_index(drop=True)
new_df = new_df.fillna('')

#%%
new_df_dict = new_df.to_dict('records')

starts = ('Acta Universitatis Lodziensis', 'Białostockie Studia Literaturoznawcze', 'Collectanea Philologica', 'Colloquia Litteraria', 'Forum Poetyki', 'Góry, Literatura, Kultura', 'Jednak Książki', 'Literatura i Kultura Popularna', 'Litteraria Copernicana', 'Narracje o Zagładzie', 'Porównania', 'Postscriptum Polonistyczne', 'Poznańskie Studia Polonistyczne', 'Prace Filologiczne. Literaturoznawstwo', 'Prace Literackie', 'Papers in Literature', 'Przestrzenie Teorii', 'Stylistyka', 'Zagadnienia Rodzajów Literackich', 'Zoophilologica', 'Śląskie Studia Polonistyczne', 'Fabrica Litterarum Polono-Italica', 'Paidia i Literatura', 'Wortfolge', 'Dzieciństwo. Kultura i Literatura', 'Dzieciństwo. Literatura i Kultura')

for record in new_df_dict:
    if record['source'].startswith(starts):
        value = record['source'].split('; ')
        source = value[0]
        number = value[1]
        pages = value[-1]
        year = re.search('(?<=\()\d{4}(?=\))', number)
        if year:
            record['source_date'] = year.group(0)
        record['source'] = source 
        record['source_number'] = number
        record['pages'] = pages
 
        
starts = ('Czytanie Literatury. Łódzkie Studia Literaturoznawcze', 'Łódzkie Studia Literaturoznawcze')
for record in new_df_dict:
    if record['source'].startswith(starts):
        value = record['source'].split(';')
        source = value[0]
        number = value[-1]
        record['source'] = source 
        record['source_number'] = number

# wstawic roniczki komp
starts = ('Rocznik Komparatystyczny')
for record in new_df_dict:
    if record['identifier'].startswith(starts):
        source = 'Rocznik Komparatystyczny'
        record['source'] = source 


starts = ('Autobiografia')
for record in new_df_dict:
    if record['identifier'].startswith(starts):
        source = 'Autobiografia. Literatura - Kultura - Media'
        record['source'] = source 
        if record['source_number']:
            number = 'nr ' + record['source_number'].split('.')[-2] + ' (' + record['source_number'].split('.')[-1].split('-')[0]  + ')'
            record['source_number'] = number


starts = ('Meluzyna')
for record in new_df_dict:
    if record['identifier'].startswith(starts):
        source = 'Meluzyna. Dawna literatura i kultura'
        record['source'] = source 
        if record['source_number']:
            number = 'nr ' + record['source_number'].split('.')[-1].split('-')[0]
            year = record['source_number'].split('.')[-2]
            record['source_number'] = number
            record['source_date'] = year

            
# usunąć source
starts = ('Napis', 'Teksty Drugie', 'Pamiętnik Literacki')
for start in starts:
    for record in new_df_dict:
        if record['identifier'].startswith(start):
            source = start
            record['source'] = source
            
            number = [e for e in record['title'].split(' | ') if e.startswith(start)]
            if number:
                number = number[0]
                record['source_number'] = number
                year = re.search('(?<=\()\d{4}(?=\))', number)
                if year:
                    record['source_date'] = year.group(0)
                
            title = [e for e in record['title'].split(' | ') if not e.startswith(start)]
            if title:
                record['title'] = title[0]
               
                
starts = ('XIX wiek',)
for start in starts:
    for record in new_df_dict:
        if record['identifier'].startswith(start):
            new_start = 'Wiek XIX'
            source = new_start
            record['source'] = source
            
            number = [e for e in record['title'].split(' | ') if e.startswith(new_start + ',')]
            if number:
                number = number[0].replace(new_start + ', ', '')
                record['source_number'] = number
                year = re.search('(?<=\()\d{4}(?=\))', number)
                if year:
                    record['source_date'] = year.group(0)
                
            title = [e for e in record['title'].split(' | ') if not e.startswith(new_start + ',')]
            if title:
                record['title'] = title[0]             

for record in new_df_dict:
    splitted_authors = record['author'].split(' | ')
    author = [e for e in splitted_authors if not 'Tł.' in e]
    if author:
        record['author'] = ' | '.join(author)


for record in new_df_dict:
    splitted_title = record['title'].split(' | ')
    if len(splitted_title) > 1:
        title = [e for e in splitted_title if detect(e) == 'pl']
        if title:
            record['title'] = ' | '.join(title)

new_df_dict2 = []
for record in new_df_dict:
    if any([e.lower() == record['title'].lower() for e in ('Od Redakcji', 'Streszczenia', 'Od redaktora', 'Wprowadzenie', 'Wstęp', 'Noty o autorach', 'Sprostowania' 'Podziękowanie', 'Nadesłane', 'Ogłoszenie', 'Stopka redakcyjna', 'Słowo wstępne', 'Spis Treści', 'List do redakcji', 'Sprostowanie', 'Wiersze')]) or 'Contents of the fascicle' in record['title'] or record['title'].startswith(('Nekrolog:', 'Sprawozdanie z')):
        continue
    else:
        new_df_dict2.append(record)
    

temp_df = pd.DataFrame.from_dict(new_df_dict2)

#%% file rename

new_ids = {}
for idx, row in temp_df.iterrows():
    old_id = row['identifier']
    new_id = row['source'].replace(',.', '').replace(' ', '_').strip() + '_' + str(idx+1).zfill(8)
    new_ids[old_id] = new_id

for directory in os.listdir('czasopisma_pdfy'):
    journal_dir = os.path.join('czasopisma_pdfy', directory)
    for filename in tqdm(os.listdir(journal_dir)):  
        if filename not in new_ids:
            old_name = os.path.join(journal_dir, filename)
            os.remove(old_name)
        else:
            old_name = os.path.join(journal_dir, filename)
            new_name = os.path.join(journal_dir, new_ids[filename] + '.pdf')
            os.rename(old_name, new_name)

for record in new_df_dict2:
    record['identifier'] = new_ids[record['identifier']]

temp_df = pd.DataFrame.from_dict(new_df_dict2)

#%%

def check_lang(string):
    try:
        return detect(string)
    except:
        return ''

temp_df['lang'] = temp_df['title'].apply(check_lang)
temp_df['multi_title'] = temp_df['title'].apply(lambda x: ' | ' in x)

temp_df.to_excel('test.xlsx', index=False)


