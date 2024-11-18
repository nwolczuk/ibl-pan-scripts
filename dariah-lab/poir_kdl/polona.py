# -*- coding: utf-8 -*-
#%% import modules
import pandas as pd
import requests
from tqdm import tqdm
import json
import regex as re

#%% load persons list
df = pd.read_excel('Lista osób literaturoznawczych.xlsx')
df = df.fillna('')

polona_resp = []
for index, row in df.iterrows():
    name = row['Imię']
    surname = row['Nazwisko']
    epoka = row['Epoka']
    gender = row['Płeć']
    if (works := row['Ważne teksty']):
        for idx,work in enumerate(works.split(';')):
            identifier = 'polona' + str(index).zfill(8) + str(idx).zfill(2)
            work_name = work.split('(')[0].strip()
            work_year = work.split('(')[1].strip(' )') if len(work.split('(')) > 1 else ''
            response = ''
            polona_row = [identifier, name, surname, gender, epoka, work_name, work_year, response]
            polona_resp.append(polona_row)
    else:
        identifier = 'polona' + str(index).zfill(8) + '00'
        work_name = ''
        work_year = ''
        response = ''
        polona_row = [identifier, name, surname, gender, epoka, work_name, work_year, response]
        polona_resp.append(polona_row)
    
url = 'https://polona2.pl/api/entities/?query='
for elem in tqdm(polona_resp):
    if elem[5]:
        query = elem[1] + ' ' + elem[2] + ' ' + elem[5]
        resp = requests.get(url + query.strip())
        if resp.status_code == 200:
            elem[-1] = resp.json()['hits']
        else:
            elem[-1] = 'Response error'

out_with_resp = []
out_without_resp = []
for elem in polona_resp:
    if elem[-1]:
        out_with_resp.append(elem)
    else:
        out_without_resp.append(elem)
    
with open('polona_1_with_resp.json', 'w', encoding='utf-8') as jfile:
    json.dump(out_with_resp, jfile, indent=4, ensure_ascii=False)
    
with open('polona_1_without_resp.json', 'w', encoding='utf-8') as jfile:
    json.dump(out_without_resp, jfile, indent=4, ensure_ascii=False)
    
#%%
with open('polona_1_with_resp.json', 'r', encoding='utf-8') as jfile:
    out_with_resp = json.load(jfile)
    
with open('polona_1_without_resp.json', 'r', encoding='utf-8') as jfile:
    out_without_resp = json.load(jfile)
    
url = 'https://polona2.pl/api/entities/?query='
for elem in tqdm(out_without_resp):
    if elem[2] and elem[5]:
        title = elem[5].split(',')[0]
        query = elem[1] + ' ' + elem[2] + ' ' + title
        resp = requests.get(url + query.strip())
        if resp.status_code == 200:
            elem[-1] = resp.json()['hits']
        else:
            elem[-1] = 'Response error'

for elem in out_without_resp:
    if elem[-1]:
      out_with_resp.append(elem)  

out_without_resp = [e for e in out_without_resp if not e[-1]]

#%% elements with polona resp

to_df = []
for elem in tqdm(out_with_resp):
    if elem[-1] != 'Response error':
        for idx,e in enumerate(elem[-1]):
            author = e.get('creator_name')
            title =  e.get('title')
            year = e.get('date_descriptive')
            pub_place = e.get('publish_place')
            form = e.get('form_and_type')
            rights = e.get('rights')
            link=None
            for res in e.get('resources'):
                if res.get('mime') == 'application/xml':
                    r = requests.get(res.get('url'))
                    link = re.search('(?<=rdf\:about\=\").+?(?=\")', r.text)
                    if link: link = link.group(0)
                    else: link = None
            new_id = elem[0] + '_' + str(idx)
            new_item = [new_id] + elem[1:-1] + [author, title, year, form, rights, link]
            to_df.append(new_item)

df = pd.DataFrame(to_df)
df.to_excel('polona_match_output.xlsx')

#%%

def get_polona_reps(query):
    url = 'https://polona2.pl/api/entities/?query='
    output = []
    resp = requests.get(url + query.strip())
    if resp.status_code == 200:
        output.extend(resp.json()['hits'])
    
    hits_count = resp.json()['hits_count']
    resp_from = 150
    while hits_count > 150:
        url = 'https://polona2.pl/api/entities/?query='
        resp = requests.get(url + query.strip() + f'&from={resp_from}')
        if resp.status_code == 200:
            output.extend(resp.json()['hits'])
        hits_count -= 150
        resp_from += 150
        if resp_from > 600:
            return output
    if output:
        return output
    else:
        return None
        
authors_without_titles = [e for e in out_without_resp if e[-1] == '']

for elem in tqdm(authors_without_titles):
    if elem[2]:
        query = elem[1] + ' ' + elem[2] + '&size=150'
        resp = get_polona_reps(query.strip())
        elem[-1] = resp
      

authors_without_titles2 = [e for e in authors_without_titles if e[-1]]

with open('authors_without_titles.json', 'w', encoding='utf-8') as jfile:
    json.dump(authors_without_titles2, jfile, indent=4, ensure_ascii=False)


to_df = []
for elem in tqdm(authors_without_titles2):
    if elem[-1] != 'Response error':
        for idx,e in enumerate(elem[-1]):
            try:
                author = e.get('creator_name')
                title =  e.get('title')
                year = e.get('date_descriptive')
                pub_place = e.get('publish_place')
                form = e.get('form_and_type')
                rights = e.get('rights')
                link=None
                for res in e.get('resources'):
                    if res.get('mime') == 'application/xml':
                        r = requests.get(res.get('url'))
                        link = re.search('(?<=rdf\:about\=\").+?(?=\")', r.text)
                        if link: link = link.group(0)
                        else: link = None
                new_id = elem[0] + '_' + str(idx)
                new_item = [new_id] + elem[1:-1] + [author, title, year, form, rights, link]
                to_df.append(new_item)
            except KeyboardInterrupt as err: raise err
            except: continue

df = pd.DataFrame(to_df)
df.to_excel('authors_without_titles.xlsx')
