from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch
from requests.exceptions import HTTPError
from tqdm import tqdm
import json
import random
from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor
import urllib
import os
import pandas as pd

# prepare sample humanities data
search_domain = 'literature' #literature
journal_url = 'https://bibliotekanauki.pl/journals/'
oai_url = 'https://bibliotekanauki.pl/api/oai/articles'
sickle = Sickle(oai_url)

# get hum journals sets 
hum_sets = []
sets = [oai_set.setSpec for oai_set in sickle.ListSets()]
asession = AsyncHTMLSession()
async def get_sets():
    global hum_sets
    for set_spec in tqdm(sets):
        req_url = 'https://bibliotekanauki.pl/journals/' + set_spec
        response = await asession.get(req_url)
        await response.html.arender()
        domain_list = [domain.text.split('\n') for domain in response.html.find('div.scientificField')]
        domain_list = [item for sub in domain_list for item in sub]
        if search_domain in domain_list:
            hum_sets.append(set_spec) 
await get_sets() # await because IPython kernel is running own loop

with open('/data/bibnau_hum_sets.json', 'w', encoding='utf-8') as jfile:
    json.dump(hum_sets, jfile, indent=4, ensure_ascii=False)

# get identifiers from sets
with open('/data/bibnau_hum_sets.json', 'r', encoding='utf-8') as jfile:
    hum_sets = json.load(jfile)

records_identifiers = []
errors = []
for set_spec in tqdm(hum_sets):
    try:
        set_records = sickle.ListIdentifiers(metadataPrefix='jats', ignore_deleted=True, set=set_spec)
        for rec in set_records:
            records_identifiers.append(rec.identifier)
    except KeyboardInterrupt as err:
        raise err
    except NoRecordsMatch:
        continue
    except HTTPError:
        errors.append(set_spec)
    
with open('/data/bibnau_hum_identifiers.json', 'w', encoding='utf-8') as jfile:
    json.dump(records_identifiers, jfile, indent=4, ensure_ascii=False)


# get sample records
with open('/data/bibnau_hum_identifiers.json', 'r', encoding='utf-8') as jfile:
    records_identifiers = json.load(jfile)
    
sample_limit = 100
sample_records = {}
while len(sample_records) != sample_limit:
    while (rand_id := random.choice(records_identifiers)) in sample_records:
        continue
    url = f'https://bibliotekanauki.pl/api/oai/articles?verb=GetRecord&metadataPrefix=jats&identifier={rand_id}'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'xml')
    domains = [element.text for element in soup.find_all('subject')]
    citations = [element.text for element in soup.find_all('mixed-citation')]
    if search_domain in domains and citations:
        title = soup.find('article-title').text
        sample_records[rand_id] = {'title': title, 'citations': citations}
                        
with open('/data/sample_records.json', 'w', encoding='utf-8') as jfile:
    json.dump(sample_records, jfile, indent=4, ensure_ascii=False)

# ruby parser
os.system('ruby .\citations_parser.rb')

# get crossref response
with open('/data/anystyle_output.json', 'r', encoding='utf-8') as jfile:
    sample_records = json.load(jfile)

def get_crossref_match(reference, score_limit=120, mailto='nikodem.wolczuk@ibl.waw.pl'):
    headers = {
        'mailto': mailto
    }
    encoded_reference = urllib.parse.quote(reference)
    query = f'https://api.crossref.org/works?query.bibliographic={encoded_reference}&select=DOI,author,title,score,published'
    try:
        response = requests.get(query, headers=headers)
    except: return None
    if crossref_matches := response.json().get('message').get('items'):
        if crossref_matches[0]['score'] >= score_limit:
           return (reference, crossref_matches[0])
        else: return None

zrobione = []

for key, value in tqdm(sample_records.items()):
    if key not in zrobione:
        references_list = value['citations']
        with ThreadPoolExecutor() as executor:
            results = [e for e in list(executor.map(get_crossref_match, references_list)) if e]
        if results:
            value['crossref'] = results
    zrobione.append(key)


with open('/data/sample_records_plus_crossref.json', 'w', encoding='utf-8') as jfile:
    json.dump(sample_records, jfile, indent=4, ensure_ascii=False)
    
# get bn response
with open('/data/sample_records_plus_crossref.json', 'r', encoding='utf-8') as jfile:
    sample_records = json.load(jfile)

def get_bn_match(reference):
    
    bn_url = 'http://data.bn.org.pl/api/institutions/bibs.json?'
    
    key = reference[0]
    anystyle_resp = reference[1][0]
    
    if not (title := anystyle_resp.get('title', [None])[0]):
        return
    author = anystyle_resp.get('author', [{}])[0].get('family')
    year = anystyle_resp.get('date', [None])[0]
    for index, elem in enumerate((title, author, year)):
        if elem:
            match index:
                case 0:
                    bn_url += 'title=' + elem
                case 1:
                    bn_url += '&amp;author=' + elem
                case 2:
                    bn_url += '&amp;publicationYear=' + elem        
    else:
       bn_url += '&amp;limit=10'
       
    response = requests.get(bn_url)
    if (response_match := response.json().get('bibs')):
        response_match = response_match[0]
        return (key, response_match)
    else: 
        return

for key, value in tqdm(sample_records.items()):
    references_list = value['parsed_references']
    with ThreadPoolExecutor() as executor:
        results = [e for e in list(executor.map(get_bn_match, references_list)) if e]
    if results:
        value['bn_matches'] = results
        
with open('/data/sample_records_final.json', 'w', encoding='utf-8') as jfile:
    json.dump(sample_records, jfile, indent=4, ensure_ascii=False)
    
# count matches
with open('/data/sample_records_final.json', 'r', encoding='utf-8') as jfile:
    final_results = json.load(jfile)

output = {}
for key, value in final_results.items():
    temp = {}
    
    for elem in value['citations']:
        temp[elem] = {'bn': False, 'cr': False}
        
    if bn := value.get('bn_matches'):
        for elem in bn:
            if elem[0] in temp: 
                temp[elem[0]]['bn'] = True
                
    if cr := value.get('crossref'):
        for elem in cr:
            if elem[0] in temp: 
                temp[elem[0]]['cr'] = True
    output[key] = temp

total = 0
bn = 0
cr = 0
bn_and_cr = 0
no_match = 0
for value in output.values():
    for val in value.values():
        total += 1
        if val['bn'] and val['cr']:
            bn_and_cr += 1
        elif val['bn']:
            bn += 1
        elif val['cr']:
            cr += 1
        else:
            no_match += 1
print(f"\ntotal citations: {total}\nbn and crossref matches: {bn_and_cr}\nonly crossref matches: {cr}\nonly bn matches: {bn}\nno matches: {no_match}")  

# tables for manual check
to_df_dict = {}
for key, value in final_results.items():
    temp = {}
    
    for elem in value['citations']:
        temp[elem] = {'bn': '', 'cr': ''}
        
    if bn := value.get('bn_matches'):
        for elem in bn:
            if elem[0] in temp: 
                temp[elem[0]]['bn'] = elem[1]
                
    if cr := value.get('crossref'):
        for elem in cr:
            if elem[0] in temp: 
                temp[elem[0]]['cr'] = elem[1]
    to_df_dict[key] = temp


to_df = []
for key, value in to_df_dict.items():
    for k, v in value.items():
        if (cr := v.get('cr')):
            cr_doi = cr.get('DOI')
            cr_author = '; '.join([e.get('given', '') + ' ' + e.get('family', '') for e in cr.get('author', [])])
            cr_title = cr.get('title', [None])[0]
            cr_year = cr.get('published', {}).get('date-parts',[[None]])[0][0]
        else:
            cr_doi = None
            cr_author = None
            cr_title = None
            cr_year = None
        
        if (bn := v.get('bn')):
            bn_id = bn.get('id')
            bn_author = bn.get('author')
            bn_title = bn.get('title')
            bn_year = bn.get('publicationYear')
        else:
            bn_id = None
            bn_author = None
            bn_title = None
            bn_year = None
    
        row = [key, k, cr_doi, cr_author, cr_title, cr_year, bn_id, bn_author, bn_title, bn_year]
        to_df.append(row)

df = pd.DataFrame(to_df, columns=['bib_nau_id', 'reference', 'crossref_doi', 'crossref_author', 'crossref_title', 'crossref_year', 'bn_id', 'bn_author', 'bn_title', 'bn_year'])

df.to_excel('/data/bib_nau_references.xlsx', index=False)





       