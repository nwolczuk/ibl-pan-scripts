#%%
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import regex as re
from tqdm import tqdm

#%% funcs

def parse_bn_marc(text_list):
    out_dict = {}
    key, value = None, None
    for line in text_list:
        if not line.startswith(' '):
            key = line[:3]
            value = line[7:].strip()
            out_dict.setdefault(key, list()).append(value)
        else:
            value = line.strip()
            out_dict[key][-1] = out_dict[key][-1] + value
    return out_dict


#%%

df = pd.read_csv('/data/bibnau_references_enriched.csv')
df = df[df['bn_permalink'].notna()]

all_references = len(df)
bn_matched = len(df[(df['bn_permalink'] != 'x') & (df['bn_permalink'] != '?')])
cr_matched = len(df[df['crossref_doi'].notna()])

# bn
bn_authors = {}
bn_publishers = {}
bn_subjects = {}

for link in tqdm(df[(df['bn_permalink'] != 'x') & (df['bn_permalink'] != '?')]['bn_permalink']):
    alma_id = link.split('/')[-1].replace('alma', '')
    rec_link = 'https://isoalma.bn.org.pl/cgi-bin/marc?mmsid=' + alma_id
    resp = requests.get(rec_link)
    record = parse_bn_marc(resp.text.split('\r\n'))
    
    if (auth := record.get('100')):
        for elem in auth:
            subf_a = re.search('(?<=_a).+?(?=_|$)', elem)
            subf_d = re.search('(?<=_d).+?(?=_|$)', elem)
            if not subf_a:
                continue
            elif subf_d:
                elem = subf_a.group(0) + ' ' + subf_d.group(0).strip(' :,.;')
            else:
                elem = subf_a.group(0)
            bn_authors[elem] = bn_authors.get(elem, 0) + 1
    
    if (pub260 := record.get('260')):
        for elem in pub260:
            subf_b = re.search('(?<=_b).+?(?=_|$)', elem)
            if not subf_b: continue
            elem = subf_b.group(0).strip(' :,.;')
            bn_publishers[elem] = bn_publishers.get(elem, 0) + 1
                
    if (pub264 := record.get('264')):           
        for elem in pub264:
            subf_b = re.search('(?<=_b).+?(?=_|$)', elem)
            if not subf_b: continue
            elem = subf_b.group(0).strip(' :,.;')
            bn_publishers[elem] = bn_publishers.get(elem, 0) + 1
    
    if (sub := record.get('650')):
        for elem in sub:
            subf_a = re.search('(?<=_a).+?(?=_|$)', elem)
            if not subf_a: continue
            elem = subf_a.group(0)
            bn_subjects[elem] = bn_subjects.get(elem, 0) + 1
  

# cr
cr_authors = {}
cr_publishers = {}
cr_subjects = {}

for doi in tqdm(df[df['crossref_doi'].notna()]['crossref_doi']):
    link = 'https://api.crossref.org/works/' + doi
    resp = requests.get(link)
    rec_json = resp.json()['message']
    
    if (auths := rec_json.get('author')):
        for aut in auths:
            full_name = aut.get('family') + ', ' + aut.get('given')
            cr_authors[full_name] = cr_authors.get(full_name, 0) + 1
            
    if (pub := rec_json.get('publisher')):
        cr_publishers[pub] = cr_authors.get(pub, 0) + 1
    
    if (subs := rec_json.get('subject')):
        for sub in subs:
            cr_subjects[sub] = cr_subjects.get(sub, 0) + 1

all_df = pd.DataFrame([('all references', all_references), ('bn matches', bn_matched), ('cr matches', cr_matched)])

bn_authors_df = pd.DataFrame(bn_authors.items(), columns=['autor', 'liczba wystąpień']).sort_values('liczba wystąpień', ascending=False)
bn_publishers_df = pd.DataFrame(bn_publishers.items(), columns=['wydawca', 'liczba wystąpień']).sort_values('liczba wystąpień', ascending=False)
bn_subjects_df = pd.DataFrame(bn_subjects.items(), columns=['temat', 'liczba wystąpień']).sort_values('liczba wystąpień', ascending=False)

cr_authors_df = pd.DataFrame(cr_authors.items(), columns=['autor', 'liczba wystąpień']).sort_values('liczba wystąpień', ascending=False)
cr_publishers_df = pd.DataFrame(cr_publishers.items(), columns=['wydawca', 'liczba wystąpień']).sort_values('liczba wystąpień', ascending=False)
cr_subjects_df = pd.DataFrame(cr_subjects.items(), columns=['temat', 'liczba wystąpień']).sort_values('liczba wystąpień', ascending=False)

with pd.ExcelWriter("/data/stats.xlsx") as writer:
    all_df.to_excel(writer, sheet_name="all", index=False)
    bn_authors_df.to_excel(writer, sheet_name="bn authors", index=False)
    bn_publishers_df.to_excel(writer, sheet_name="bn publishers", index=False)
    bn_subjects_df.to_excel(writer, sheet_name="bn subjects", index=False)
    cr_authors_df.to_excel(writer, sheet_name="cr authors", index=False)
    cr_publishers_df.to_excel(writer, sheet_name="cr publishers", index=False)
    cr_subjects_df.to_excel(writer, sheet_name="cr subjects", index=False)