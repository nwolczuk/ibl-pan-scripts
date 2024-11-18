#%% import modules
import pandas as pd
import string
from tqdm import tqdm
import json

import Levenshtein as lv

import spacy
nlp = spacy.load('pl_core_news_lg')

#%% load data

stl_v1_1_df = pd.read_excel('słownik terminów literackich v1.1.xlsx')


#%%

def simplify_str(input_string):
    if input_string:
        doc = nlp(input_string)
        input_string = ' '.join([token.lemma_ for token in doc]) 
        output_string = input_string.translate(str.maketrans('', '', string.punctuation)).replace(' ', '')
    return output_string.lower()


stl_v1_2_dict = {}
for _, row in tqdm(stl_v1_1_df.iterrows()):
    term = row['Termin'].lower()
    simply_term = simplify_str(term) if simplify_str(term) else term
    provenance = str(row['Źródło']) if not isinstance(row['Źródło'], float) else ''
    variant = str(row['Wariant']) if not isinstance(row['Wariant'], float) else ''
    lemma_1 = str(row['Lemat_1']) if not isinstance(row['Lemat_1'], float) else ''
    lemma_2 = str(row['Lemat_2']) if not isinstance(row['Lemat_2'], float) else ''
    version = str(round(row['Wersja'], 1))
    
    if simply_term in stl_v1_2_dict:
        stl_v1_2_dict[simply_term]['names'].append(term)
        stl_v1_2_dict[simply_term]['provenances'].add(provenance)
        stl_v1_2_dict[simply_term]['variants'].add(variant)
        stl_v1_2_dict[simply_term]['lemmas_1'].add(lemma_1)
        stl_v1_2_dict[simply_term]['lemmas_2'].add(lemma_2)
        stl_v1_2_dict[simply_term]['versions'].add(version)
    else:
        stl_v1_2_dict[simply_term] = {'names': [term], 'provenances': {provenance}, 'variants': {variant}, 'lemmas_1': {lemma_1}, 'lemmas_2': {lemma_2}, 'versions': {version}}

xlsx = pd.ExcelFile('Wydobyte terminy (do słownika).xlsx')
for sheet_name in tqdm(xlsx.sheet_names):
     temp_df = pd.read_excel(xlsx, sheet_name=f'{sheet_name}')
     for _, row in temp_df.iterrows():
         if simplify_str(str(row['Termin']).lower()) in stl_v1_2_dict:
             if not isinstance(row['Źródło'], float):
                 stl_v1_2_dict[simplify_str(str(row['Termin']).lower())]['provenances'].add(row['Źródło'])
xlsx.close()

#%% bn lit

with open('bn_lit.json', encoding='utf-8') as jotson:
    bn_lit = {k:v for k,v in json.load(jotson).items() if 'Literaturoznawstwo' in v['kategorie_tematyczne']}

with open('forma_gatunek_lit_wiki.json', encoding='utf-8') as jotson:
    bn_gat_wiki = json.load(jotson)

bn_wiki = {}
for k,v in bn_gat_wiki.items():
    if v:
        for elem in v['455'] + [k]:
            name = k.lower().replace(' (gatunek literacki)', '').replace(' (lit.)', ''.replace(' (literatura)', '')).replace(' (figura stylistyczna)', '')
            simply_name = simplify_str(name) if simplify_str(name) else name
            bn_wiki[simply_name] = {'bn': 'http://data.bn.org.pl/api/institutions/authorities.json?marc=009+' + str(v['009'][0]),
                               'wiki': v.get('wikidata', {}).get('item', '')}
    
for k,v in bn_lit.items():
    if v:
        for elem in v.get('450a', []) + [k]:
            name = k.lower().replace(' (gatunek literacki)', '').replace(' (lit.)', ''.replace(' (literatura)', '')).replace(' (figura stylistyczna)', '')
            simply_name = simplify_str(name) if simplify_str(name) else name
            bn_wiki[simply_name] = {'bn': 'http://data.bn.org.pl/api/institutions/authorities.json?marc=009+' + str(v['009a'][0]),
                               'wiki': v.get('item', '')}

matches = 0
for k,v in stl_v1_2_dict.items():
    if (ids := bn_wiki.get(k)):
        v['bn'] = ids['bn']
        v['wiki'] = ids['wiki']
        matches+=1
    else:
       v['bn'] = ''
       v['wiki'] = '' 
       

#%% prepare to export

to_df = [[
    v['names'][0],
    ' | '.join([e for e in v['variants'] if e]),
    ' | '.join([e for e in v['lemmas_1'] if e]),
    ' | '.join([e for e in v['lemmas_2'] if e]),
    ' | '.join([e for e in v['provenances'] if e]),
    ' | '.join([e for e in v['versions'] if e]),
    v['bn'],
    v['wiki']
    ]
         for k,v in stl_v1_2_dict.items()]

#%% lematyzacja

for row in tqdm(to_df):
    if not row[2]:
        doc = nlp(row[0])
        row[2] = ' '.join([token.lemma_ for token in doc])

#%% export

df = pd.DataFrame(to_df, columns=['Termin', 'Wariant', 'Lemat_1', 'Lemat_2', 'Źródło', 'Wersja', 'BN', 'Wiki'])

df.to_excel('słownik terminów literackich v1.3.xlsx')

#%% 

df = pd.read_excel('słownik terminów literackich v1.3.xlsx')
terms = set(df['Termin'])
all_groups = []
used = set()
group = []
for term in tqdm(terms):
    if term not in used:
        group.append(term)
        used.add(term)
        for match_term in terms:
            if match_term not in used:
                if lv.ratio(term, match_term) > .9:
                    group.append(match_term)
                    used.add(match_term)
        all_groups.append(group)
        group = []

all_groups.sort(key=lambda x: len(x), reverse=True)
out_list = []
for idx,elem in enumerate(all_groups):
    for e in elem:
        out_list.append((idx,e))
        
out_df = pd.DataFrame(out_list)
out_df.to_excel('słownik terminów literackich 1.3 - groups 09.xlsx', index=False)
