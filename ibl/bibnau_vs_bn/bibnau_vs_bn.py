#%% modules
import json
import ijson
from bs4 import BeautifulSoup
from tqdm import tqdm
import spacy
from collections import Counter
import pandas as pd
import statistics
import numpy as np
import os

#%% json load

with open('/data/bibnau_kwds_domains.json', 'r', encoding='utf-8') as jotson:
    bibnau_kwds_lists = json.load(jotson)

with open('/data/bibnau_domains.json', 'r', encoding='utf-8') as jotson:
    bibnau_domains_lists = json.load(jotson)

with open('/data/bibnau_kwds_for_subdomain.json', 'r', encoding='utf-8') as jotson:
    bibnau_kwds_for_subdomain = json.load(jotson)

with open('/data/bibnau_lemmatize_kwds_count_pl.json', 'r', encoding='utf-8') as jotson:
    bibnau_lemmatize_kwds_count_pl = json.load(jotson)

with open('/data/bn.json', 'r', encoding='utf-8') as jotson:
    bn = json.load(jotson)
    
with open('/data/bn_lemmatize_kwds_count.json', 'r', encoding='utf-8') as jotson:
    bn_lemmatize_kwds_count = json.load(jotson)
    
with open('/data/bibnau_domains_similarity.json', 'r', encoding='utf-8') as jotson:
    bibnau_domains_similarity = json.load(jotson)

bn_domains_mapping = pd.read_excel('/data/domains_mapping.xlsx')
bn_domains_mapping = {row['bibnau_domain']:row['bn_domain'].split('|') for index,row in bn_domains_mapping.iterrows()}

with open('/data/kwds_bibnau_full.json', 'r', encoding='utf-8') as jotson:
    kwds_bibnau_full = json.load(jotson)
    
with open('/data/kwds_bibnau_literary_pl.json', 'r', encoding='utf-8') as jotson:
    kwds_bibnau_literary_pl = json.load(jotson) 
    
#%% data import
bn_kwds_path = "/data/bib nauki vs miotacz/bn_kwds_with_cats.json"
bibnau_kwds_path = "/data/bib nauki vs miotacz/bib_nau_kwds.json"
bibnau_full_dump_path = "/data/BibNauk_dump_15-10-2022.json"

with open(bn_kwds_path, encoding='utf-8') as jotson:
    bn_kwds = json.load(jotson)

with open(bibnau_kwds_path, encoding='utf-8') as jotson:
    bibnau_kwds = json.load(jotson)

#%% bibnau full dump
bibnau_domains = {}
bibnau_kwds = {}

i = 0
with open(bibnau_full_dump_path, encoding='utf-8') as jotson:
   bibnau_full_dump = ijson.items(jotson, 'item')
   for obj in tqdm(bibnau_full_dump):
       if '<header status="deleted">' in obj:
           continue
       soup = BeautifulSoup(obj, 'xml')
       subjects = soup.find_all('subject')
       record_domains = {'main': set(), 'subdomains': set()}
       for sub in subjects:
           if sub.text[0].isupper():
               key = sub.text
               record_domains['main'].add(key)
           else:
               bibnau_domains.setdefault(key, set()).add(sub.text)
               record_domains['subdomains'].add(sub.text)
               
       kwd_groups = soup.find_all('kwd-group')
       for group in kwd_groups:
           lang = group.attrs.get('xml:lang')
           for kwd in group.find_all('kwd'):
               if kwd.text in bibnau_kwds:
                   bibnau_kwds[kwd.text]['lang'].add(lang)
                   bibnau_kwds[kwd.text]['maindomains'].update(record_domains['main'])
                   bibnau_kwds[kwd.text]['subdomains'].update(record_domains['subdomains'])
               else:
                   bibnau_kwds[kwd.text] = {'lang': {lang}, 
                                            'maindomains': record_domains['main'].copy(), 
                                            'subdomains': record_domains['subdomains'].copy()}
       if i == -4: break
       i += 1
       if len(bibnau_kwds['Muzeum Mleka w Grajewie']['maindomains']) > 1: break

bibnau_kwds_lists = {}
for key, value in tqdm(bibnau_kwds.items()):
    bibnau_kwds_lists[key] = {}
    for k,v in value.items():
        bibnau_kwds_lists[key][k] = list(v)

bibnau_domains_lists = {}
for key, value in tqdm(bibnau_domains.items()):
    bibnau_domains_lists[key] = list(value)

with open('/data/bibnau_kwds_domains.json', 'w', encoding='utf-8') as jotson:
    json.dump(bibnau_kwds_lists, jotson, indent=4, ensure_ascii=False)  

with open('/data/bibnau_domains.json', 'w', encoding='utf-8') as jotson:
    json.dump(bibnau_domains_lists, jotson, indent=4, ensure_ascii=False)    
    


#%% kwds for domains per lang

bibnau_kwds_for_subdomain = {}

for key, value in tqdm(bibnau_kwds_lists.items()):
    for subdomain in value['subdomains']:
        for lang in value['lang']:
            bibnau_kwds_for_subdomain.setdefault(subdomain, dict()).setdefault(lang, list()).append(key)
    
with open('/data/bibnau_kwds_for_subdomain.json', 'w', encoding='utf-8') as jotson:
    json.dump(bibnau_kwds_for_subdomain, jotson, indent=4, ensure_ascii=False)   

#%% lemmatize and count pl kwds
nlp = spacy.load("pl_core_news_lg")
bibnau_lemmatize_kwds_count_pl = {}

for key, value in bibnau_kwds_for_subdomain.items():
    print(key)
    if key in bibnau_lemmatize_kwds_count_pl: continue
    lemmatize_kwds = []
    if (pl_kwds := value.get('pl')):
        for kwd in tqdm(pl_kwds):
            lemmatize_kwds.extend([word.lemma_ for word in nlp(kwd) if len(word.lemma_) > 2])
    counted_lemmas = Counter(lemmatize_kwds)
    bibnau_lemmatize_kwds_count_pl[key] = counted_lemmas

output = {k:dict(v) for k,v in bibnau_lemmatize_kwds_count_pl.items()}

with open('/data/bibnau_lemmatize_kwds_count_pl.json', 'w', encoding='utf-8') as jotson:
    json.dump(bibnau_lemmatize_kwds_count_pl, jotson, indent=4, ensure_ascii=False) 

#%% prepare bn domains
bn_domains = set()
for key, value in tqdm(bn.items()):
    cat = value.get('subject_category')
    if cat:
        bn_domains.add(cat)
bn_domains = list(bn_domains)    
     
with open('/data/bn_categories.json', 'w', encoding='utf-8') as jotson:
    json.dump(bn_domains, jotson, indent=4, ensure_ascii=False)

#%% get bn domain for bibnau keywords
kwds_bibnau_full = {}
kwds_bibnau_literary = {}
kwds_bibnau_literary_pl = {}
for key, value in tqdm(bibnau_kwds_lists.items()):
    new_dict = {'bibnau_lang': value['lang'],
                'bibnau_main_domains': value['maindomains'],
                'bibnau_subdomains': value['subdomains'],
                'bn_domains_mapped': [elem for sublist in [bn_domains_mapping[domain] for domain in value['subdomains']] for elem in sublist],
                'kwd_lemma': ' '.join([word.lemma_ for word in nlp(key)])}
    if 'literaturoznawstwo' in new_dict['bn_domains_mapped']:
        kwds_bibnau_literary[key] = new_dict
        if 'pl' in new_dict['bibnau_lang']:
            kwds_bibnau_literary_pl[key] = new_dict
    kwds_bibnau_full[key] = new_dict

with open('/data/kwds_bibnau_full.json', 'w', encoding='utf-8') as jotson:
    json.dump(kwds_bibnau_full, jotson, indent=4, ensure_ascii=False)

with open('/data/kwds_bibnau_literary.json', 'w', encoding='utf-8') as jotson:
    json.dump(kwds_bibnau_literary, jotson, indent=4, ensure_ascii=False)
    
with open('/data/kwds_bibnau_literary_pl.json', 'w', encoding='utf-8') as jotson:
    json.dump(kwds_bibnau_literary_pl, jotson, indent=4, ensure_ascii=False)
    
#%% bn lemmas count 
bn_kwds_lemmas_for_domain = {}
for key, value in tqdm(bn.items()):
    domain = value.get('subject_category')
    lemmas = [e for e in value.get('150_lemma').split(' ') if len(e) > 2]
    bn_kwds_lemmas_for_domain.setdefault(domain, list()).extend(lemmas)
    
bn_lemmatize_kwds_count = {k:dict(Counter(v)) for k,v in bn_kwds_lemmas_for_domain.items()}
  
with open('/data/bn_lemmatize_kwds_count.json', 'w', encoding='utf-8') as jotson:
    json.dump(bn_lemmatize_kwds_count, jotson, indent=4, ensure_ascii=False)
    
#%% bibnau domains similarity
bibnau_domains_similarity = {}
for key, value in tqdm(kwds_bibnau_full.items()):
    domains = value.get('bibnau_subdomains')
    for dom in domains:
        bibnau_domains_similarity.setdefault(dom, list()).extend(domains)

bibnau_domains_similarity = {k:dict(Counter(v)) for k,v in bibnau_domains_similarity.items()}
bibnau_domains_similarity = {key: {k:round(v/bibnau_domains_similarity[key][key], 2) for k,v in value.items()} for key,value in bibnau_domains_similarity.items()}

with open('/data/bibnau_domains_similarity.json', 'w', encoding='utf-8') as jotson:
    json.dump(bibnau_domains_similarity, jotson, indent=4, ensure_ascii=False)

#%% bibnau kwds lit lemmas

for key, value in tqdm(kwds_bibnau_literary_pl.items()):
    value['kwd_lemma'] = ' '.join([word.lemma_ for word in nlp(key)])

with open('/data/kwds_bibnau_literary_pl.json', 'w', encoding='utf-8') as jotson:
    json.dump(kwds_bibnau_literary_pl, jotson, indent=4, ensure_ascii=False)

#%% spacy semantic similarity
bn_lit_kwds = []
for key, value in tqdm(bn.items()):
    if value.get('subject_category') == 'literaturoznawstwo':
       bn_lit_kwds.append(value['150_lemma'])
       
bn_lit_kwds_str = ' '.join(bn_lit_kwds)
nlp = spacy.load("pl_core_news_lg")
lit_nlp = nlp(bn_lit_kwds_str)

# kwd_token = nlp('dramaturg')

rows = []
for key, value in tqdm(kwds_bibnau_literary_pl.items()):
    similarities = []
    kwd_token = nlp(value['kwd_lemma'])
    for elem in bn_lit_kwds:
        elem_nlp = nlp(str(elem))
        similarities.append(elem_nlp.similarity(kwd_token))    
    similarity_2 = lit_nlp.similarity(kwd_token)
    rows.append([key, value['kwd_lemma'], max(similarities), statistics.mean(similarities), similarity_2])

out_df = pd.DataFrame(rows, columns=['bibnau_kwd', 'kwd_lemma', 'similarity_v1_max', 'similarity_v1_mean', 'similarity_v2'])
out_df.to_excel('bibnau_lit_semantic_similarity_sample.xlsx', index=False)

#%%
def sort_lemmas(lemmas_dict):
    lemmas_array = list(lemmas_dict.items())
    return sorted(lemmas_array, key= lambda x: x[1])[::-1]

bibnau_lemmas_sorted = {k: sort_lemmas(v) for k,v in bibnau_lemmatize_kwds_count_pl.items()}
bn_lemmas_sorted = {k: sort_lemmas(v) for k,v in bn_lemmatize_kwds_count.items()}

discipline = 'literaturoznawstwo'
top10_lit_bn = bn_lemmas_sorted[discipline][:10]

full_top10 = []

for elem in top10_lit_bn:
    kwd = elem[0]
    count = elem[1]
    
    other_disciplines_bn = {}
    for disc, lemmas in bn_lemmatize_kwds_count.items():
        if kwd in bn_lemmatize_kwds_count[disc] and disc != discipline:
            for idx, lemma in enumerate(bn_lemmas_sorted[disc]):
                if lemma[0] == kwd:
                    other_disciplines_bn[disc] = (lemma[0], lemma[1])
            
    other_disciplines_bibnau = {}
    for disc, lemmas in bibnau_lemmatize_kwds_count_pl.items():
        if kwd in bibnau_lemmatize_kwds_count_pl[disc] and disc != discipline:
            for idx, lemma in enumerate(bibnau_lemmas_sorted[disc]):
                if lemma[0] == kwd:
                    other_disciplines_bibnau[disc] = (lemma[0], lemma[1])
    
    other_disciplines_bn = sorted([(k, v[0], v[1]) for k,v in other_disciplines_bn.items()], key=lambda x: x[2])[::-1]
    other_disciplines_bibnau = sorted([(k, v[0], v[1]) for k,v in other_disciplines_bibnau.items()], key=lambda x: x[2])[::-1]
    
    full_top10.append((kwd, count, other_disciplines_bn, other_disciplines_bibnau))


# search for lemma in disciplines
kwd = 'kultura'

other_disciplines_bn = {}
for disc, lemmas in bn_lemmatize_kwds_count.items():
    if kwd in bn_lemmatize_kwds_count[disc]:
        for idx, lemma in enumerate(bn_lemmas_sorted[disc]):
            if lemma[0] == kwd:
                other_disciplines_bn[disc] = (idx, lemma[0], lemma[1])
        
other_disciplines_bibnau = {}
for disc, lemmas in bibnau_lemmatize_kwds_count_pl.items():
    if kwd in bibnau_lemmatize_kwds_count_pl[disc]:
        for idx, lemma in enumerate(bibnau_lemmas_sorted[disc]):
            if lemma[0] == kwd:
                other_disciplines_bibnau[disc] = (idx, lemma[0], lemma[1])


#%%
# all bibnau kwds = 975018; pl = 406684

nlp = spacy.load("pl_core_news_lg")

allWords = [
  orth
  for orth in nlp.vocab.vectors 
  if nlp.vocab[orth].has_vector 
  ]

def find_similar_vectors(vec, topn=10, tabu=list(), only_lowercase=True):

    def _cosine(v1, v2): return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))

    results = [
        w
        for w in tqdm(allWords)
        if nlp.vocab[w].text not in tabu
        and nlp.vocab[w].is_lower == only_lowercase
        and _cosine(nlp.vocab[w].vector, vec) > 0.5
    ]
    
    results.sort(key=lambda w: _cosine(nlp.vocab[w].vector, vec), reverse=True)

    return [nlp.vocab[r].text for r in results[:topn]]

# ['pisarz','poeta','przekład','literacki','nagroda','filologia','dramaturg','literatura','fikcyjny','krytyk','świat','fantastyczny','stworzenie','postać','poetycki']
bn_lit_start_terms = ['pisarz','poeta','przekład','literacki','filologia','dramaturg','literatura','poetycki', 'literaturoznawstwo', 'poetyk', 'wiersza'] # wszystkie ktore mialy ponad 1 i sa sensowne

terms_to_spacy_enrich = ['pisarz','poeta','literacki', 'filologia','dramaturg','literatura', 'poetycki', 'literaturoznawstwo', 'poetyk', 'wiersza']

terms_for_match = set(bn_lit_start_terms)

for term in terms_to_spacy_enrich:
    vec = nlp(term).vector
    sim_terms = find_similar_vectors(vec, topn=6)
    for t in sim_terms:
        terms_for_match.add(t)

matched_kwds = {}
for key, value in tqdm(kwds_bibnau_full.items()):
    if 'pl' in value['bibnau_lang']:
        lemma = set([word.lemma_ for word in nlp(key) if len(word.lemma_) > 2])
        if any([e in lemma for e in terms_for_match]):
            matched_kwds[key] = value

# zmaczowane 2350 rekordow

with open('/data/matched_kwds.json', 'w', encoding='utf-8') as jotson:
    json.dump(matched_kwds, jotson, indent=4, ensure_ascii=False)
    
    
#terms_for_match
# dramatopisarz
# literatura
# poetycko-muzyczny
# dramaturgów
# mitologia
# komediopisarz
# germanistyka
# literaturoznawstwo
# pisarz
# poeta
# literat
# liryk
# pisarski
# literaturoznawstwa
# literacko
# wiersza
# wierszy
# poetycko
# powieściopisarz
# językoznawstwo
# humanistyka
# kulturoznawstwo
# wierszyka
# literaturoznawcze
# lingwistyka
# bajkopisarz
# poematu
# antologia
# dramaturg
# literacki
# poetycka
# filologia
# autobiograficzny
# wiersz
# poet
# eseistyka
# beletrystyka
# poetycki
# historiografia
# filologie
# poetyk
# przekład
# wierszu
# filologiczna
# prozatorski
# dramaturga
# literaturoznawstwie