import requests
import os
import json
from tqdm import tqdm

crossref_endpoint = 'https://api.crossref.org/works/'

def get_meta(doi):
    url = crossref_endpoint + doi
    response = requests.get(url)
    if response.ok:
        response_json = response.json()
        return {
            'DOI' : response_json['message'].get('DOI'),
            'title' : response_json['message'].get('title'),
            'publisher' : response_json['message'].get('publisher'),
            'published' : response_json['message'].get('published'),
            'author' : response_json['message'].get('author'),
            'link' : response_json['message'].get('URL'),
            'issn' : response_json['message'].get('ISSN'),
        }
    else: return 'error'

with open('art_classic_meta.json', 'r', encoding='utf-8') as jfile:
    meta_dct = json.load(jfile)

new_meta_dct = {}
for k, v in tqdm(meta_dct.items()):
    if v == 'error':
        doi = k[10:-4]
        doi = doi.replace('_', '/')
        art_meta = get_meta(doi)
        new_meta_dct[k] = art_meta
    else:
        new_meta_dct[k] = v

with open('art_classic_meta.json', 'w', encoding='utf-8') as jfile:
    json.dump(new_meta_dct, jfile, indent=4, ensure_ascii=False)

