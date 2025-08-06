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

output_dct = {}
for file_name in tqdm(os.listdir('abstracts_hum')):
    doi = file_name[10:-4]
    doi = doi[0:7] + '/' + doi[8:]
    art_meta = get_meta(doi)
    output_dct[file_name] = art_meta

with open('art_classic_meta.json', 'w', encoding='utf-8') as jfile:
    json.dump(output_dct, jfile, indent=4, ensure_ascii=False)