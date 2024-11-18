#%%
import json
import requests
from tqdm import tqdm

#%% load

with open('listaBibnauk_BibNar+libri.json', 'r', encoding='utf-8') as jotson:
    matching = json.load(jotson)

#%%

jats_url = 'https://bibliotekanauki.pl/api/oai/articles?verb=GetRecord&metadataPrefix=jats&identifier='
pdf_url = 'https://bibliotekanauki.pl/articles/'

for full_id in tqdm(matching):
    partial_id = full_id.split(':')[-1]
    metadata = requests.get(jats_url + full_id).text
    pdf_content = requests.get(pdf_url + partial_id + '.pdf').content
    
    with open(f'metadata/metadata_{partial_id}.xml', 'w', encoding='utf-8') as meta, open(f'pdfs/bibliotekanauki_{partial_id}.pdf', 'wb') as pdf:
        meta.write(metadata)
        pdf.write(pdf_content)