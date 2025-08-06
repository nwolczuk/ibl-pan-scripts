import pandas as pd
import json


with open('art_classic_meta.json', 'r', encoding='utf-8') as jfile:
    meta_dct = json.load(jfile)

to_df = []
for k,v in meta_dct.items():
    doi = v.get('DOI')
    title = ' | '.join(v.get('title'))
    publisher = v.get('publisher')
    published = '-'.join([str(e) for e in v.get('published').get('date-parts')[0]])
    author = ' | '.join([e.get('given', '') + ' ' + e.get('family', '') for e in v.get('author')]) if v.get('author') else ''
    link = v.get('link')
    issn = ' | '.join(v.get('issn'))
    to_df.append((k, doi, title, publisher, published, author, link, issn))

df = pd.DataFrame(to_df, columns=['id', 'doi', 'title', 'publisher', 'published', 'author', 'link', 'issn'])

df.to_excel('abstracts_classic_meta.xlsx', index=False)