from sickle import Sickle
import pandas as pd
from tqdm import tqdm

df = pd.read_excel('data/clarin_keywords_bibliotekanauki.xlsx')

sickle = Sickle('https://bibliotekanauki.pl/api/oai/articles')

output_list = []
for idx,row in tqdm(df.iterrows()):
    record_id = row['Unnamed: 0']
    oai_id = 'oai:bibliotekanauki.pl:' + record_id.replace('bibliotekanauki_', '')
    record = sickle.GetRecord(identifier=oai_id, metadataPrefix='oai_dc')
    lang = record.metadata.get('language', '')
    output_list.append((record_id,lang))

output_list = [(e[0], e[1][0]) for e in output_list]

out_df = pd.DataFrame(output_list)
out_df.to_excel('data/oai_langs.xlsx', index=False)
