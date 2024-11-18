from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch 
import json
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.error import HTTPError

#%%
sets_path = "/data/bibliotekanauki_ssh_journals_sets.json"
with open(sets_path, 'r', encoding='utf-8') as f:
    ssh_sets = json.load(f)
    
sickle = Sickle(' https://bibliotekanauki.pl/api/oai/articles')

output = set()
errors = []

for s in tqdm(ssh_sets):
    set_spec = s.get('setSpec')
    if set_spec:
        try:
            recs = sickle.ListRecords(metadataPrefix="jats", set=set_spec, ignore_deleted=True)
            for rec in recs:
                soup = BeautifulSoup(rec.raw, 'xml')
                result = soup.find_all('kwd')
                for kwd in result:
                    output.add(kwd.text)
        except NoRecordsMatch:
            errors.append((set_spec, 'No records match'))
            continue
        except HTTPError:
            errors.append((set_spec, 'HTTP error'))
            continue
        except KeyboardInterrupt:
            break
        except:
           errors.append((set_spec, 'Other error'))
           continue 

with open('/data/bib_nau_kwds.json', 'w', encoding='utf-8') as f:
    json.dump(list(output), f, indent=4, ensure_ascii=False)
