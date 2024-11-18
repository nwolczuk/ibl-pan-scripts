#%% modules
import requests
from tqdm import tqdm
import time
import json
import pandas as pd

#%%
def get_bn_authorities(endpoint, kind='person', resp_limit=100, since_id=1):
    if '?' not in endpoint or endpoint.endswith('?'):
        endpoint = endpoint.replace('?', '')
        url = endpoint + '?deleted=false&kind={}&limit={}&sinceId={}'.format(kind, resp_limit, since_id)
    else:
        url = endpoint
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json() 
    else: return

def iterate_through_authorities(endpoint):
    while (resp := get_bn_authorities(endpoint)):
        time.sleep(.5)
        authorities, next_page = resp.get('authorities'), resp.get('nextPage')
        for authority in authorities:
            yield authority
        if next_page:
            endpoint = next_page
        else:
            print('Authority iteration ended sucessful.')
            return
    else:
        raise Exception('ERROR')

def get_unique_auth_values(*fields, endpoint=None):
    if not endpoint:
        raise Exception('Pass an endpoint!')
    if not fields:
        raise Exception('Pass at least one field')
    
    output_dict = {}
    for auth in tqdm(iterate_through_authorities(endpoint)):
        for field in auth.get('marc').get('fields'):
            for key, value in field.items():
                if key in fields:
                    if isinstance(value, str):
                        output_dict.set_default(key, set()).add(value)
                    else:
                        for subfield in value.get('subfields'):
                            for k,v in subfield.items():
                                output_dict.setdefault(key, dict()).setdefault(k, set()).add(v)
    return output_dict

#%%
bn_api_endpoint = 'http://data.bn.org.pl/api/institutions/authorities.json'

test = get_unique_auth_values('372', '374', endpoint=bn_api_endpoint)
test2 = {}
for key, value in test.items():
    for k,v in value.items():
        test2.setdefault(key, dict()).update({k: list(v)})

with open('372_374_values.json', 'w', encoding='utf-8') as jfile:
    json.dump(test2, jfile, indent=4, ensure_ascii=False)

#%%

with open('372_374_values.json', 'r', encoding='utf-8') as jfile:
    bn_data = json.load(jfile)
    
df_372 = pd.DataFrame(bn_data['372']['a'])
df_374 = pd.DataFrame(bn_data['374']['a'])

df_372.to_excel('df_372.xlsx', index=False)
df_374.to_excel('df_374.xlsx', index=False)