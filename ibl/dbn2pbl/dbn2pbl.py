import json
import pandas as pd
from tqdm import tqdm
import copy
import hashlib

#%% load dfs

dbn_655 = pd.read_excel('./data/dfs/mapowanie BN-Oracle - 655.xlsx')
dbn_655 = dbn_655[['X655', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2',	'dzial_PBL_3',	'haslo_przedmiotowe_PBL_1']].fillna('')
dbn_655 = dbn_655[dbn_655['decyzja'] == 'zmapowane']

dbn_650_1 = pd.read_excel('./data/dfs/mapowanie BN-Oracle - BEATAD.xlsx')
dbn_650_2 = pd.read_excel('./data/dfs/mapowanie BN-Oracle - GOSIA.xlsx')
dbn_650_3 = pd.read_excel('./data/dfs/mapowanie BN-Oracle - PAULINA.xlsx')

dbn_650 = pd.concat([dbn_650_1, dbn_650_2, dbn_650_3])
dbn_650 = dbn_650[['X650', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2',	'dzial_PBL_3']].fillna('')
dbn_650 = dbn_650[dbn_650['decyzja'] == 'zmapowane']

del dbn_650_1, dbn_650_2, dbn_650_3

#%%
old_pbl_dzialy = pd.read_excel('/data/old_pbl_dzialy.xlsx').fillna('')
old_pbl_dzialy_dct = {}
for index, row in old_pbl_dzialy.iterrows():
    key = row['SCIEZKI_DZIALOW']
    lower = (row['DZ_DZIAL_ID'], row['DZ_NAZWA'])
    chain = [lower,]
    for col_name in ['NAD_DZ_NAZWA','NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_NAD_NAD_DZ_NAZWA']:
        col_prefix = col_name.replace('DZ_NAZWA', '')
        if row[col_name]:
            chain.append((int(row[col_prefix + 'DZ_DZIAL_ID']), row[col_name]))
    old_pbl_dzialy_dct[key] = {'first': lower, 'chain': chain}

# old_pbl_hasla_przekrojowe = pd.read_excel('old_pbl_hasla_przekrojowe.xlsx').fillna('')
# old_pbl_hasla_przekrojowe_dct = {}
# for index,row in old_pbl_hasla_przekrojowe.iterrows():
#     key = row['HASŁA_PRZEKROJOWE']
#     chain = []
#     for col_name in ['KH_NAZWA', 'HP_NAZWA']:
#         if row[col_name]:
#             hp_nazwa = row[col_name]
#             if col_name == 'KH_NAZWA':
#                 hp_id = int(row['KH_KLUCZ_ID']) if row['KH_KLUCZ_ID'] else None
#             else:
#                 hp_id = int(row['HP_HASLO_ID']) if row['HP_HASLO_ID'] else None
#             chain.append((hp_id, hp_nazwa))
#     lower = chain[0]
#     old_pbl_hasla_przekrojowe_dct[key] = {'first': lower, 'chain': chain}
    

#%%

dbn_655_dct = {}
for idx, row in dbn_655.iterrows():
    key_value = row['X655']
    headings = [e for e in [row['dzial_PBL_1'],	row['dzial_PBL_2'],	row['dzial_PBL_3']] if e]
    # subjects = [e for e in [row['haslo_przedmiotowe_PBL_1'], row['haslo_przedmiotowe_PBL_2'], row['haslo_przedmiotowe_PBL_3']] if e]
    dct = dbn_655_dct.setdefault(key_value, dict())
    dct.setdefault('headings', set()).update(headings)
    # dct.setdefault('subjects', set()).update(subjects)
    
    
dbn_650_dct = {}
for idx, row in dbn_650.iterrows():
    key_value = row['X650']
    headings = [e for e in [row['dzial_PBL_1'],	row['dzial_PBL_2'],	row['dzial_PBL_3']] if e]
    # subjects = [e for e in [row['haslo_przedmiotowe_PBL_1'], row['haslo_przedmiotowe_PBL_2'], row['haslo_przedmiotowe_PBL_3']] if e]
    dct = dbn_650_dct.setdefault(key_value, dict())
    dct.setdefault('headings', set()).update(headings)
    # dct.setdefault('subjects', set()).update(subjects)


for key, val in  dbn_655_dct.items():
    for k,v in val.items():
        val[k]  = list(v)
    
for key, val in  dbn_650_dct.items():
    for k,v in val.items():
        val[k] = list(v)
    
#%% 
# for key, value in dbn_655_dct.items():
#     if key in dbn_650_dct:
#         dbn_650_dct[key]['headings'].extend(value['headings'])
#         dbn_650_dct[key]['headings'] = list(set(dbn_650_dct[key]['headings']))
#         dbn_650_dct[key]['subjects'].extend(value['subjects'])
#         dbn_650_dct[key]['subjects'] = list(set(dbn_650_dct[key]['subjects']))
#     else:
#         dbn_650_dct[key] = value
        
#%%

# for key,value in dbn_650_dct.items():
#     subjects = copy.deepcopy(value['subjects'])
#     for sub in value['subjects']:
#         sub_splitted = sub.lower().split(' - ')
#         for head in value['headings']:
#             if all([e in head.lower() for e in sub_splitted]):
#                 try:
#                     subjects.remove(sub)
#                 except ValueError:
#                     pass
#     value['subjects'] = subjects

#%% full
headings650_dct = {}
for key,value in dbn_650_dct.items():
    headings = []
    for k,v in value.items():
        match k:
            case 'headings':
                for elem in v:
                    path_str = elem
                    if elem := old_pbl_dzialy_dct.get(path_str):
                        elem['first_str'] = elem['first'][1]
                        elem['path_str'] = path_str
                        headings.append(elem)
            case 'subjects':
                for elem in v:
                    path_str = elem
                    if elem := old_pbl_hasla_przekrojowe_dct.get(path_str):
                        elem['first_str'] = elem['first'][1]
                        elem['path_str'] = path_str
                        headings.append(elem)
    headings650_dct[key] = headings
    
headings655_dct = {}
for key,value in dbn_655_dct.items():
    headings = []
    for k,v in value.items():
        match k:
            case 'headings':
                for elem in v:
                    path_str = elem
                    if elem := old_pbl_dzialy_dct.get(path_str):
                        elem['first_str'] = elem['first'][1]
                        elem['path_str'] = path_str
                        headings.append(elem)
            case 'subjects':
                for elem in v:
                    path_str = elem
                    if elem := old_pbl_hasla_przekrojowe_dct.get(path_str):
                        elem['first_str'] = elem['first'][1]
                        elem['path_str'] = path_str
                        headings.append(elem)
    headings655_dct[key] = headings

with open('/data/headings650.json', 'w', encoding='utf-8') as jfile:
    json.dump(headings650_dct, jfile, indent=4, ensure_ascii=False)

with open('/data/headings655.json', 'w', encoding='utf-8') as jfile:
    json.dump(headings655_dct, jfile, indent=4, ensure_ascii=False)




# SAVE FINAL DBN2PBL
with open('/data/dbn2pbl.json', 'w', encoding='utf-8') as jfile:
    json.dump(full_headings_dct, jfile, indent=4, ensure_ascii=False)
    
#%%
new_pbl_dzialy = pd.read_excel('/data/updated_headings.xlsx').fillna('')
new_pbl_dzialy_by_nr = {}
for index, row in new_pbl_dzialy.iterrows():
    new_pbl_dzialy_by_nr[row['nr działu']] = row['nazwa działu']

def get_chain(number):
    numbers_chain = []
    for subnum in number.split('.')[:-1]:
        if not numbers_chain:
            numbers_chain.insert(0, subnum + '.')
        else:
            numbers_chain.insert(0, numbers_chain[0] + subnum + '.')
    output_chain = []
    for elem in numbers_chain:
        output_chain.append((elem, new_pbl_dzialy_by_nr[elem]))
    return output_chain
    
# new_pbl_dzialy_dct = {}
# for index, row in new_pbl_dzialy.iterrows():
#     key = row['nazwa działu'].lower()
#     value = {
#         'nr_dzialu': row['nr działu'],
#         'nazwa_dzialu': row['nazwa działu'],
#         'simplified_str': row['simplified_str'],
#         'hash': row['new_md5'],
#         'chain': get_chain(row['nr działu']),        
#         }
#     new_pbl_dzialy_dct.setdefault(key, []).append(value)

new_pbl_dzialy_dct = {}
for index, row in new_pbl_dzialy.iterrows():
    key = row['nr działu']
    value = {
        'nr_dzialu': row['nr działu'],
        'nazwa_dzialu': row['nazwa działu'],
        'simplified_str': row['simplified_str'],
        'hash': row['new_md5'],
        'chain': get_chain(row['nr działu']),        
        }
    new_pbl_dzialy_dct[key] = value

# SAVE FINAL NEW_PBL_HEADINGS
with open('/data/new_pbl_headings.json', 'w', encoding='utf-8') as jfile:
    json.dump(new_pbl_dzialy_dct, jfile, indent=4, ensure_ascii=False)    

