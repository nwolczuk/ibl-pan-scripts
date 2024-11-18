import pandas as pd
import numpy as np
import json


ks_adnotations_df = pd.read_excel('pbl_ks_adnotacje_20-10-2024.xlsx').fillna('')

ks_adnotations_df['adnotations_full'] = ks_adnotations_df['ZA_ADNOTACJE'] + ' ' + ks_adnotations_df['ZA_ADNOTACJE2'] + ' ' + ks_adnotations_df['ZA_ADNOTACJE3']
ks_adnotations_df = ks_adnotations_df.sample(frac=1).reset_index(drop=True)
ks_adnotations_df['group'] = np.arange(len(ks_adnotations_df)) // 5

ks_adnotations_dct = {}
for idx, row in ks_adnotations_df.iterrows():
    ks_adnotations_dct.setdefault(100001 + row['group'], []).append((row['ZA_ZAPIS_ID'], row['adnotations_full'].strip('\n')))
    
with open('ks_adnotations_dct.json', 'w', encoding='utf-8') as jfile:
    json.dump(ks_adnotations_dct, jfile, indent=4, ensure_ascii=False)
    
for key, val in ks_adnotations_dct.items():
    with open(f'txt_files/KS/{key}.txt', 'a', encoding='utf-8') as txt:
        for tup in val:
            txt.writelines(tup[1] + '\n')
