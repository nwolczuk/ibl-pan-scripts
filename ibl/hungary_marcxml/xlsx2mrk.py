#%%
import sys
import pandas as pd
import regex as re
from tqdm import tqdm
import marcfuncs.marc_functions as mf
import unicodenormalizer.unicode_normalizer as un
import regex as re
import json

#%%
path = "/data/National_Library_of_Hungary_export_after_2000_20221024.xlsx"
hun_df = pd.read_excel(path)
output = []

for _, row in tqdm(hun_df.iterrows(), total=len(hun_df)):
    row = row[row.notnull()]
    record = {}
    for field in row.items():
        field_code = field[0] if field[0] != '000' else 'LDR'
        field_value = re.findall('(?<=^|;).+?(?=;..\$|$)', str(field[1]))
        record[field_code] = field_value
    output.append(un.normalize_data(record))

out_path = "/data/National_Library_of_Hungary.mrk"
mf.write_mrk(out_path, output)

#%%

hun_mrk = mf.read_mrk("/data/National_Library_of_Hungary.mrk")

temo = hun_mrk[0]

# w ilu recs 6xx all
counter_6xx = {}
for rec in tqdm(hun_mrk):
    for key in rec:
        if key.startswith('6'):
            if key in counter_6xx:
                counter_6xx[key] += 1
            else:
                counter_6xx[key] = 1

# w ilu srednio jest 6xx na rekord all
counter_6xx_per_rec = {}
for rec in tqdm(hun_mrk):
    for key, value in rec.items():
        if key.startswith('6'):
            if key in counter_6xx_per_rec:
                counter_6xx_per_rec[key] += len(value)
            else:
                counter_6xx_per_rec[key] = len(value)
                
# ile poszczegolnych 650, 655
counter_650_655 = {'650': {}, '655': {}}
for rec in tqdm(hun_mrk):
    for key, value in rec.items():
        if key in ('650', '655'):
            if len(value) in counter_650_655[key]:
                counter_650_655[key][len(value)] += 1
            else:
                counter_650_655[key][len(value)] = 1
suma = 0
for key, value in counter_650_655['650'].items():
    suma += value
counter_650_655['650'][0] = len(hun_mrk) - suma

suma = 0
for key, value in counter_650_655['655'].items():
    suma += value
counter_650_655['655'][0] = len(hun_mrk) - suma

# analiza jakociowa 655, 650
quality = {'650': {}, '655': {}}
for rec in tqdm(hun_mrk):
    for key, value in rec.items():
        if key in ('650', '655'):
            for field in value:
                desc = re.search('(?<=\$a).+?(?=\$|$)', field).group(0)
                if desc in quality[key]:
                    quality[key][desc] += 1
                else:
                    quality[key][desc] = 1

# rozkład typów materialu z LDR
ldr = {}
for rec in tqdm(hun_mrk):
    for key, value in rec.items():
        if key == 'LDR':
            code = value[0][7]
            if code in ldr:
                ldr[code] += 1
            else:
                ldr[code] = 1


for rec in tqdm(hun_mrk):
    for key, value in rec.items():
        if key == '655' and len(value) == 7:
            temp = rec
with open('/data/rekord_dla_patryczka_co_ma_duzo_gatunkow.json', 'w', encoding='utf-8') as f:
    json.dump(temp, f)

#%% PIERDOFIL

path = "/data/Petofi_Literary_Museum_books_articles_2000-2021.mrk"

petofi_mrk = mf.read_mrk(path)

# w ilu recs 6xx all
counter_6xx = {}
for rec in tqdm(petofi_mrk):
    for key in rec:
        if key.startswith('6'):
            if key in counter_6xx:
                counter_6xx[key] += 1
            else:
                counter_6xx[key] = 1

# w ilu srednio jest 6xx na rekord all
counter_6xx_per_rec = {}
for rec in tqdm(petofi_mrk):
    for key, value in rec.items():
        if key.startswith('6'):
            if key in counter_6xx_per_rec:
                counter_6xx_per_rec[key] += len(value)
            else:
                counter_6xx_per_rec[key] = len(value)
                
# ile poszczegolnych 650, 655
counter_650_655 = {'650': {}, '655': {}}
for rec in tqdm(petofi_mrk):
    for key, value in rec.items():
        if key in ('650', '655'):
            if len(value) in counter_650_655[key]:
                counter_650_655[key][len(value)] += 1
            else:
                counter_650_655[key][len(value)] = 1
suma = 0
for key, value in counter_650_655['650'].items():
    suma += value
counter_650_655['650'][0] = len(petofi_mrk) - suma

suma = 0
for key, value in counter_650_655['655'].items():
    suma += value
counter_650_655['655'][0] = len(petofi_mrk) - suma

# analiza jakociowa 655, 650
quality = {'650': {}, '655': {}}
for rec in tqdm(petofi_mrk):
    for key, value in rec.items():
        if key in ('650', '655'):
            for field in value:
                desc = re.search('(?<=\$a).+?(?=\$|$)', field).group(0)
                if desc in quality[key]:
                    quality[key][desc] += 1
                else:
                    quality[key][desc] = 1

# rozkład typów materialu z LDR
ldr = {}
for rec in tqdm(petofi_mrk):
    for key, value in rec.items():
        if key == '002':
            code = value[0]
            if code in ldr:
                ldr[code] += 1
            else:
                ldr[code] = 1

pola = set()
for rec in tqdm(petofi_mrk):
    for key, value in rec.items():
        pola.add(key)


for rec in tqdm(petofi_mrk):
    for key, value in rec.items():
        if key == '652':
            temp = rec
            break
