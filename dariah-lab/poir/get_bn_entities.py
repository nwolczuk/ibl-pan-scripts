#%%
import json
from tqdm import tqdm
import pandas as pd
from pymarc import MARCReader
from ast import literal_eval
import regex as re

#%% load

tworcy_374 = pd.read_excel('tworcy/df_374.xlsx')
tworcy_374 = set([e.lower() for e in tworcy_374[tworcy_374['yes/no'] == 'yes']['value']])
tworcy_372 = pd.read_excel('tworcy/df_372.xlsx')
tworcy_372 = set([e.lower() for e in tworcy_372[tworcy_372['yes/no'] == 'yes']['value']])

badacze_374 = pd.read_excel('badacze/df_374.xlsx')
badacze_374 = set([e.lower() for e in badacze_374[badacze_374['yes/no'] == 'yes']['value']])
badacze_372 = pd.read_excel('badacze/df_372.xlsx')
badacze_372 = set([e.lower() for e in badacze_372[badacze_372['yes/no'] == 'yes']['value']])

tlumacze_374 = pd.read_excel('tlumacze/df_374.xlsx')
tlumacze_374 = set([e.lower() for e in tlumacze_374[tlumacze_374['yes/no'] == 'yes']['value']])
tlumacze_372 = pd.read_excel('tlumacze/df_372.xlsx')
tlumacze_372 = set([e.lower() for e in tlumacze_372[tlumacze_372['yes/no'] == 'yes']['value']])


#%%
tworcy = []
badacze = []
tlumacze = []
postacie_fikcyjne = []
literatura_do_spr = []

with open('authorities-all.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        if any(['osobowe' in val.subfields for val in record.get_fields('667')]) and record.leader[5] not in ['d', 'o']:
            fields_100 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('100')] for val in sublist if len(val)>1])
            fields_374 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('374')] for val in sublist if len(val)>1])
            fields_372 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('372')] for val in sublist if len(val)>1])
            fields_368 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('368')] for val in sublist if len(val)>1])
            fields_667 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('667')] for val in sublist if len(val)>1])

            # 368 Postacie fikcyjne i 100 (postać fikcyjna)
            if '(postać fikcyjna)' in fields_100 or 'postacie fikcyjne' in fields_374 | fields_368:
                postacie_fikcyjne.append(record)
                continue
            
            used = False
            
            # tworcy
            to_667_search = {'pisarz', 'pisarka', 'poeta', 'poetka', 'prozaik', 'prozaiczka', 'dramaturg', 'dramaturżka'}
            if any([e in tworcy_374 for e in fields_374]) or any([e in tworcy_372 for e in fields_372]) or any([e.lower() in ' '.join(fields_667).lower() for e in to_667_search]):
                tworcy.append(record)
                used = True
                
            # badacze
            to_667_search = {'literaturoznawca', 'literaturoznawczyni', 'polonista', 'polonistka', 'filolog', 'filolożka', 'historyk literatury', 'historyczka literatury', 'teoretyk literatury', 'teoretyczka literatury'}
            if any([e in badacze_374 for e in fields_374]) or any([e in badacze_372 for e in fields_372]) or any([e.lower() in ' '.join(fields_667).lower() for e in to_667_search]):
                badacze.append(record)
                used = True
                
            # tlumacze
            to_667_search = {'tłumacz'}
            if any([e in tlumacze_374 for e in fields_374]) or any([e in tlumacze_372 for e in fields_372]) or any([e.lower() in ' '.join(fields_667).lower() for e in to_667_search]):
                tlumacze.append(record)
                used = True
                
            if not used and 'literatura' in ' '.join(fields_374 | fields_372 | fields_667):
                literatura_do_spr.append(record)

#%% save 

with open('output/tworcy.marc', 'wb') as out:
    for rec in tworcy:
        out.write(rec.as_marc())       
with open('output/tworcy.json', 'w', encoding='utf-8') as out:
    json.dump([literal_eval(rec.as_json()) for rec in tworcy], out, indent=4, ensure_ascii=False)

with open('output/badacze.marc', 'wb') as out:
    for rec in badacze:
        out.write(rec.as_marc())    
with open('output/badacze.json', 'w', encoding='utf-8') as out:
    json.dump([literal_eval(rec.as_json()) for rec in badacze], out, indent=4, ensure_ascii=False)
    
with open('output/tlumacze.marc', 'wb') as out:
    for rec in tlumacze:
        out.write(rec.as_marc())    
with open('output/tlumacze.json', 'w', encoding='utf-8') as out:
    json.dump([literal_eval(rec.as_json()) for rec in tlumacze], out, indent=4, ensure_ascii=False)
    
with open('output/postacie_fikcyjne.marc', 'wb') as out:
    for rec in postacie_fikcyjne:
        out.write(rec.as_marc())    
with open('output/postacie_fikcyjne.json', 'w', encoding='utf-8') as out:
    json.dump([literal_eval(rec.as_json()) for rec in postacie_fikcyjne], out, indent=4, ensure_ascii=False)
    
with open('output/literatura_do_spr.marc', 'wb') as out:
    for rec in literatura_do_spr:
        out.write(rec.as_marc())    
with open('output/literatura_do_spr.json', 'w', encoding='utf-8') as out:
    json.dump([literal_eval(rec.as_json()) for rec in literatura_do_spr], out, indent=4, ensure_ascii=False)
    
#%%
korp_368 = set()
korp_372 = set()
grupy_lit = []

with open('authorities-all.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        if any(['korporatywne' in val.subfields for val in record.get_fields('667')]) and record.leader[5] not in ['d', 'o']:
            fields_368 = set([val for sublist in [e.subfields for e in record.get_fields('368')] for val in sublist if len(val)>1])
            fields_372 = set([val for sublist in [e.subfields for e in record.get_fields('372')] for val in sublist if len(val)>1])
            korp_368.update(fields_368)
            korp_372.update(fields_372)
        
        fields_550 = set([val for sublist in [e.subfields for e in record.get_fields('550')] for val in sublist if len(val)>1])
        
        if 'Grupy literackie' in fields_550:
            grupy_lit.append(record)


with open('out_all/grupy_literackie/grupy.marc', 'wb') as out:
    for rec in grupy_lit:
        out.write(rec.as_marc())       
with open('out_all/grupy_literackie/grupy.json', 'w', encoding='utf-8') as out:
    json.dump([literal_eval(rec.as_json()) for rec in grupy_lit], out, indent=4, ensure_ascii=False)


with open('out_all/instytucje_kultury/368_do_analizy.json', 'w', encoding='utf-8') as out:
    json.dump(list(korp_368), out, indent=4, ensure_ascii=False)
with open('out_all/instytucje_kultury/372_do_analizy.json', 'w', encoding='utf-8') as out:
    json.dump(list(korp_372), out, indent=4, ensure_ascii=False)

#%%

places = set()

with open('bn_books_2022-08-26.mrk', 'r', encoding='utf-8') as file:
    for line in tqdm(file.readlines()):
        if line.startswith('=651'):
            value = re.search('(?<=\$a).+?(?=\$|$)', line)
            if value:
                value = value.group(0)
            places.add(value)

real_places = []
fictional_places = []
for elem in places:
    if elem:
        if 'kraina fikcyjna' in elem.lower():
            fictional_places.append(elem)
        else:
            real_places.append(elem)


#%% bibs-czasopismo.marc
f_650 = set()
f_655 = set()
f_658 = set()

with open('bibs-czasopismo.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        fields_650 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('650')] for val in sublist if len(val)>1 and val != 'DBN'])
        fields_655 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('655')] for val in sublist if len(val)>1 and val != 'DBN'])
        fields_658 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('658')] for val in sublist if len(val)>1 and val != 'DBN'])
        f_650.update(fields_650)
        f_655.update(fields_655)
        f_658.update(fields_658)

with open('out_all/czasopisma/650_do_analizy.json', 'w', encoding='utf-8') as out:
    json.dump(list(f_650), out, indent=4, ensure_ascii=False)   
with open('out_all/czasopisma/655_do_analizy.json', 'w', encoding='utf-8') as out:
    json.dump(list(f_655), out, indent=4, ensure_ascii=False)  
with open('out_all/czasopisma/658_do_analizy.json', 'w', encoding='utf-8') as out:
    json.dump(list(f_658), out, indent=4, ensure_ascii=False)
    
#%%
output=[]
with open(r'out_all\byty osobowe\tworcy\tworcy.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        try:
            name = record.get_fields('100')[0].get_subfields('a')[0]
        except: continue
    
        date = record.get_fields('100')[0].get_subfields('d')
        if date:
            date = date[0]
        else: date = ''
    
        fields_372 = [elem.get_subfields('a') for elem in record.get_fields('372')]
        fields_372 = ' | '.join([item for sub in fields_372 for item in sub])
        
        fields_374 = [elem.get_subfields('a') for elem in record.get_fields('374')]
        fields_374 = ' | '.join([item for sub in fields_374 for item in sub])
        
        fields_667 = [elem.get_subfields('a') for elem in record.get_fields('667')]
        fields_667 = ' | '.join([item for sub in fields_667 for item in sub])
        
        output.append([name, date, fields_372, fields_374, fields_667])

# print(record)
df = pd.DataFrame(output, columns=['name', 'date', 'fields 372', 'fields 374', 'fields 667'])
df.to_excel(r'out_all\byty osobowe\tworcy\tworcy.xlsx', index=False)


#%%
output=[]
with open(r'out_all\byty osobowe\tlumacze\tlumacze.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        try:
            name = record.get_fields('100')[0].get_subfields('a')[0]
        except: continue
    
        date = record.get_fields('100')[0].get_subfields('d')
        if date:
            date = date[0]
        else: date = ''
    
        fields_372 = [elem.get_subfields('a') for elem in record.get_fields('372')]
        fields_372 = ' | '.join([item for sub in fields_372 for item in sub])
        
        fields_374 = [elem.get_subfields('a') for elem in record.get_fields('374')]
        fields_374 = ' | '.join([item for sub in fields_374 for item in sub])
        
        fields_667 = [elem.get_subfields('a') for elem in record.get_fields('667')]
        fields_667 = ' | '.join([item for sub in fields_667 for item in sub])
        
        output.append([name, date, fields_372, fields_374, fields_667])

# print(record)
df = pd.DataFrame(output, columns=['name', 'date', 'fields 372', 'fields 374', 'fields 667'])
df.to_excel(r'out_all\byty osobowe\tlumacze\tlumacze.xlsx', index=False)

#%%
output=[]
with open(r'out_all\byty osobowe\badacze\badacze.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        try:
            name = record.get_fields('100')[0].get_subfields('a')[0]
        except: continue
    
        date = record.get_fields('100')[0].get_subfields('d')
        if date:
            date = date[0]
        else: date = ''
    
        fields_372 = [elem.get_subfields('a') for elem in record.get_fields('372')]
        fields_372 = ' | '.join([item for sub in fields_372 for item in sub])
        
        fields_374 = [elem.get_subfields('a') for elem in record.get_fields('374')]
        fields_374 = ' | '.join([item for sub in fields_374 for item in sub])
        
        fields_667 = [elem.get_subfields('a') for elem in record.get_fields('667')]
        fields_667 = ' | '.join([item for sub in fields_667 for item in sub])
        
        output.append([name, date, fields_372, fields_374, fields_667])

# print(record)
df = pd.DataFrame(output, columns=['name', 'date', 'fields 372', 'fields 374', 'fields 667'])
df.to_excel(r'out_all\byty osobowe\badacze\badacze.xlsx', index=False)

#%%
output=[]
with open(r'out_all\byty osobowe\postacie_fikcyjne\postacie_fikcyjne.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        try:
            name = record.get_fields('100')[0].get_subfields('a')[0]
        except: continue
    
        field_100c = record.get_fields('100')[0].get_subfields('c')
        if field_100c:
            field_100c = field_100c[0]
        else: field_100c = ''
    
        fields_368 = [elem.get_subfields('c') for elem in record.get_fields('368')]
        fields_368 = ' | '.join([item for sub in fields_368 for item in sub])
        
        fields_374 = [elem.get_subfields('a') for elem in record.get_fields('374')]
        fields_374 = ' | '.join([item for sub in fields_374 for item in sub])
        
        fields_667 = [elem.get_subfields('a') for elem in record.get_fields('667')]
        fields_667 = ' | '.join([item for sub in fields_667 for item in sub])
        
        output.append([name, field_100c, fields_368, fields_374, fields_667])

    
# print(record)
df = pd.DataFrame(output, columns=['name', 'field_100c', 'fields 368', 'fields 374', 'fields 667'])
df.to_excel(r'out_all\byty osobowe\postacie_fikcyjne\postacie_fikcyjne.xlsx', index=False)

#%%
output=[]
with open(r'out_all\byty osobowe\literatura_do_spr.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        try:
            name = record.get_fields('100')[0].get_subfields('a')[0]
        except: continue
    
        date = record.get_fields('100')[0].get_subfields('d')
        if date:
            date = date[0]
        else: date = ''
    
        fields_372 = [elem.get_subfields('a') for elem in record.get_fields('372')]
        fields_372 = ' | '.join([item for sub in fields_372 for item in sub])
        
        fields_374 = [elem.get_subfields('a') for elem in record.get_fields('374')]
        fields_374 = ' | '.join([item for sub in fields_374 for item in sub])
        
        fields_667 = [elem.get_subfields('a') for elem in record.get_fields('667')]
        fields_667 = ' | '.join([item for sub in fields_667 for item in sub])
        
        output.append([name, date, fields_372, fields_374, fields_667])

# print(record)
df = pd.DataFrame(output, columns=['name', 'date', 'fields 372', 'fields 374', 'fields 667'])
df.to_excel(r'out_all\byty osobowe\literatura_do_spr.xlsx', index=False)

#%%
output=[]
with open(r'out_all\grupy_literackie\grupy.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        try:
            name = record.get_fields('110')[0].get_subfields('a')[0]
        except: continue
        output.append([name])
    
# print(record)
df = pd.DataFrame(output, columns=['name'])
df.to_excel(r'out_all\grupy_literackie\grupy.xlsx', index=False)

#%% czasopisma

czasop_658 = pd.read_excel('out_all/czasopisma/658_do_analizy.xlsx')
czasop_655 = pd.read_excel('out_all/czasopisma/655_do_analizy.xlsx')
czasop_650 = pd.read_excel('out_all/czasopisma/650_do_analizy.xlsx')

czasop_658 = set(czasop_658[czasop_658['czy_ok'] == 'tak']['desk'])
czasop_655 = set(czasop_655[czasop_655['czy_ok'] == 'tak']['desk'])
czasop_650 = set(czasop_650[czasop_650['czy_ok'] == 'tak']['desk'])

czasopisma = []
with open('bibs-czasopismo.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        fields_658 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('658')] for val in sublist if len(val)>1])
        fields_655 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('655')] for val in sublist if len(val)>1])
        fields_650 = set([val.lower() for sublist in [e.subfields for e in record.get_fields('650')] for val in sublist if len(val)>1])

        # tworcy
        if any([e in czasop_658 for e in fields_658]) or any([e in czasop_655 for e in fields_655]) or any([e in czasop_650 for e in fields_650]):
            czasopisma.append(record)


output = []
for record in tqdm(czasopisma):
    title = ' '.join(record['245'].get_subfields('a', 'b', 'p'))

    fields_658 = [elem.get_subfields('a') for elem in record.get_fields('658')]
    fields_658 = ' | '.join([item for sub in fields_658 for item in sub])
    
    fields_655 = [elem.get_subfields('a') for elem in record.get_fields('655')]
    fields_655 = ' | '.join([item for sub in fields_655 for item in sub])
    
    fields_650 = [elem.get_subfields('a') for elem in record.get_fields('650')]
    fields_650 = ' | '.join([item for sub in fields_650 for item in sub])
    
    output.append([title, fields_658, fields_655, fields_650])
    
df = pd.DataFrame(output, columns=['title', 'fields_658', 'fields_655', 'fields_650'])
df.to_excel(r'out_all\czasopisma\czasopisma.xlsx', index=False)

#%% instytucje kultury

with open('out_all/instytucje_kultury/368_do_analizy.json', encoding='utf-8') as jfile:
    field_368 = json.load(jfile)
    df_368 = pd.DataFrame(field_368)
    df_368.to_excel('out_all/instytucje_kultury/368_do_analizy.xlsx', index=False)

with open('out_all/instytucje_kultury/372_do_analizy.json', encoding='utf-8') as jfile:
    field_372 = json.load(jfile)
    df_372 = pd.DataFrame(field_372)
    df_372.to_excel('out_all/instytucje_kultury/372_do_analizy.xlsx', index=False)
    
inst_368 = pd.read_excel('out_all/instytucje_kultury/368_do_analizy.xlsx')
inst_368 = set(inst_368[inst_368['czy_ok'] == 'tak']['desk'])

instytucje = []
with open('authorities-all.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        if any(['korporatywne' in val.subfields for val in record.get_fields('667')]) and record.leader[5] not in ['d', 'o']:
            
            fields_368 = set([val for sublist in [e.subfields for e in record.get_fields('368')] for val in sublist if len(val)>1])
            
            if any([e in fields_368 for e in inst_368]):
                instytucje.append(record)
     
output = []
for record in tqdm(instytucje):
    try:
        name = record['110'].value()
    except:
        name = record['100'].value()

    fields_368 = [elem.get_subfields('a') for elem in record.get_fields('368')]
    fields_368 = ' | '.join([item for sub in fields_368 for item in sub])
    
    output.append([name, fields_368])

df = pd.DataFrame(output, columns=['name', 'fields_368'])
df.to_excel('out_all/instytucje_kultury/instytucje.xlsx', index=False)

#%% utwory

utwory = {}

with open('bn_books_2022-08-26.mrk', 'r', encoding='utf-8') as file:
    title = None
    for line in tqdm(file.readlines()):
        if line.startswith('=245'):
            title_a = re.search('(?<=\$a).+?(?=\$|$)', line)
            title_b = re.search('(?<=\$b).+?(?=\$|$)', line)
            if title_a and title_b:
                title = title_a.group(0) + ' ' + title_b.group(0)
            else:
                title = title_a.group(0)
        if line.startswith('=380') and '$aLiterature$' in line:
            utwory[title] = utwory.get(title, 0) + 1

output = [k for k,v in utwory.items() if v > 1]

df = pd.DataFrame(output)
df.to_excel('out_all/utwory/utwory.xlsx', index=False)


