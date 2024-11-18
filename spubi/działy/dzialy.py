import regex as re
import pandas as pd

#%%

txt1 = '/data/SPUB1.txt'
txt2 = '/data/SPUB2.txt'

with open(txt1, 'r', encoding='utf-8-sig') as txt:
    lines1 = txt.readlines()
    
with open(txt2, 'r', encoding='utf-8-sig') as txt:
    lines2 = txt.readlines()
    
#%% 
part1 = []
for line in lines1:
    line = line.strip()
    if not line:
        continue
    elif line[0].isdigit():
        part1.append([line])
    else:
        part1[-1].append(line)
        
part2 = []
for line in lines2:
    line = line.strip()
    if not line:
        continue
    elif line[0].isdigit():
        part2.append([line])
    else:
        part2[-1].append(line)

eng_names = len(part1) + len(part2)
descriptions = 0
for elem in (part1 + part2):
    if len(elem) > 1: descriptions += 1    
    
#%%

output = {}

for elem in part1:
    if len(elem) > 1: description = elem[1]
    else: description = ''
    elem = re.sub('\t+', ' ', elem[0])
    elem = re.sub('{0><}0{>|<0}', '', elem)
    number = elem.split(' ')[0]
    heading = re.sub(number, '', elem).strip()
    output[number] = {'heading': heading, 'description': description}
    
for elem in part2:
    if len(elem) > 1: description = elem[1]
    else: description = ''
    elem = re.sub('\t+', ' ', elem[0])
    elem = re.sub('{0><}0{>|<0}', '', elem)
    number = elem.split(' ')[0]
    heading = re.sub(number, '', elem).strip()
    output[number] = {'heading': heading, 'description': description}  

#%%

mapping_df = pd.read_excel('/data/Mapowanie działów.xlsx')[['nr działu', 'en_name', 'en_description']].fillna('')

for index, row in mapping_df.iterrows():
    number = row['nr działu']
    if number in output:
        if not row['en_name']:
            row['en_name'] = output[number]['heading']
        if not row['en_description']:
            row['en_description'] = output[number]['description']

mapping_df.to_excel('mapowanie_output.xlsx', index=False)

#%%

do_analizy = pd.read_excel('/data/mapowanie na LoC - dodatkowe deskryptory.xlsx', sheet_name='do analizy')
aw = pd.read_excel('/data/mapowanie na LoC - dodatkowe deskryptory.xlsx', sheet_name='AW')
bd = pd.read_excel('/data/mapowanie na LoC - dodatkowe deskryptory.xlsx', sheet_name='BL')
bl = pd.read_excel('/data/mapowanie na LoC - dodatkowe deskryptory.xlsx', sheet_name='BD')

pbl_headings = {}
for df in (aw, bd, bl):
    for index, row in df.iterrows():
        if not isinstance(row['link LCSH'], float):
            lcsh_id = re.search('sh.+?(?=\.|$)', row['link LCSH'].strip())
            if lcsh_id:
                pbl_headings[row['dzial']] = lcsh_id.group(0)

good_lcsh_ids = set()
for index, row in do_analizy.iterrows():
    if row['ile'] == 1:
        lcsh_id = re.search('sh.+?(?=\.|$)', row['link LCSH'].strip())
        if lcsh_id:
            good_lcsh_ids.add(lcsh_id.group(0))

output = {}
for key, value in pbl_headings.items():
    if value in good_lcsh_ids:
        heading_name = key.split(' -> ')[0]
        heading_name = heading_name.replace(' (tematy, motywy)', '')
        permalink = 'http://id.loc.gov/authorities/subjects/' + value
        if heading_name in output:
            output[heading_name].append(permalink)
        else:
            output[heading_name] = [permalink]

mapping_df = pd.read_excel('/data/Mapowanie działów.xlsx')[['nr działu', 'nazwa działu', 'loc_id']].fillna('')

for index, row in mapping_df.iterrows():
    name = row['nazwa działu']
    if name in output and len(output[name]) < 2:
        row['loc_id'] = output[name][0]

mapping_df.to_excel('/data/mapowanie_output_loc_ids.xlsx', index=False)
