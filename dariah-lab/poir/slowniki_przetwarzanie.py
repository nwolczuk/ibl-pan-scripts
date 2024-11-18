#%% modules
import regex as re
import pandas as pd
from striprtf.striprtf import rtf_to_text

#%% slownik_rodzajow_i_gatunkow_literackich

terms = []
with open('out_all/terminy/slownik_rodzajow_i_gatunkow_literackich.txt', 'r', encoding='utf-8') as txt:
    for line in txt.readlines():
        line = line.replace('\n', '')
        if len(line)>1:
            splitted = line.split('zob.')
            
            if len(splitted)>1:
                term, zob = splitted
            else:
                term = splitted[0]
                zob = ''
            
            term = re.sub('\d+', '', term).strip(' ,.')
            zob = re.sub('^ też ', '', zob).strip(' ,.')
            
            terms.append((term, zob, 'Słownik rodzajów i gatunków literackich. Grzegorz Gazda'))
            
df = pd.DataFrame(terms, columns=['termin', 'zob.', 'źródło'])
df.to_csv('out_all/terminy/slownik_rodzajow_i_gatunkow_literackich.csv', index=False)

#%% slownik_terminow_teatralnych

terms = []
with open('out_all/terminy/slownik_terminow_teatralnych.txt', 'r', encoding='utf-8') as txt:
    category = ''
    for line in txt.readlines():
        line = line.replace('\n', '')      
        if len(line)>1:
            
            if line.startswith('#'):
                category = line[1:]
                continue
            
            splitted = line.split('zob.')
            
            if len(splitted)>1:
                term, zob = splitted
            else:
                term = splitted[0]
                zob = ''
                
            term = re.sub('\[\d+\]', '', term).strip()
            zob = zob.strip()
            
            terms.append((term, zob, category, 'Słownik terminów teatralnych. Patrice Pavis'))

df = pd.DataFrame(terms, columns=['termin', 'zob.', 'kategoria', 'źródło'])
df.to_csv('out_all/terminy/slownik_terminow_teatralnych.csv', index=False)

#%% slownik_gatunkow_literackich

terms = []
with open('out_all/terminy/slownik_gatunkow_literackich.txt', 'r', encoding='utf-8') as txt:
    for line in txt.readlines():
        line = line.replace('\n', '')  
        line = re.sub('\d+', '##', line).strip()
        if len(line)>1:
                        
            splitted = [e.strip() for e in line.split('##') if e.strip()]
            
            for term in splitted:
                czas = term.split(' ')[-1]
                term = ' '.join([e.strip() for e in term.split(' ')[:-1] if e.strip()])
                if term:
                    terms.append((term, czas, 'Słownik gatunków literackich. Marek Bernacki, Marta Pawlus'))

df = pd.DataFrame(terms, columns=['termin', 'czas', 'źródło'])
df.to_csv('out_all/terminy/slownik_gatunkow_literackich.csv', index=False)

#%% teorie_lit_xx_wieku

terms = []
with open('out_all/terminy/teorie_lit_xx_wieku.txt', 'r', encoding='utf-8') as txt:
    for line in txt.readlines():
        line = line.replace('\n', '') 
        line = line.strip('0123456789, -')
        if len(line)>1:
            term = line
            zob = ''
            terms.append((term, zob, 'Teorie literatury XX wieku. Anna Burzyńska, Michał Paweł Markowski'))

df = pd.DataFrame(terms, columns=['termin', 'zob', 'źródło'])
df.to_csv('out_all/terminy/teorie_lit_xx_wieku.csv', index=False)

#%% makowiecki "C:\Users\Nikodem\Documents\ibl_files\code\poir\out_all\byty osobowe\postacie_fikcyjne\makowiecki_indeks.txt"

new_lines = []
with open(r'out_all\byty osobowe\postacie_fikcyjne\makowiecki_indeks.txt', 'r', encoding='utf-8') as txt:
    for line in txt.readlines():
        line = line.replace('\n', '')
        new_lines.append(line)

new_new_lines = []
for index, line in enumerate(new_lines):
    if index != 0 and new_lines[index-1].endswith('-'): continue

    if line.endswith('-'):
        new_new_lines.append(line[:-1] + new_lines[index+1])
    elif not line[0].isupper():
        new_new_lines[-1] = new_new_lines[-1] + ' ' + line
    else:
        new_new_lines.append(line)

new_new_lines = [re.sub('[\d, ]+$', '', e) for e in new_new_lines]

###
lines = []
with open(r'out_all\byty osobowe\postacie_fikcyjne\makowiecki_indeks2.txt', 'r', encoding='utf-8') as txt:
    for line in txt.readlines():
        line = line.replace('\n', '')
        lines.append(line)

to_df = []
for idx, elem in enumerate(lines):
    elem = elem.strip()
    if idx == 0:
        to_df.append((elem, True))
        continue
    
    if elem[0] != lines[idx-1][0]:
        to_df.append((elem, False))
    else:
        to_df.append((elem, True))
   
df = pd.DataFrame(to_df, columns=['postać', 'czy osobny byt'])
    
#%% slownik terminow literackich sierotwinski

with open("slowniki/słownik-terminów-literackich_Sierotwiński.rtf", encoding='utf-8') as rtf:
    content = rtf.read()
    text = rtf_to_text(content, errors="ignore")

lines = [e for e in text.split('\n') if e][85:5451]

terms = []
for line in lines:
    if len(line)>1:
        regx = re.search('^([\p{Lu} -]+)(.+)$', line)
        if regx:
            term, definition = regx.groups()
            term = term.strip()
            definition = definition.strip()
            if len(term) < 2 or (len(term)==3 and term[1]==' '):
                definition = term + definition
                terms[-1][1] = terms[-1][1] + '\n' + definition
            else:
                terms.append([term, definition])

df = pd.DataFrame(terms)
df.to_csv('out_all/terminy/slownik_terminow_literackich.csv', index=False)

#%% slownik pojec i tekstow kultury

with open("slowniki/Słownik pojęć i tekstów kultury.rtf", encoding='utf-8') as rtf:
    content = rtf.read()
    text = rtf_to_text(content, errors="ignore")

lines = [e for e in text.split('\n') if e][213:28128]
indeks = [e for e in text.split('\n') if e][28128:][:1822]
del text, content, rtf

terms_from_indeks = [re.search('^.+?(?= \d|$)', e).group(0) for e in indeks if e[0].islower() and len(e) > 1]
terms_from_full_txt = [e for e in lines if any([x in e for x in ('[', '-*', '->')])]

terms_comma = []
for elem in lines:
    if len(elem) == 1 and elem != '9':
        terms_comma.append(elem)
    elif re.search('^.+?(?=,)', elem) and elem[-1] != ',':
        term = re.search('^.+?(?=,)', elem).group(0)
        if len(term) > 3 and len(term) < 20 and len(term.split(' ')[0]) > 2:
            terms_comma.append(term)

new_out = []
letter = 'a'
for elem in terms_comma:
    if len(elem) == 1:
        letter = elem.lower()
        continue
    if elem[0].lower() == letter or elem.startswith('Zob.'):
        new_out.append(elem)


