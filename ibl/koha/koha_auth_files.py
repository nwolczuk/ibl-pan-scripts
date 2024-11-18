import sys
from marc_functions import write_mrk
import pandas as pd
from tqdm import tqdm
import regex as re
import numpy as np


#%%

def clear_new_lines(value):
    value = value.replace('\n', ' ')
    return value

def get_subfield_code(mapping_string):
    mapping_str_len = len(mapping_string)
    match mapping_str_len:
        case 4:
            return '$' + mapping_string[3]
        case 3:
            return ''
        case _:
            raise Exception('MAPPING STRING LENGTH ERROR')

def get_indicators(field_code):
    match field_code:
        case '001':
            return ''
        case '100':
            return '1\\'
        case '400' | '550' | '667':
            return '\\\\'
        case _:
            raise Exception('NO MATCHING FIELD CODE CASE')

def get_separator(field_code, subfield_code):
    match_phrase = field_code + subfield_code
    match match_phrase:           
        case '100$a':
            return ', '
        case '100$d':
            return ''
        case '667$a':
            return '; '
        case '400$a' | '550$a':
            return ''
        case _:
            raise Exception('CAN\'T GET SEPARATOR')


def prepare_dates(field100):
    field100 = record_dict['100'][0]
    if '$d' in field100:
        dates = re.search('(?<=\$d).+?(?=\$|$)', field100).group(0)
        dates = dates.replace('--', '-')
        field100 = re.sub('(?<=\$d).+?(?=\$|$)', dates, field100)
    return field100
    
def create_new_field():
    indicators = get_indicators(field_code)
    field = indicators + subfield_code + value
    if field_code in record_dict:
        record_dict[field_code].append(field)
    else:
        record_dict[field_code] = [field]
    return

def update_last_field():
    if field_code in record_dict:
        last_field = record_dict[field_code][-1]
        if subfield_code in last_field:
            separator = get_separator(field_code, subfield_code)
            last_field = last_field + separator + value
        else:
            last_field = last_field + subfield_code + value
        record_dict[field_code][-1] = last_field
    else:
        indicators = get_indicators(field_code)
        last_field = indicators
        if subfield_code in last_field:
            separator = get_separator(field_code, subfield_code)
            last_field = last_field + separator + value
        else:
            last_field = last_field + subfield_code + value
        record_dict[field_code] = [last_field]
    return

def update_record_dict():
    match field_code:
        case '100':
            update_last_field()
            return
        case '550':
            global value
            value = dzialy.get(value)
            create_new_field()
            return
        case '001' | '400' | '667':
            create_new_field()
            return
        case _:
            return

#%%

authors_df = pd.read_excel('/data/tworcy.xlsx').fillna('')
dzialy = pd.read_excel('/data/dzialy.xlsx').fillna('')
dzialy = {str(row['DZ_DZIAL_ID']):row['DZ_NAZWA'] for index, row in dzialy.iterrows()}

mapping = { 
            'TW_TWORCA_ID': '001',
            'TW_NAZWISKO': '100a',
            'TW_IMIE': '100a',
            'TW_DZ_DZIAL_ID': '550a',
            'TW_NAZW_WLASCIWE': '',
            'TW_PSEUDONIMY': '400a',
            'TW_DATA_URODZIN': '',
            'TW_DATA_ZGONU': '',
            'TW_ROCZNIKI_PBL': '',
            'TW_SLOWA_KLUCZOWE': '',
            'TW_UWAGI': '667a',
            'TW_ROK_URODZIN': '100d',
            'TW_ROK_ZGONU': '100d',
            'TW_LICZBA_ZAPISOW': '',
            'TW_ADNOTACJE': '667a',
            'TW_TRANSLITERACJE': '',
            'TW_STATUS': '',
            'TW_NUMER_BN': '',
            'TW_VIAF': '1001'
            }

output = []
for index, row in tqdm(authors_df.iterrows(), total = len(authors_df)):
    record_dict = dict()
    for heading, value in row.items():
        if value and value != 'NaT':
            if heading == 'TW_ROK_URODZIN':
                value = str(int(value)) + '-'
            elif heading == 'TW_ROK_ZGONU':
                value = '-' + str(int(value))
            else:
                value = str(value)
            value = clear_new_lines(value)
            mapping_string = mapping.get(heading)
            if mapping_string:
                field_code = mapping_string[:3]
                if field_code == '001':
                    value = value.zfill(8)
                subfield_code = get_subfield_code(mapping_string)
                update_record_dict()
            else: continue
        else: continue
    
    record_dict['100'][0] = prepare_dates(record_dict['100'][0])
    
    if '667' in record_dict:
        record_dict['667'].append('\\\\$aKartoteka twórców')
    else:
        record_dict['667'] = ['\\\\$aKartoteka twórców']
    record_dict['040'] = ['\\\\$aIBL']
    record_dict['008'] = ['221121|| aca||aabn           | a|a     d']
    record_dict['LDR'] = ['00000nz  a22     n  4500']   
    
    output.append(record_dict)

write_mrk('/data/tworcy.mrk', output)

#%%


authors_df = pd.read_excel('/data/autorzy.xlsx').fillna('')

mapping = { 
            'AM_AUTOR_ID': '001', 
            'AM_IMIE': '100a',
            'AM_NAZWISKO': '100a', 
            'AM_KRYPTONIM': '400a',
            'AM_KRYPTONIM_OPIS': '667a',
            'AM_UWAGI': '667a', 
            'AM_LICZBA_ZAPISOW': '',
            'AM_TRANSLITERACJE': '',
            'AM_STATUS': '',
            'AM_NUMER_BN': '',
            'AM_VIAF': '1001'
            }

output = []
for index, row in tqdm(authors_df.iterrows(), total = len(authors_df)):
    record_dict = dict()
    for heading, value in row.items():
        if value and value != 'NaT':
            if heading == 'TW_ROK_URODZIN':
                value = str(int(value)) + '-'
            elif heading == 'TW_ROK_ZGONU':
                value = '-' + str(int(value))
            else:
                value = str(value)
            value = clear_new_lines(value)
            mapping_string = mapping.get(heading)
            if mapping_string:
                field_code = mapping_string[:3]
                if field_code == '001':
                    value = value.zfill(8)
                subfield_code = get_subfield_code(mapping_string)
                update_record_dict()
            else: continue
        else: continue
    
        
    if '100' not in record_dict and '400' in record_dict:
        record_dict['100'] = [record_dict['400'][0]]
        del record_dict['400'][0]
        if not record_dict['400']:
            del record_dict['400']
    elif '100' not in record_dict and '400' not in record_dict: continue   
            
            
    record_dict['100'][0] = prepare_dates(record_dict['100'][0])
    
    if '667' in record_dict:
        record_dict['667'].append('\\\\$aKartoteka autorów')
    else:
        record_dict['667'] = ['\\\\$aKartoteka autorów']
    
    record_dict['040'] = ['\\\\$aIBL']
    record_dict['008'] = ['221121|| aca||aabn           | a|a     d']
    record_dict['LDR'] = ['00000nz  a22     n  4500']   
    
    output.append(record_dict)

output1 = output[:60000]   
output2 = output[60000:]  
write_mrk('autorzy1.mrk', output1)
write_mrk('autorzy2.mrk', output2)

#%%

osoby_df = pd.read_excel('/data/osoby.xlsx').fillna('')
mapping = { 
            'OS_OSOBA_ID': '001', 
            'OS_NAZWISKO': '100a',
            'OS_IMIE': '100a', 
            'OS_UWAGI': '667a',
            'OS_LICZBA_ZAPISOW': '',
            'OS_STATUS': '',
            'OS_NUMER_BN': '',
            'OS_VIAF': '1001'
            }

output = []
for index, row in tqdm(osoby_df.iterrows(), total = len(osoby_df)):
    record_dict = dict()
    for heading, value in row.items():
        if value and value != 'NaT':
            if heading == 'TW_ROK_URODZIN':
                value = str(int(value)) + '-'
            elif heading == 'TW_ROK_ZGONU':
                value = '-' + str(int(value))
            else:
                value = str(value)
            value = clear_new_lines(value)
            mapping_string = mapping.get(heading)
            if mapping_string:
                field_code = mapping_string[:3]
                if field_code == '001':
                    value = value.zfill(8)
                subfield_code = get_subfield_code(mapping_string)
                update_record_dict()
            else: continue
        else: continue
    
        
    if '100' not in record_dict: continue
            
    record_dict['100'][0] = prepare_dates(record_dict['100'][0])
    
    if '667' in record_dict:
        record_dict['667'].append('\\\\$aKartoteka współtwórców')
    else:
        record_dict['667'] = ['\\\\$aKartoteka współtwórców']
    
    record_dict['040'] = ['\\\\$aIBL']
    record_dict['008'] = ['221121|| aca||aabn           | a|a     d']
    record_dict['LDR'] = ['00000nz  a22     n  4500']   
    
    output.append(record_dict)

 
write_mrk('/data/wspoltworcy.mrk', output)

#%% teatry

def get_subfield_code(mapping_string):
    mapping_str_len = len(mapping_string)
    match mapping_str_len:
        case 4:
            return '$' + mapping_string[3]
        case 3:
            return ''
        case _:
            raise Exception('MAPPING STRING LENGTH ERROR')

def get_indicators(field_code):
    match field_code:
        case '001':
            return ''
        case '110' | '510':
            return '1\\'
        case '370' | '550' | '667':
            return '\\\\'
        case _:
            raise Exception('NO MATCHING FIELD CODE CASE')

def create_new_field():
    indicators = get_indicators(field_code)
    field = indicators + subfield_code + value
    if field_code in record_dict:
        record_dict[field_code].append(field)
    else:
        record_dict[field_code] = [field]
    return

def update_record_dict():
    global value
    match field_code:
        case '510':
            value = teatry_dict.get(value)
            if not value: return
            create_new_field()
            return
        case '550':
            try:
                value = dzialy[value]
            except:
                value = rodzaj_teatru.get(value)
            create_new_field()
            return
        case '001' | '110' | '370' | '667':
            create_new_field()
            return
        case _:
            return
        
mapping = {
    'TE_TEATR_ID': '001',
	'TE_SKROT': '', 
	'TE_NAZWA': '110a',
	'TE_MIASTO': '370e',
	'TE_RT_SYMBOL': '550a', # get rodzaj
	'TE_PANSTWO': '370c',
	'TE_DZ_DZIAL_ID': '550a', # get dzial.xlsx
	'TE_TE_TEATR_ID': '510a', # get theatre name
	'TE_ADNOTACJE': '667a',
	'TE_LICZBA_ZAPISOW': ''

    }

rodzaj_teatru = {
    'PA':'Teatry w państwie',
    'MI': 'Teatry w mieście',
    'TE': 'Teatr',
    'SC': 'Scena'}

# wszedzie dodac 368c 'teatry'
teatry_df = pd.read_excel('/data/teatry.xlsx').fillna('')
dzialy = pd.read_excel('/data/dzialy.xlsx').fillna('')
dzialy = {str(row['DZ_DZIAL_ID']):row['DZ_NAZWA'] for index, row in dzialy.iterrows()}
teatry_dict = {str(row['TE_TEATR_ID']):row['TE_NAZWA'] for index, row in teatry_df.iterrows()}

output = []
for index, row in tqdm(teatry_df.iterrows(), total = len(teatry_df)):
    record_dict = dict()
    for heading, value in row.items():
        if value:
            if isinstance(value, float):
                value = int(value)
            value = str(value).replace('\n', ' ').strip()
            mapping_string = mapping.get(heading)
            if mapping_string:
                field_code = mapping_string[:3]
                if field_code == '001':
                    value = value.zfill(8)
                subfield_code = get_subfield_code(mapping_string)
                update_record_dict()
            else: continue
        else: continue
    
    if '667' in record_dict:
        record_dict['667'].append('\\\\$aKartoteka teatrów')
    else:
        record_dict['667'] = ['\\\\$aKartoteka teatrów']
    
    record_dict['040'] = ['\\\\$aIBL']
    record_dict['008'] = ['221121|| aca||aabn           | a|a     d']
    record_dict['LDR'] = ['00000nz  a22     n  4500']   
    record_dict['368'] = ['\\\\$cteatry']
    output.append(record_dict)
 
write_mrk('/data/teatry.mrk', output)

#%% zrodla

def get_subfield_code(mapping_string):
    mapping_str_len = len(mapping_string)
    match mapping_str_len:
        case 4:
            return '$' + mapping_string[3]
        case 3:
            return ''
        case _:
            raise Exception('MAPPING STRING LENGTH ERROR')

def get_indicators(field_code):
    match field_code:
        case '001':
            return ''
        case '130' | '430' | '530':
            return '\\0'
        case '510':
            return '1\\'
        case '368' | '386' | '550' | '667':
            return '\\\\'
        case _:
            raise Exception('NO MATCHING FIELD CODE CASE')

def create_new_field():
    indicators = get_indicators(field_code)
    field = indicators + subfield_code + value
    if field_code in record_dict:
        record_dict[field_code].append(field)
    else:
        record_dict[field_code] = [field]
    return

def update_record_dict():
    global value
    match field_code:
        case '001' | '130' | '510':
            create_new_field()
            return
        case '368':
            value = czestotliwosc.get(value)
            if not value: return
            create_new_field()
            return
        case '386':
            value = value + '$mMiejscowość'
            if not value: return
            create_new_field()
            return
        case '430':
            value = row['ZR_TYTUL'] + ': ' + value
            create_new_field()
            return
        case '530':
            value = zrodla_dict.get(value)
            if not value: return
            create_new_field()
            return
        case '667':
            if heading == 'ZR_SKROT':
                value = 'Skrót: ' + value
            elif heading == 'ZR_ZBIOR_SYGNATURA':
                value = 'Sygnatura: ' + value
            elif heading == 'ZR_ROK_NUMERY':
                value = 'Numery: ' + value
            elif heading == 'ZR_BRAKI_NUMEROWE':
                value = 'Braki numerowe: ' + value
            elif heading == 'ZR_ADNOTACJE_ZMIANY':
                value = 'Zmiany: ' + value
            create_new_field()
            return
        case _:
            return
     
# 001 130 368 386 430 510 530 667     
mapping = {
    'ZR_ZRODLO_ID': '001',
	'ZR_ZR_ZRODLO_ID': '530a', # get title 
	'ZR_SKROT': '667a', # + skrot
	'ZR_TYTUL': '130a',
    'ZR_PODTYTUL': '430a', # tytuł + podtytuł
	'ZR_DODATEK_DO': '667a',
	'ZR_RZR_SYMBOL': '368a', #authorized values - czestotliwosc.xlsx
	'ZR_INSTYTUCJA': '510a',
	'ZR_MIEJSCE_WYD': '386a', # + $mMiejscowość
	'ZR_ZBIOR_SYGNATURA': '667a', # + sygnatura
	'ZR_ROK_WYDANIA': '',
	'ZR_ROK_NUMERY': '667a', # + numery
	'ZR_BRAKI_NUMEROWE': '667a', # + braki
	'ZR_SLOWA_KLUCZOWE': '',
	'ZR_UWAGI': '667a',
	'ZR_ADNOTACJE_ZMIANY': '667a', # + zmiany
	'ZR_KTO_OPRACOWUJE': '',
	'ZR_LICZBA_ZAPISOW': '',
	'ZR_CZY_BIEZACY_ROCZNIK': '',
	'ZR_CZY_PUBLIKACJA_WWW': ''
    }

zrodla_df = pd.read_excel('/data/zrodla.xlsx').fillna('')
czestotliwosc = {elem['RZR_SYMBOL']:elem['RZR_NAZWA'] for elem in pd.read_excel('czestotliwosc.xlsx').fillna('').to_dict(orient='records')}
zrodla_dict = {str(row['ZR_ZRODLO_ID']):row['ZR_TYTUL'] for index, row in zrodla_df.iterrows()}

output = []
for index, row in tqdm(zrodla_df.iterrows(), total = len(zrodla_df)):
    record_dict = dict()
    for heading, value in row.items():
        if value:
            if isinstance(value, float):
                value = int(value)
            value = str(value).replace('\n', ' ').strip()
            mapping_string = mapping.get(heading)
            if mapping_string:
                field_code = mapping_string[:3]
                if field_code == '001':
                    value = value.zfill(8)
                subfield_code = get_subfield_code(mapping_string)
                update_record_dict()
            else: continue
        else: continue
    
    if '667' in record_dict:
        record_dict['667'].append('\\\\$aKartoteka źródeł')
    else:
        record_dict['667'] = ['\\\\$aKartoteka źródeł']
    
    record_dict['040'] = ['\\\\$aIBL']
    record_dict['008'] = ['221121|| aca||aabn           | a|a     d']
    record_dict['LDR'] = ['00000nz  a22     n  4500']   

    output.append(record_dict)

write_mrk('/data/zrodla.mrk', output)

#%%


