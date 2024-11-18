import pandas as pd
import gender_guesser.detector as gender
import math
import regex as re
import openpyxl

#%% load data

def clear_authors(authors):
    if not authors:
        return ''
    new_auth_str = []
    for auth in authors.split(' | '):
        if '(tłum.)' not in auth and '(aut. dzieła rec.)' not in auth:
            new_auth_str.append(auth)
    return ' | '.join(new_auth_str)


df = pd.read_excel('KDL_wszystkie zasoby + metadane.xlsx', sheet_name='do równoważenia').fillna('')
df['author'] = df[['author']].apply(lambda x: clear_authors(x['author']), axis=1)

#%% 

def get_epoch(year):
    if not isinstance(year, int):
        try:
            year = int(year)
        except ValueError:
            return 'Year Error'
        
    if year <= 1863:
        return 'Romantyzm'
    elif year <= 1890:
        return 'Pozytywizm'
    elif year <= 1918:
        return 'Młoda Polska'
    elif year < 1939:
        return 'Dwudziestolecie międzywojenne'
    elif year < 1945:
        return '1939-1945'
    elif year <= 1989:
        return '1945-1989'
    else:
        return 'Po 1989'
    
def get_decade(year):
    if not isinstance(year, int):
        try:
            year = int(year)
        except ValueError:
            return 'Year Error'
    return str(math.floor(year/10) * 10)

def get_gender(authors, gender_detector):
    if not authors:
        return ''
    mapping = {'male': 'mężczyzna', 
               'female': 'kobieta', 
               'unknown': 'b.d.', 
               'andy': 'b.d.', 
               'mostly_male': 'mężczyzna', 
               'mostly_female': 'kobieta',
               }
    genders = []
    for auth in authors.split(' | '):
        if ',' in auth:
            name = auth.split(',')[1].strip().split(' ')[0]
        else:
            name = auth.split(' ')[0].strip()
        gender = gender_detector.get_gender(name)
        gender = mapping.get(gender)
        genders.append(gender)
    return ' | '.join(genders)

def get_year(string):
    if year := re.search('\d{4}', str(string)):
        return year.group(0)
    else: return ''
    
    
gender_detector = gender.Detector()
df['author_gender'] = df[['author', 'author_gender']].apply(lambda x: get_gender(x['author'], gender_detector) if not x['author_gender'] else x['author_gender'], axis=1)
df['year_to_check'] = df[['publication_date', 'source_date']].apply(lambda x: get_year(x['publication_date']) if x['publication_date'] else get_year(x['source_date']), axis=1)
df['epoka literacka'] = df[['year_to_check']].apply(lambda x: get_epoch(x), axis=1)
df['dekada'] = df[['year_to_check']].apply(lambda x: get_decade(x), axis=1)

df.to_excel('rownowazenie.xlsx', index=False)
