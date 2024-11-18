import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import regex as re

#%% initial operations

uri_base = 'http://example.org/'
output_df = pd.DataFrame(columns=['subject', 'type', 'predicate', 'object'])

entities_dict = {
        'authors': {},
        'subjects': {},
        'genreforms': {},
    }

flow_control = {
        'work_last_id': 0,
        'instance_last_id': 0,
        'item_last_id': 0,
        'topic_last_id': 0,
        'genreform_last_id': 0,
        'author_last_id': 0,
    }

#%% load df func

def load_input_df(df_name):
    df = pd.read_csv(df_name).fillna('')
    return df

#%% preprocessing funcs
def clear_viaf_uri(url):
    if uri := re.match('https:\/\/viaf\.org\/viaf\/\d+', url):
        return uri.group(0) + '/'
    
def get_viaf_label(url):
    if viaf_uri := clear_viaf_uri(url):
        url = viaf_uri + 'viaf.json'
        response = requests.get(url)
        if response.ok:
            if 'redirect' not in response.json():
                try:
                    label = response.json()['mainHeadings']['data'][0]['text']
                except KeyError:
                    label = response.json()['mainHeadings']['data']['text']
                return label
    
def get_filmpolski_label(url):
    response = requests.get(url)
    if response.ok:
        response.encoding = 'utf-8'
        soup = bs(response.text, 'lxml')
        label = soup.find('article', {'id': 'film'}).find('h1').text
        return label

def preprocess_authors(df):   
    authors_list = set(zip(df['Autor'], df['VIAF autor 1'], df['VIAF autor 2'], df['VIAF autor 3']))
    for author_tuple in authors_list:
        author_splitted = author_tuple[0].split('|')
        for idx, aut in enumerate(author_splitted):
            if idx > 2: break
            if (viaf_url := author_tuple[idx + 1]):
                label = get_viaf_label(viaf_url)
                viaf_uri = clear_viaf_uri(viaf_url)
            else:
                label = aut.strip()
                viaf_uri = None   
            if label and label not in entities_dict['authors']:
                last_id = flow_control['author_last_id']
                author_uri = uri_base + 'agents/agent' + str(last_id + 1).zfill(8)
                entities_dict['authors'][label] = {
                        'uri': author_uri,
                        'viaf_uri': viaf_uri,
                        'bibframe_type': 'bf:Agent',
                    }
                flow_control['author_last_id'] += 1
            
def preprocess_topics(df):
    topics_map = {
            'czasopismo': 'Work',
            'film': 'MovingImage',
            'instytucja': 'Organization',
            'kraj': 'Place',
            'książka': 'Work',
            'miejscowość': 'Place',
            'osoba': 'Person',
            'spektakl': 'Work',
            'wydarzenie': 'Event',
        }
    
    # entities
    df_entities = df[['byt 1', 'zewnętrzny identyfikator bytu 1', 'byt 2', 'zewnętrzny identyfikator bytu 2', 'byt 3', 'zewnętrzny identyfikator bytu 3']]
    entities_list = list(zip(df_entities['byt 1'], df_entities['zewnętrzny identyfikator bytu 1'])) + list(zip(df_entities['byt 2'], df_entities['zewnętrzny identyfikator bytu 2'])) + list(zip(df_entities['byt 3'], df_entities['zewnętrzny identyfikator bytu 3']))
    entities_list = [e for e in entities_list if e[0] and e[1]]
        
    for elem in tqdm(entities_list):
        if elem[1].startswith('https://viaf.org/'):
            label = get_viaf_label(elem[1])
            if label:
               uri = clear_viaf_uri(url)
               bibframe_type = 'bf:' + topics_map[elem[0]]
        elif elem[1].startswith('https://filmpolski.pl/'):
            label = get_filmpolski_label(elem[1])
            if label:
                uri = elem[1]
                bibframe_type = 'bf:' + topics_map[elem[0]]
        if label and label not in entities_dict['subjects']:
            last_id = flow_control['topic_last_id']
            topic_uri = uri_base + 'subjects/subject' + str(last_id + 1).zfill(8)
            entities_dict['subjects'][label] = {
                    'uri': topic_uri,
                    'external_uri': uri,
                    'bibframe_type': bibframe_type,
                }
            flow_control['topic_last_id'] += 1
    
    # other subjects
    topics = set(df['Sekcja'])
    for topic in topics:
        if topic not in entities_dict['subjects']:
            last_id = flow_control['topic_last_id']
            topic_uri = uri_base + 'subjects/subject' + str(last_id + 1).zfill(8)
            entities_dict['subjects'][topic] = {
                    'uri': topic_uri,
                    'external_uri': None,
                    'bibframe_type': 'bf:Topic',
                }
            flow_control['topic_last_id'] += 1
        
def preprocess_forms(df):
    forms = [
                'artykuł', 
                'esej',
                'felieton',
                'inne',
                'kalendarium',
                'kult',
                'list',
                'miniatura prozą',
                'nota',
                'opowiadanie',
                'poemat',
                'proza',
                'proza poetycka',
                'recenzja',
                'reportaż',
                'rozmyślanie religijne',
                'scenariusz',
                'słownik',
                'sprostowanie',
                'szkic',
                'teksty dramatyczne',
                'wiersz',
                'wpis blogowy',
                'wspomnienie',
                'wypowiedź',
                'wywiad',
                'zgon',
            ]
    
    for idx, form in enumerate(forms):
        form_uri = uri_base + 'genreForms/genreform' + str(idx + 1).zfill(8)
        entities_dict['genreforms'][form] = form_uri
    
    

def preprocess_row(idx, row):
    for col, value in row.iteritems():
        work_uri = uri_base + str(flow_control.get('work_last_id') + 1).zfill(8)
        flow_control['work_last_id'] += 1
        instance_uri = uri_base + str(flow_control.get('instance_last_id') + 1).zfill(8)
        flow_control['instance_last_id'] += 1
        item_uri = uri_base + str(flow_control.get('item_last_id') + 1).zfill(8)
        flow_control['item_last_id'] += 1
        
        updt_row = {
                'subject': work_uri, 
                'type': 'bf:Work', 
                'predicate': 'rdf:about', 
                'object': 'https://literarybibliography.eu/pl/search/record/b' + str(flow_control.get('work_last_id')).zfill(8),
            }
        # row update
        updt_row = {
                'subject': work_uri, 
                'type': 'bf:Work', 
                'predicate': 'bf:hasInstance', 
                'object': instance_uri,
            }
        # row update
        updt_row = {
                'subject': instance_uri, 
                'type': 'bf:Instance', 
                'predicate': 'bf:hasItem', 
                'object': item_uri,
            }        
        # row update
        
        match col:
            case 'Link':
                updt_row = {
                        'subject': item_uri, 
                        'type': 'bf:Item', 
                        'predicate': 'bf:electronicLocator', 
                        'object': value.strip()
                    }
            case 'Data publikacji':
                updt_row = {
                        'subject': instance_uri, 
                        'type': 'bf:Instance', 
                        'predicate': 'bf:originDate', 
                        'object': value.strip()
                    }
            case 'Autor':
                pass
            case 'do PBL':
                pass
            case 'VIAF autor 1':
                pass
            case 'VIAF autor 2':
                pass
            case 'VIAF autor 3':
                pass
            case 'Sekcja':
                pass
            case 'Tytuł artykułu':
                updt_row = {
                        'subject': work_uri, 
                        'type': 'bf:Work', 
                        'predicate': 'bf:title', 
                        'object': value.strip()
                    }
            case 'Opis':
                updt_row = {
                        'subject': work_uri, 
                        'type': 'bf:Work', 
                        'predicate': 'bf:summary', 
                        'object': value.strip()
                    }
            case 'Numer':
                # EnumerationAndChronology
                pass
            case 'Tagi':
                pass
            case 'forma/gatunek':
                pass
            case 'hasła przedmiotowe':
                pass
            case 'byt 1':
                pass
            case 'zewnętrzny identyfikator bytu 1':
                pass
            case 'byt 2':
                pass
            case 'zewnętrzny identyfikator bytu 2':
                pass
            case 'byt 3':
                pass
            case 'zewnętrzny identyfikator bytu 3':
                pass
            case 'adnotacje':
                pass
            case 'Linki zewnętrzne':
                pass
            case 'Linki do zdjęć':
                pass


def preprocess_df(df):
    pass


#%% main

input_df_names = ['dwutygodnik_2024-05-06 - Posts.csv']
df = load_input_df(input_df_names[0])



