import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import regex as re

from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS

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
            label = aut.strip()
            if (viaf_url := author_tuple[idx + 1]):
                viaf_label = get_viaf_label(viaf_url)
                viaf_uri = clear_viaf_uri(viaf_url)
            else:
                viaf_label = None
                viaf_uri = None   
            if label not in entities_dict['authors']:
                last_id = flow_control['author_last_id']
                author_id = str(last_id + 1).zfill(8)
                entities_dict['authors'][label] = {
                        'author_id': author_id,
                        'viaf_uri': viaf_uri,
                        'viaf_label': viaf_label,
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
            topic_id = str(last_id + 1).zfill(8)
            entities_dict['subjects'][topic] = {
                    'topic_id': topic_id,
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



# work = EX["works/000001"]
# g.add((work, RDF.type, BF.Work))

# contribution = BNode()
# g.add((work, BF.contribution, contribution))
# g.add((contribution, RDF.type, BF.Contribution))
# g.add((contribution, RDF.type, BF.PrimaryContribution))

# # Tworzenie węzła bf:Agent
# agent = EX["agents/000001"]
# g.add((contribution, BF.agent, agent))
# g.add((agent, RDF.type, BF.Agent))
# g.add((agent, RDF.type, BF.Person))
# g.add((agent, RDFS.label, Literal("Casado Velarde, Manuel")))

# print(g.serialize(format="pretty-xml"))

value = 'TEST VALUE TEST VALUE TEST VALUE'


ELB = Namespace("http://literarybibliography.eu/")
BF = Namespace("http://id.loc.gov/ontologies/bibframe/")

g = Graph()
g.bind("elb", ELB)
g.bind("bf", BF)

print(g.serialize(format="pretty-xml"))
    
def preprocess_row(idx, row):
    for col, value in row.iteritems():
        work_id = str(flow_control.get('work_last_id') + 1).zfill(8)
        work = ELB[f'works/{work_id}']
        flow_control['work_last_id'] += 1
        g.add((work, RDF.type, BF.Work))
        
        instance_id = str(flow_control.get('instance_last_id') + 1).zfill(8)
        instance = ELB[f'instances/{instance_id}']
        flow_control['instance_last_id'] += 1
        g.add((work, BF.hasInstance, instance))
        g.add((instance, RDF.type, BF.Instance))
        g.add((instance, BF.instanceOf, work))
        
        item_id = str(flow_control.get('item_last_id') + 1).zfill(8)
        item = ELB[f'items/{item_id}']
        flow_control['item_last_id'] += 1
        g.add((instance, BF.hasItem, item))
        g.add((item, RDF.type, BF.Item))
        g.add((item, BF.itemOf, instance))
        
        match col:
            case 'Link':
                g.add((item, BF.electronicLocator, URIRef(value.strip())))
                
            case 'Data publikacji':
                g.add((instance, BF.originDate, Literal(value.strip())))
                
            case 'Autor':
                for author in value.split('|'):
                    author_dct = entities_dict['authors'].get(author.strip())
                    if author_dct:
                        author_id, viaf_uri, viaf_label = author_dct['author_id'], author_dct['viaf_uri'], author_dct['viaf_label'] # it is possible to use locals().update(author_dct)
                        label = viaf_label if viaf_label else author
                        
                        # create an Agent
                        agent = ELB[f'agents/{author_id}']
                        g.add((agent, RDF.type, BF.Agent))
                        g.add((agent, RDF.type, BF.Person))
                        g.add((agent, RDFS.label, Literal(label)))
                        if viaf_uri:
                            identifier = BNode()
                            g.add((identifier, RDF.type, BF.Identifier))
                            g.add((identifier, RDF.value, Literal(viaf_uri)))
                            g.add((agent, BF.identifiedBy, identifier))
        
                        # add Agent
                        contribution = BNode()
                        g.add((work, BF.contribution, contribution))
                        g.add((contribution, RDF.type, BF.Contribution))
                        g.add((contribution, RDF.type, BF.PrimaryContribution))
                        g.add((contribution, BF.agent, agent))
                
            case 'do PBL':
                pass
            
            case 'VIAF autor 1':
                pass
            
            case 'VIAF autor 2':
                pass
            
            case 'VIAF autor 3':
                pass
            
            case 'Sekcja':
                topic_dct = entities_dict['subjects'].get(value.strip())
                if topic_dct:
                    topic_id = topic_dct['topic_id']
                    topic = ELB[f'subjects/{topic_id}']
                    g.add((work, BF.subject, topic))
                    g.add((topic, RDF.type, BF.Topic))
                    g.add((topic, RDFS.label, Literal(value.strip())))
                
            case 'Tytuł artykułu':                
                title = BNode()
                g.add((work, BF.title, title))
                g.add((title, RDF.type, BF.Title))
                g.add((title, BF.mainTitle, Literal(value.strip())))
                
            case 'Opis':
                summary = BNode()
                g.add((work, BF.summary, summary))
                g.add((summary, RDF.type, BF.Summary))
                g.add((summary, RDFS.label, Literal(value.strip())))
                
            case 'Numer':
                # EnumerationAndChronology
                pass
            
            case 'Tagi':
                pass
            
            case 'forma/gatunek':
                genreform = BNode()
                g.add((work, BF.genreForm, genreform))
                g.add((genreform, RDF.type, BF.GenreForm))
                g.add((genreform, RDFS.label, Literal(value.strip())))
                
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



