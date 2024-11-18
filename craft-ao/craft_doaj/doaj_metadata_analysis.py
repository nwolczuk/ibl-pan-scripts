# oai_doaj   
from sickle import Sickle
import xml.etree.ElementTree as ET
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import json
import matplotlib


#%% diamond journals set
diamond_df = pd.read_csv('./data/journalcsv__doaj_20231024_1002_utf8.csv').fillna('')[['Journal title', 'Journal ISSN (print version)', 'Journal EISSN (online version)', 'APC']]
diamond_df = diamond_df[diamond_df['APC'] == 'No'].reset_index().drop(columns=['index', 'APC'])
diamond_issns = set([e for e in list(diamond_df['Journal ISSN (print version)']) + list(diamond_df['Journal EISSN (online version)']) if e])

#%% harvest 2018-present records
border_date = datetime.fromisoformat('2018-01-01')
records_list = []
records_count = 0

# Namespaces for XML parsing
namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'oai_doaj': 'http://doaj.org/features/oai_doaj/1.0/'
}

sickle = Sickle('https://doaj.org/oai.article')
records = sickle.ListRecords(metadataPrefix='oai_doaj', raw=True, ignore_deleted=True)

try:
    for idx, record in tqdm(enumerate(records)):   
        tree = ET.ElementTree(ET.fromstring(record.raw))
        
        # check if issn is in diamond journals issns set
        issns = tree.findall(".//oai_doaj:issn", namespaces)
        eissns = tree.findall(".//oai_doaj:eissn", namespaces)
        all_issns_str = [e.text for e in issns + eissns]
        if any([e in diamond_issns for e in all_issns_str]): 
            if publication_date := tree.findall(".//oai_doaj:publicationDate", namespaces):
                publication_date_dt = datetime.fromisoformat(publication_date[0].text)
                if publication_date_dt >= border_date:
                    records_list.append(record.raw)
        
        records_count += 1
except Exception as e:
    print(f"Error while fetching records: {e}")
    
#%% save in txt

with open('./data/diamond_articles_2018-2023.txt', 'w', encoding='utf-8') as txt:
    for rec in tqdm(records_list):
        txt.write(rec)
        txt.write('\n')
        
#%% load and process txt
all_articles_counter = {
    'all_recs': 0,
    'journalTitle': 0,
    'issn': 0,
    'eissn': 0,
    'volume': 0,
    'issue': 0,
    'startPage': 0,
    'endPage': 0,
    'orcid_count': 0,
    'doi': 0,
    'fullTextUrl': 0,
    'title': 0,
    'language': 0,
    'publisher': 0,
    'publicationDate': 0,
    'abstract': 0,
    'author': 0,
    'keywords': 0,
    'publisherRecordId': 0,
    'documentType': 0,
    'issn_or_eissn': 0,
}

namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'oai_doaj': 'http://doaj.org/features/oai_doaj/1.0/'
}
records_count = 0
issn_counter = {}

with open('./data/diamond_articles_2018-2023.txt', 'r', encoding='utf-8') as txt:
    rec = ''
    for line in tqdm(txt):
        rec += line.replace('\n', '')
        if '</record>' in line:
            field_counts = {
                'all_recs': 0,
                'journalTitle': 0,
                'issn': 0,
                'eissn': 0,
                'volume': 0,
                'issue': 0,
                'startPage': 0,
                'endPage': 0,
                'orcid_count': 0,
                'doi': 0,
                'fullTextUrl': 0,
                'title': 0,
                'language': 0,
                'publisher': 0,
                'publicationDate': 0,
                'abstract': 0,
                'author': 0,
                'keywords': 0,
                'publisherRecordId': 0,
                'documentType': 0,
                'issn_or_eissn': 0,
            }
        
            tree = ET.ElementTree(ET.fromstring(rec))
            
            # check if issn is in diamond journals issns set
            issns = tree.findall(".//oai_doaj:issn", namespaces)
            eissns = tree.findall(".//oai_doaj:eissn", namespaces)
            all_issns_str = [e.text for e in issns + eissns]
            if all_issns_str:
                field_counts['issn_or_eissn'] += 1
            
            field_counts['all_recs'] += 1
            
            # count tags occurence
            for tag in field_counts.keys():
                result = tree.findall(f".//oai_doaj:{tag}", namespaces)
                if result:
                    field_counts[tag] += 1
                    
            # Extracting ORCID from authors
            authors = tree.findall(".//oai_doaj:author", namespaces)
            for author in authors:
                orcid = author.findall("oai_doaj:orcid_id", namespaces)
                if orcid:
                    field_counts['orcid_count'] += 1
                    break
            
            if tuple(all_issns_str) in issn_counter:
                for k,v in field_counts.items():
                    issn_counter[tuple(all_issns_str)][k] += v
            else:
                issn_counter[tuple(all_issns_str)] = field_counts
            
            for k,v in field_counts.items():
                all_articles_counter[k] += v

            rec = ''
        
# process 
journal_groups = [(k, v) for k,v in issn_counter.items()]

diamond_journals_dict = {}
for idx, row in diamond_df.iterrows():
    diamond_journals_dict[idx] = {'journal': row['Journal title'], 'issns': [e for e in [row['Journal ISSN (print version)'], row['Journal EISSN (online version)']] if e]}

issns_dict = {}
for k,v in diamond_journals_dict.items():
    for issn in v['issns']:
        issns_dict.setdefault(issn, []).append(k)

for elem in journal_groups:
    for issn in elem[0]:
        if issn in issns_dict:
            idx = issns_dict[issn][0]
            if 'stats' in diamond_journals_dict[idx]:
                for k,v in elem[1].items():
                    diamond_journals_dict[idx]['stats'][k] += v
            else:
               diamond_journals_dict[idx]['stats'] = elem[1].copy()
            break

journal_gruops_combined = {k:v for k,v in diamond_journals_dict.items() if len(v)==3}

df = pd.DataFrame()
for k,v in tqdm(journal_gruops_combined.items()):
    row = pd.DataFrame().from_dict([v['stats']])
    row['journal'] = v['journal']
    df = pd.concat([df, row])

df = df[['journal', 'abstract', 'author', 'doi', 'eissn', 'fullTextUrl', 'issn', 'keywords', 'language', 'orcid_count', 'publicationDate', 'publisherRecordId']]

df2 = pd.DataFrame()
for k,v in tqdm(journal_gruops_combined.items()):
    row = pd.DataFrame().from_dict([v['stats']])
    row = row.div(row['all_recs'], axis=0).round(2)
    row['journal'] = v['journal']
    df2 = pd.concat([df2, row])

df2 = df2[['journal', 'abstract', 'author', 'doi', 'eissn', 'fullTextUrl', 'issn', 'keywords', 'language', 'orcid_count', 'publicationDate', 'publisherRecordId']]

# save aoutputs
df.to_excel('./data/output_2018_2023/diamond_journals_articles_stats.xlsx', index=False)
   
df_all_recs = pd.DataFrame().from_dict(all_articles_counter, orient='index')
df_all_recs.to_excel('./data/output_2018_2023/diamond_journals_full_stats2.xlsx')

desc = df.describe()
desc.to_excel('./data/output_2018_2023/diamond_journals_describe.xlsx')

desc2 = df2.describe()
desc2.to_excel('./data/output_2018_2023/diamond_journals_describe_percentage.xlsx')
    
#%% tests
test = df.quantile(0.9)
desc = df.describe()

desc.to_excel('test.xlsx')

gauss_df = df[['journal', 'doi']]
gauss_df.plot.density()
gauss_df.plot.kde()


# with open('./data/diamond_articles_2018-2023.txt', 'r', encoding='utf-8') as txt:
#     rec = ''
#     for line in tqdm(txt):
#         rec += line.replace('\n', '')
#         if '</record>' in line:
        
#             tree = ET.ElementTree(ET.fromstring(rec))
            
#             # check if issn is in diamond journals issns set
#             title = tree.findall(".//oai_doaj:title", namespaces)
#             if not title:
#                 break
#             rec = ''








#%% count full stats
# Initialize the Sickle object
sickle = Sickle('https://doaj.org/oai.article')

# Define the dictionary to store counts of the fields
all_records_field_counter = {
    'all_recs': 0,
    'journalTitle': 0,
    'issn': 0,
    'eissn': 0,
    'volume': 0,
    'issue': 0,
    'startPage': 0,
    'endPage': 0,
    'orcid_count': 0,
    'doi': 0,
    'fullTextUrl': 0,
    'title': 0,
    'language': 0,
    'publisher': 0,
    'publicationDate': 0,
    'abstract': 0,
    'author': 0,
    'keywords': 0,
    'publisherRecordId': 0,
    'documentType': 0,
    'issn_or_eissn': 0,
}

issn_counter = {}

records_count = 0

# Namespaces for XML parsing
namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'oai_doaj': 'http://doaj.org/features/oai_doaj/1.0/'
}

records = sickle.ListRecords(metadataPrefix='oai_doaj', raw=True, ignore_deleted=True)

try:
    for idx, record in tqdm(enumerate(records)):        
        field_counts = {
            'all_recs': 0,
            'journalTitle': 0,
            'issn': 0,
            'eissn': 0,
            'volume': 0,
            'issue': 0,
            'startPage': 0,
            'endPage': 0,
            'orcid_count': 0,
            'doi': 0,
            'fullTextUrl': 0,
            'title': 0,
            'language': 0,
            'publisher': 0,
            'publicationDate': 0,
            'abstract': 0,
            'author': 0,
            'keywords': 0,
            'publisherRecordId': 0,
            'documentType': 0,
            'issn_or_eissn': 0,
        }
    
        tree = ET.ElementTree(ET.fromstring(record.raw))
        
        # check if issn is in diamond journals issns set
        issns = tree.findall(".//oai_doaj:issn", namespaces)
        eissns = tree.findall(".//oai_doaj:eissn", namespaces)
        all_issns_str = [e.text for e in issns + eissns]
        if any([e in diamond_issns for e in all_issns_str]): 
    
            if all_issns_str:
                field_counts['issn_or_eissn'] += 1
            
            field_counts['all_recs'] += 1
            
            # count tags occurence
            for tag in field_counts.keys():
                result = tree.findall(f".//oai_doaj:{tag}", namespaces)
                if result:
                    field_counts[tag] += 1
                    
            # Extracting ORCID from authors
            authors = tree.findall(".//oai_doaj:author", namespaces)
            for author in authors:
                orcid = author.findall("oai_doaj:orcid_id", namespaces)
                if orcid:
                    field_counts['orcid_count'] += 1
                    break
            
            if tuple(all_issns_str) in issn_counter:
                for k,v in field_counts.items():
                    issn_counter[tuple(all_issns_str)][k] += v
            else:
                issn_counter[tuple(all_issns_str)] = field_counts
            
            for k,v in field_counts.items():
                all_records_field_counter[k] += v
        records_count += 1
except Exception as e:
    print(f"Error while fetching records: {e}")

#%%

groups_to_save = [(k, v) for k,v in issn_counter.items()]
all_records_field_counter['doaj_database_counter'] = records_count

with open('/data/doaj_recs_stats_full.json', 'w', encoding='utf-8') as jfile:
    json.dump(all_records_field_counter, jfile, indent=4, ensure_ascii=False)
    
with open('/data/doaj_recs_stats_groups.json', 'w', encoding='utf-8') as jfile:
    json.dump(groups_to_save, jfile, indent=4, ensure_ascii=False)
    
#%%
with open('/data/doaj_recs_stats_full.json', 'r', encoding='utf-8') as jfile:
    all_records = json.load(jfile)
    
with open('/data/doaj_recs_stats_groups.json', 'r', encoding='utf-8') as jfile:
    journal_groups = json.load(jfile)

diamond_journals_dict = {}
for idx, row in diamond_df.iterrows():
    diamond_journals_dict[idx] = {'journal': row['Journal title'], 'issns': [e for e in [row['Journal ISSN (print version)'], row['Journal EISSN (online version)']] if e]}

issns_dict = {}
for k,v in diamond_journals_dict.items():
    for issn in v['issns']:
        issns_dict.setdefault(issn, []).append(k)

for elem in journal_groups:
    for issn in elem[0]:
        if issn in issns_dict:
            idx = issns_dict[issn][0]
            if 'stats' in diamond_journals_dict[idx]:
                for k,v in elem[1].items():
                    diamond_journals_dict[idx]['stats'][k] += v
            else:
               diamond_journals_dict[idx]['stats'] = elem[1].copy()
            break

output = {k:v for k,v in diamond_journals_dict.items() if len(v)==3}

with open('/data/doaj_recs_stats_groups_combined.json', 'w', encoding='utf-8') as jfile:
    json.dump(output, jfile, indent=4, ensure_ascii=False)

# abstract
# author
# doi
# eissn
# fullTextUrl
# issn
# keywords
# language
# orcid_count
# publicationDate
# publisherRecordId

with open('/data/doaj_recs_stats_groups_combined.json', 'r', encoding='utf-8') as jfile:
    journal_gruops_combined = json.load(jfile)


df = pd.DataFrame()
for k,v in tqdm(journal_gruops_combined.items()):
    row = pd.DataFrame().from_dict([v['stats']])
    row = row.div(row['all_recs'], axis=0).round(2)
    row['journal'] = v['journal']
    df = pd.concat([df, row])

df = df[['journal', 'abstract', 'author', 'doi', 'eissn', 'fullTextUrl', 'issn', 'keywords', 'language', 'orcid_count', 'publicationDate', 'publisherRecordId']]

df.to_excel('/data/output/diamond_journals_articles_stats.xlsx', index=False)

#%%

test = df.quantile(0.9)
desc = df.describe()

desc.to_excel('test.xlsx')

gauss_df = df[['journal', 'doi']]
gauss_df.plot.density()
gauss_df.plot.kde()

#%%
df_all_recs = pd.DataFrame().from_dict(all_records, orient='index')
df_all_recs.to_excel('/data/output/diamond_journals_full_stats.xlsx')


desc = df.describe()
desc.to_excel('/data/output/diamond_journals_describe_percentage.xlsx')

