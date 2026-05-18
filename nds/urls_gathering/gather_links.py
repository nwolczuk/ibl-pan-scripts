from sickle import Sickle
from tqdm import tqdm
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from sickle.oaiexceptions import NoRecordsMatch

oai_url = 'https://bibliotekanauki.pl/api/oai/articles'

with open('subjects.txt', 'r', encoding='utf-8') as txt:
    accepted_subjects = txt.readlines()

accepted_subjects = set([e.strip() for e in accepted_subjects])

sickle = Sickle(oai_url)

start_date = datetime.strptime("2024-06-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-01-01", "%Y-%m-%d")

while True:
    
    if start_date == end_date:
        print('End date reached. End of process.')
        break

    arguments = {
                    'metadataPrefix': 'jats',
                    'from': start_date.strftime("%Y-%m-%d"),
                    'until': start_date.strftime("%Y-%m-%d"),
                    'ignore_deleted': True
                }
    
    print(f'\nstart processing day {start_date.strftime("%Y-%m-%d")}')
    
    try:
        records = sickle.ListRecords(**arguments)
    except NoRecordsMatch:
        print('No records match')
        start_date = start_date + timedelta(days=1)
        continue
    
    with open('links.txt', 'a', encoding='utf-8') as txt:
        for record in tqdm(records):
            soup = BeautifulSoup(record.raw, "lxml")
            article_subjects = soup.find_all('subject')
            article_subjects = [e.text for e in article_subjects]
            if set(article_subjects) & accepted_subjects:
                txt.write(record.header.identifier + ' | ' + record.header.datestamp + '\n')
    print('process ended successfuly')
    
    start_date = start_date + timedelta(days=1)

    