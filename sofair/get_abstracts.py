import requests
import os
import re
from tqdm import tqdm

# Lista ISSNów DH
raw_issns = ['2532-8816', '2297-2668', '2055-768X', '1938-4122', '1918-3666', '2165-6673', '2188-7276', '2416-5999', '2524-7840', '1556-4711', '2162-5603', '1572-8412', '1755-1706', '1715-0736', '1205-5743', '2376-4228', '2158-3846', '2363-5401', '1746-8256', '2049-1565', '2059-481X', '2397-2068', '2363-4952', '1746-8256', '1940-5758', '2059-5824', '1937-5034', '1574-0218', '1989-9947', '1435-5655', '1432-1300', '2076-0752', '1558-3430', '2050-4551', '1573-7519', '1934-8371', '1544-4554', '2150-6698', '1531-5169', '1095-8363', '1745-2651', '2398-6255', '1873-5371', '1879-1999', '1095-9238', '1533-2756', '1758-8871', '1744-5027', '1467-9841', '1532-2890', '2330-1643', '2325-7989', '1872-7409', '1741-2021', '1530-9282', '1531-4812', '2054-166X', '2159-9610', '1469-8110', '1745-7939', '1804-0462', '1758-7689', '1886-6298', '1934-8118', '2327-9702', '1712-526X', '2053-9517', '1080-2711', '1552-8251', '2377-9039']

raw_issns = [e.strip() for e in raw_issns]

errors = []

# Parsowanie i czyszczenie ISSNów
def extract_issns(issn_entry):
    return re.findall(r'\d{4}-\d{3}[\dxX]', issn_entry)

all_issns = set()
for entry in raw_issns:
    all_issns.update(extract_issns(entry))

# Tworzenie katalogu wyjściowego
output_dir = "abstracts_dh"
os.makedirs(output_dir, exist_ok=True)

# Pobranie danych z Crossref API
def fetch_articles_by_issn(issn):
    url = f"https://api.crossref.org/journals/{issn}/works"
    rows = 1000  # maksymalna liczba
    abstracts = []

    params = {
        'rows': rows,
        'filter': 'has-abstract:true',
    }
    response = requests.get(url, params=params)
    print(response.url)
    if response.ok:
        data = response.json()
        items = data['message'].get('items', [])
        print(data.get('message').get('total-results'))

        for item in items:
            abstract = item.get('abstract')
            doi = item.get('DOI')
            if abstract and doi:
                abstracts.append((doi, abstract))
        return abstracts
    else:
        print(f"Błąd dla ISSN {issn}: {response.status_code}")
        errors.append(issn + '\n')

# Czyszczenie abstraktu z tagów XML
def clean_abstract(abstract):
    return re.sub('<[^ ][^<]+?[^ ]>', '', abstract).strip()

# Zapisywanie abstraktów do plików
for issn in tqdm(all_issns):
    print(f"Pobieranie artykułów dla ISSN: {issn}")
    articles = fetch_articles_by_issn(issn)
    if articles:
        for doi, abstract in articles:
            clean_doi = doi.replace("/", "_")
            filename = f"{output_dir}/{issn}_{clean_doi}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(clean_abstract(abstract))

with open('errors.txt', "w", encoding="utf-8") as f:
    f.writelines(errors)