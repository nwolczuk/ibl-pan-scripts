import requests
import os
import re
from tqdm import tqdm

# Lista ISSNów
raw_issns = [
    "2406-0518",
    "2340-2784",
    "1496-0974",
    "1578-7044",
    "1224-3086 (Print)",
    "1581-8918 (Print)",
    "1411-2639",
    "1654-6970",
    "1861-6127",
    "1537-0852",
    "1860-2010"
]

errors = []

# Parsowanie i czyszczenie ISSNów
def extract_issns(issn_entry):
    return re.findall(r'\d{4}-\d{3}[\dxX]', issn_entry)

all_issns = set()
for entry in raw_issns:
    all_issns.update(extract_issns(entry))

# Tworzenie katalogu wyjściowego
output_dir = "abstracts"
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
    return re.sub('<[^<]+?>', '', abstract).strip()

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