from pymarc import MARCReader, MARCWriter, record_to_xml
from tqdm import tqdm

def find_latvian_to_polish_translations(marc_file_path):
    ltv_records = []
    # Otwieranie pliku MARC21
    with open(marc_file_path, 'rb') as marc_file:
        reader = MARCReader(marc_file)
        for record in tqdm(reader):
            
            if '008' in record:
                field_008 = record['008'].data
                try:
                    year_008 = int(field_008[7:11])
                except ValueError: continue
                    
                if year_008 < 2012: continue
            else: continue
        
            # Sprawdzanie obecności pola 041 (Language Code)
            field_041 = record.get_fields('041')
            for field in field_041:
                subfield_h = field.get_subfields('h')
                subfield_a = field.get_subfields('a')
                
                # Sprawdzanie czy język źródłowy to łotewski ('lav' dla łotewskiego)
                if any(e in subfield_h for e in ['lav', 'lv']) and 'pol' in subfield_a:
                    ltv_records.append(record)
    return ltv_records               

marc_file_path = r"C:\Users\Nikodem\Documents\bn data\bibs-all.marc"
ltv_translations = find_latvian_to_polish_translations(marc_file_path)


with open('latvian_translations_records.txt', 'w', encoding='utf-8') as fh:
    for record in ltv_translations:
        fh.write(str(record))
        fh.write('\n\n')
        
with open('latvian_translations_records.mrc', 'wb') as fh:
    writer = MARCWriter(fh)
    for record in ltv_translations:
        writer.write(record)
    writer.close()
    
with open('output_file.xml', 'wb') as fh:
    fh.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<collection>\n')
    for record in ltv_translations:
        fh.write(record_to_xml(record))
    fh.write(b'</collection>')

#%%

from pymarc import MARCReader
import pandas as pd

# Funkcja do wczytania pliku MARC i wyciągnięcia najważniejszych informacji
def extract_marc_info(marc_file):
    records = []
    with open(marc_file, 'rb') as fh:
        reader = MARCReader(fh)
        for record in reader:
            record_id = record['001'].value() if record['001'] else 'Brak ID'
            title = record['245']['a'] if record['245'] else 'Brak tytułu'
            author = record['100']['a'] if record['100'] else 'Brak autora'
            place_of_publication = record['260']['a'] if record['260'] else 'Brak miejsca wydania'
            publisher = record['260']['b'] if record['260'] else 'Brak wydawcy'
            pub_year = record['260']['c'] if record['260'] else 'Brak roku wydania'
            source = record['773']['t'] if record['773'] and 't' in record['773'] else 'Brak źródła'
            issue = record['773']['g'] if record['773'] and 'g' in record['773'] else 'Brak numeru'
            collaboration = record['245']['c'] if record['245'] and 'c' in record['245'] else 'Brak informacji o współtwórstwie'
            records.append({
                'ID Rekordu': record_id,
                'Autor': author,
                'Tytuł': title,
                'Infromacje dodatkowe': collaboration,
                'Miejsce wydania': place_of_publication,
                'Wydawca': publisher,
                'Rok wydania': pub_year,
                'Źródło': source,
                'Numer': issue               
            })
    return records

# Funkcja do zapisania danych do pliku XLSX przy użyciu pandas
def save_to_xlsx(records, output_file):
    df = pd.DataFrame(records)
    df.to_excel(output_file, index=False)
    
    
marc_file = 'latvian_translations_records.mrc'  # Podaj tutaj ścieżkę do swojego pliku MARC
output_file = 'lit łotewska bn.xlsx'

records = extract_marc_info(marc_file)
save_to_xlsx(records, output_file)

