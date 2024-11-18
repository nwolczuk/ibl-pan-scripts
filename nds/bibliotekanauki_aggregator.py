import requests
from sickle import Sickle

from dotenv import load_dotenv

from pdf_txt_extractor import extract_txt_from_pdf_binary


def get_pdf_txt(pdf_url):
    resp = requests.get(pdf_url)
    if resp.ok:
        pdf_txt = extract_txt_from_pdf_binary(resp.content)
        return pdf_txt
    else:
        return ''

def create_id(input_str):
    output_str = ''
    for char in input_str:
        if char.isalnum():
            output_str += char
    return output_str


if __name__ == '__main__':
    
    load_dotenv()
    
    oai_url = 'https://bibliotekanauki.pl/api/oai/articles'
    
    sickle = Sickle(oai_url)
    
    arguments = {
                    'metadataPrefix': 'oai_dc',
                    'from': '2022-01-01',
                    'ignore_deleted': True
                }
    
    records = sickle.ListRecords(**arguments)
    
    for record in records:
        # print("Identifier:", record.header.identifier)
        # print("Datestamp:", record.header.datestamp)
        entry_id = create_id(record.header.identifier)
        record.metadata
        pdf_url = record.metadata['relation'][0]
        pdf_txt = get_pdf_txt(pdf_url)
        # print("Metadata:", record.metadata)
        break
