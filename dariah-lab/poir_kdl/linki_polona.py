import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import time
import requests
from tqdm import tqdm
import random
import os
from bs4 import BeautifulSoup

#%%

polona_links = pd.read_excel('kdl_polona.xlsx', sheet_name='linki do Polony')
polona_links = [row['linki do Polony'] for _,row in polona_links.iterrows() if not isinstance(row['linki do Polony'],float)]


#%%
options = Options()
options.set_preference("browser.download.dir", "./polona_pdfs")
driver = webdriver.Firefox(options=options)

download_links = []

for url in tqdm(polona_links):
    driver.get(url)
    time.sleep(3)
    elem = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "download-list-toggle")))
    elem.click()

    try:
        elem = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'PDF')]")))
        elem.click()
    except:
        download_links.append((url, '', ''))
        continue
    
    elem = driver.find_elements(By.CLASS_NAME, "click-marker")
    if len(elem) > 0:
        elem = elem[0]
    pdf_href = elem.get_attribute('href')
    
    # metadata
    driver.get('https://www.google.com')
    driver.get(url)
    time.sleep(3)
    elem = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "download-list-toggle")))
    elem.click()
    
    elem = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'XML')]")))
    if elem:
        elem.click()
    else:
        download_links.append((url, pdf_href, ''))
        continue
    
    elem = driver.find_elements(By.CLASS_NAME, "click-marker")
    if len(elem) > 0:
        elem = elem[0]
    xml_href = elem.get_attribute('href')
    
    download_links.append((url, pdf_href, xml_href))
    
driver.close()

df = pd.DataFrame(download_links, columns=['polona_link', 'pdf_link', 'xml_link'])
df.to_excel('polona_with_pdf_links.xlsx', index=False)


    
#%%
df = pd.read_excel('polona_with_pdf_links.xlsx').fillna('')

output = []
for idx,row in tqdm(df.iterrows()):
    identifer = 'polona' + str(random.randint(10000000, 99999999))
    pdf_url = row['pdf_link']
    xml_url = row['xml_link']
    
    if pdf_url and xml_url:
        pdf_resp = requests.get(pdf_url)
        xml_resp = requests.get(xml_url)
        
        with open(f'./polona_pdfs/{identifer}.pdf', 'wb') as pdf:
            pdf.write(pdf_resp.content)
            
        with open(f'./polona_xmls/{identifer}.xml', 'wb') as xml:
            xml.write(xml_resp.content)
            
    output.append((identifer, row['polona_link'], pdf_url, xml_url))

# https://polona2.pl/archive?uid=65920985&cid=66169022
resp = requests.get('https://polona2.pl/archive?uid=65920985&cid=66169022')

with open('./polona_pdfs/test_pdf.pdf', 'wb') as pdf:
    pdf.write(resp.content)
    
#%% prepare metadata

output = []
for xml in tqdm(os.listdir('polona_xmls')):
    xml_metadata_dict = {}
    with open(f'polona_xmls/{xml}', 'r', encoding='utf-8') as xf:
        xml_str = ' '.join([e.strip() for e in xf.readlines()])
    soup = BeautifulSoup(xml_str, 'xml')
    tags = soup.find('Description').find_all()
    for tag in tags:
        tag_name = tag.name
        tag_value = tag.text
        xml_metadata_dict.setdefault(tag_name, []).append(tag_value)
    xml_metadata_dict['identifier'] = [xml[:-4]]
    output.append(xml_metadata_dict)
    
for record in output:
    for key, value in record.items():
        record[key] = ' | '.join(record[key])
        
out_df = pd.DataFrame(output)
out_df.to_excel('polona_metadata.xlsx', index=False)
       
#%% manual metadata

output = []
for xml in tqdm(os.listdir('polona_manual/xmls')):
    xml_metadata_dict = {}
    with open(f'polona_manual/xmls/{xml}', 'r', encoding='utf-8') as xf:
        xml_str = ' '.join([e.strip() for e in xf.readlines()])
    soup = BeautifulSoup(xml_str, 'xml')
    tags = soup.find('metadata').find_all()
    for tag in tags:
        tag_name = tag.name
        tag_value = tag.text
        xml_metadata_dict.setdefault(tag_name, []).append(tag_value)
    xml_metadata_dict['identifier'] = [xml[:-4]]
    output.append(xml_metadata_dict)
    
for record in output:
    for key, value in record.items():
        record[key] = ' | '.join(record[key])
        
out_df = pd.DataFrame(output)
out_df.to_excel('polona_manual_metadata.xlsx', index=False)
