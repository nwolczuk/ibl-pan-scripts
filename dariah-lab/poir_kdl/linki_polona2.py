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
def newest_file(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)


df = pd.read_excel('polona2_PCL.xlsx')

errors = []


options = Options()
options.set_preference("browser.download.dir", "./polona_pdfs2")
driver = webdriver.Firefox(options=options)
last_newest_file = 'C:/Users/Nikodem/Downloads/Mattermost-Instructions_OPERAS-AISBL-Server_CRAFT-OA-_2_.pdf'
downloads_path = "C:/Users/Nikodem/Downloads/"

for idx,row in tqdm(df.iterrows()):
    identifier = row['identifier']
    url = row['polona_link']
    
    try:
        driver.get(url)
        # //*[@id="components.bn-card-buttons.button.download"]
        download_elem = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="components.bn-card-buttons.button.download"]')))
        download_elem.click()
        
        try:
            full_pdf_radio_xpath = '/html/body/ngb-modal-window/div/div/bn-download-object-modal/bn-modal/div/div/div/div/div[2]/form/div/bn-radio[2]/div/div/div/div/fieldset/label'
            full_pdf_radio = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,full_pdf_radio_xpath)))
        except:
            errors.append((identifier, url))
            continue
            # try:
            #     full_pdf_radio_xpath = '/html/body/ngb-modal-window/div/div/bn-download-object-modal/bn-modal/div/div/div/div/div[2]/form/div/bn-radio[1]/div/div/div/div[1]/fieldset/label'
            #     full_pdf_radio = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, full_pdf_radio_xpath)))
            # except:
            #     errors.append((identifier, url))
            #     continue
            
        full_pdf_radio.click()
        
        # //*[@id="components.bn-button.download"]
        download_pdf = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="components.bn-button.download"]')))
        
        download_pdf.click()
        count = 0
        while newest_file(downloads_path) == last_newest_file:
            time.sleep(5)
            count += 1
            if count == 12:
                break
        else:
            old_name = newest_file(downloads_path)
            new_name = '/'.join(old_name.split('/')[:-1]) + '/' + identifier + '.pdf'
            os.rename(old_name, new_name)
            last_newest_file = new_name
            
    except KeyboardInterrupt:
        raise Exception()
    except: 
        errors.append((identifier, url))
        continue        

driver.close()
