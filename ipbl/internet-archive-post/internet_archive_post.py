#%% imports
import os
import sys
import signal
import argparse

import requests
import pandas as pd
import random
import time
from tqdm import tqdm
from dotenv import load_dotenv

import gspread as gs
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_dataframe import set_with_dataframe, get_as_dataframe

from concurrent.futures import ThreadPoolExecutor
from threading import Event


#%% defs
def gdrive_auth():
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile("client_secrets.json")
    gauth.LoadCredentialsFile("credentials.json")
    if not gauth.credentials:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
    gauth.SaveCredentialsFile("credentials.json")
    return gauth


def signal_handler(sig, frame):
    print('\nPrzerwanie przez Ctrl + C\nOczekiwanie na zakończenie pracy wątków\n')
    stop_event.set()
    sys.exit(0)


def gsheet_to_df(gsheetId, worksheet):
    gc = gs.oauth()
    sheet = gc.open_by_key(gsheetId)
    df = get_as_dataframe(sheet.worksheet(worksheet), evaluate_formulas=True, dtype=str).dropna(how='all').fillna('')
    return df


def start_job(target_url, cred_idx = 1):
    myaccesskey = os.getenv(f'ACCESS_KEY_{cred_idx}')
    mysecret = os.getenv(f'SECRET_KEY_{cred_idx}')
    response = requests.post(
            url = 'https://web.archive.org/save',
            headers = {
                'Accept': 'application/json',
                'Authorization': f'LOW {myaccesskey}:{mysecret}'
                },
            data = {'url': target_url,
                    'capture_outlinks': 0, 
                    'skip_first_archive': 1}
        )
    return response

def check_status(job_id, cred_idx = 1):
    myaccesskey = os.getenv(f'ACCESS_KEY_{cred_idx}')
    mysecret = os.getenv(f'SECRET_KEY_{cred_idx}')
    response = requests.get(
            url = 'https://web.archive.org/save/status/' + job_id,
            headers = {
                'Accept': 'application/json',
                'Authorization': f'LOW {myaccesskey}:{mysecret}'
                }
        )
    return response


def archive_target(target_url):
    start_response = start_job(target_url)
    if not start_response.ok:
        print(f'\nError, archiving not started -- target url: {target_url}')
        return 'error', 'Archiving not started'
    job_id = start_response.json().get('job_id')
    print(f'\nArchiving started -- target url: {target_url}')
    while True:
        time.sleep(random.randint(90, 120))
        try:
            status_response = check_status(job_id)
        except requests.exceptions.ConnectionError as error:
            print(error)
            continue
        if status_response.json()['status'] == 'success':
            print(f'\nSuccess! -- target url: {target_url}')
            return 'success', f'https://web.archive.org/web/{status_response.json()["timestamp"]}/{status_response.json()["original_url"]}'
        elif status_response.json()['status'] == 'error':
            print(f'\nError -- target url: {target_url}')
            return 'error', status_response.json()['message']


def update_links_to_archive(gauth):
    gc = gs.oauth()
    drive = GoogleDrive(gauth)
    
    archive_status = gsheet_to_df('1OucvYycLyrcM_qHBoyAQcqE38B5NisPR403IfGEIuiE', 'status')
    links_in_archive = set(archive_status['target_url'])
    
    file_list = drive.ListFile({'q': "'19t1szTXTCczteiKfF2ukYsuiWpDqyo8f' in parents and trashed=false"}).GetList()
    file_list = [(e['title'], e['id']) for e in file_list if e['title'].endswith('.xlsx')]
    new_links = {}
    for file in tqdm(file_list):
        gfile = drive.CreateFile({'id': file[1]})
        gfile.GetContentFile('temp')
        df = pd.read_excel('temp')
        new_links[file[0][:-5]] = set(df['Link'])
    links_to_add = []
    for key, value in new_links.items():
        to_add = value.difference(links_in_archive)
        for link in to_add:
            links_to_add.append((key, link))
    if links_to_add:
        file_names, target_urls = zip(*links_to_add)
        archive_status = pd.concat([archive_status, pd.DataFrame({'file_name': file_names, 'target_url': target_urls})], ignore_index=True)
        status_sheet = gc.open_by_key('1OucvYycLyrcM_qHBoyAQcqE38B5NisPR403IfGEIuiE')
        set_with_dataframe(status_sheet.worksheet('status'), archive_status)
    print('\nLinks to archive updated.\n')
    return


def archive_links():
    gc = gs.oauth()
    
    archive_status_df = gsheet_to_df('1OucvYycLyrcM_qHBoyAQcqE38B5NisPR403IfGEIuiE', 'status')
    archive_status_df = archive_status_df[archive_status_df['internet_archive_url'] == '']
    archive_status_df = archive_status_df.sample(frac=1)
    
    status_sheet = gc.open_by_key('1OucvYycLyrcM_qHBoyAQcqE38B5NisPR403IfGEIuiE')
    status_worksheet = status_sheet.worksheet('status')
    
    def process_row(row_tuple):
        if stop_event.is_set():
            return
        idx, row = row_tuple
        if not row['status']:
            status, data = archive_target(row['target_url'])
            row['status'] = status
            if status == 'error':
                row['message'] = data
            else:
                row['internet_archive_url'] = data
                row['message'] = ''
            status_worksheet.update(range_name=f'A{idx+2}:E{idx+2}', values=[list(row)])
        return
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        list(tqdm(executor.map(process_row, archive_status_df.iterrows()), total=len(archive_status_df)))
    return


#%% main
if __name__ == "__main__":
    stop_event = Event()
    signal.signal(signal.SIGINT, signal_handler)
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-u', '--update-links',
        dest='update',
        action='store_true',
        help='Update links to archive'
    )
    args = parser.parse_args()
    
    load_dotenv()
    gauth = gdrive_auth()
    
    if args.update:
        update_links_to_archive(gauth)
    
    while not stop_event.is_set():
        try:
            archive_links()
        except (KeyError, requests.exceptions.ConnectionError, requests.exceptions.JSONDecodeError) as error:
            print(error)
            continue
