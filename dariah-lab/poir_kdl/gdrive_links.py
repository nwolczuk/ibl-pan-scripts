#%%
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

import pandas as pd
import regex as re
from tqdm import tqdm

#%% goofgle drvie authenticatin
gauth = GoogleAuth()
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)


def list_files_in_folder(parent_directory_id):
    file_list = drive.ListFile({'q': "'"+parent_directory_id+"' in parents and trashed=false"}).GetList()
    output = []
    for file in file_list:
        output.append((file['title'], file['id']))
    return output

def search_file(filename, exact_search=True):
    if exact_search:
        file_list = drive.ListFile({'q': f"title='{filename}' and trashed=false"}).GetList()
    else:
        file_list = drive.ListFile({'q': f"title contains '{filename}' and trashed=false"}).GetList()
    output = []
    for file in file_list:
        output.append((file['title'], file['id'], file['mimeType']))
    return output

def get_file(filename, exact_search=True):
    if exact_search:
        file_list = drive.ListFile({'q': f"title='{filename}' and trashed=false"}).GetList()
    else:
        file_list = drive.ListFile({'q': f"title contains '{filename}' and trashed=false"}).GetList()
    return file_list

#%% load identifiers

identifiers = pd.read_excel('polona_identifiers.xlsx')
identifiers = identifiers.fillna('')

#%%
output = []
last_index = -1
for idx,row in tqdm(identifiers.iterrows()):
    if idx < last_index:
        continue
    filename = row['identifier']
    file_link = row['link']
    if filename:
        search_response = search_file(row['identifier'], exact_search=False)
        if search_response:
            file_id = search_response[0][1]
            file_link = 'https://drive.google.com/file/d/' + file_id
        else:
            file_link = ''
    else:
        file_id = row['link'].split('/')[-1]
        gdrive_file = drive.CreateFile({'id': file_id})
        gdrive_file.FetchMetadata()
        filename = re.sub('\.[^\.]+?$', '', gdrive_file['title'])
    output.append((row['worksheet'], filename, file_link))

output_df = pd.DataFrame(output, columns=['worksheet', 'identifier', 'link'])
output_df.to_excel('polona_identifiers_with_links.xlsx', index=False)

#%% delete unnecessary files

with open('czasopisma_bad_ids.txt', 'r', encoding='utf-8') as txt:
    bad_ids = set(txt.readlines())

used_ids = set()
for identifier in tqdm(bad_ids):
    if identifier not in used_ids:
        search_response = get_file(identifier, exact_search=False)
        if len(search_response) == 1:
            file = search_response[0]
            file.Delete()
            used_ids.add(identifier)
        else:
            print(identifier)
            used_ids.add(identifier)


