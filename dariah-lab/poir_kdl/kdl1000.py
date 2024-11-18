import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from tqdm import tqdm

#%% get identifiers
kdl1000_df = pd.read_excel('kdl1000_metadata.xlsx').fillna('')

kdl1000_ids = dict(zip(kdl1000_df['identifier'], kdl1000_df['link']))

#%%
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

kdl1000_items = [(k,v) for k,v in kdl1000_ids.items()]

errors = []
for idx, tup in tqdm(enumerate(kdl1000_items)):
    name, url = tup
    try:
        url = url.replace('/view', '').replace('/edit', '')
        file_id = url.split('/')[-1]
        file = drive.CreateFile({'id': file_id})
        if file['mimeType'] == 'application/pdf':
            ext = 'pdf'
        else:
            ext = 'docx'
        file.GetContentFile('kdl1000/' + f'{name}.{ext}')
    except KeyboardInterrupt:
        break
    except:
        errors.append((name, url))

errors2 = []
for tup in tqdm(errors[92:]):
    url = tup[1]
    name = tup[0]
    try:
        url = url.replace('/view', '').replace('/edit', '')
        file_id = url.split('/')[-1]
        file = drive.CreateFile({'id': file_id})
        if file['mimeType'] == 'application/pdf':
            ext = 'pdf'
        else:
            ext = 'docx'
        file.GetContentFile('kdl1000/' + f'{name}.{ext}')
    except KeyboardInterrupt:
        break
    except:
        errors2.append((name, url))
        
errors3 = []
for tup in tqdm(errors2):
    url = tup[1]
    name = tup[0]
    try:
        url = url.replace('/view', '').replace('/edit', '')
        file_id = url.split('/')[-1]
        file = drive.CreateFile({'id': file_id})
        if file['mimeType'] == 'application/pdf':
            ext = 'pdf'
        else:
            ext = 'docx'
        file.GetContentFile('kdl1000/' + f'{name}.{ext}')
    except KeyboardInterrupt:
        break
    except:
        errors3.append((name, url))