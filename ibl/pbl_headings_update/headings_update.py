import pandas as pd
import hashlib
from tqdm import tqdm

#%%nr działu	nazwa działu	simplified_str	new_md5	update


headings_df = pd.read_excel('/data/new_pbl_dzialy_md5.xlsx').fillna('')[['nr działu', 'nazwa działu', 'simplified_str', 'update']]
to_update = pd.read_excel('/data/to_update.xlsx').fillna('')

#%%
new_headings = []
for _, row in tqdm(headings_df.iterrows()):
    if row['update']:
        new_headings.append([row['nr działu'], row['nazwa działu'], row['simplified_str']]) 
        heading_int1, heading_int2 = row['nr działu'].split('.')[:2]
        literature_str = row['nazwa działu'].lower()
        for _, updt_row in to_update.iterrows():
            updt_head_int = updt_row['to_update_int'].replace('x1', heading_int1).replace('x2', heading_int2)
            updt_head_name = updt_row['to_update_str']
            updt_head_path = updt_row['to_update_path'].replace('literature_str', literature_str)
            new_headings.append([updt_head_int, updt_head_name, updt_head_path]) 
    else:
        new_headings.append([row['nr działu'], row['nazwa działu'], row['simplified_str']])        

for elem in tqdm(new_headings):
    heading_hash = hashlib.md5(elem[-1].encode()).hexdigest()
    elem.append(heading_hash)

out_df = pd.DataFrame(new_headings, columns=['nr działu', 'nazwa działu', 'simplified_str', 'new_md5'])
out_df.to_excel('/data/updated_headings.xlsx', index=False)



# updt_head_hash = hashlib.md5(updt_head_path.encode()).hexdigest()