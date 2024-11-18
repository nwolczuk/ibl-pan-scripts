import cx_Oracle
import pandas as pd
import numpy as np
from tqdm import tqdm
import hashlib
from dotenv import load_dotenv
import os

load_dotenv()

#%% znalezienie dzialow ktorych brakuje w strukturze

pbl_dzialy = pd.read_excel('/data/pbl_dzialy.xlsx')
mapowanie_dzialow = pd.read_excel('/data/Mapowanie działów.xlsx', sheet_name='Migracja działów')

pbl_dzialy = {row['DZ_DZIAL_ID']: row['DZ_NAZWA'] for index,row in pbl_dzialy.iterrows()}
mapowanie_dzialow = {row['Stary dział ID']: row['Dział w starej bazie (był)'] for index,row in mapowanie_dzialow.iterrows() if not isinstance(row['Dział w starej bazie (był)'], float)}

diff = {}

for key, value in pbl_dzialy.items():
    if key not in mapowanie_dzialow:
        diff[key] = value
        
out_df = pd.DataFrame.from_dict(diff, orient='index')

out_df.to_excel('/data/dzialy_roznica.xlsx')

#%% wyodrebnienie dzialow ktore maja rekordy i daty najwczesniej utworzonych rekordow

pbl_user = os.getenv('PBL_USER')
pbl_password = os.getenv('PBL_PASSWORD')

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

dzialy_roznica = pd.read_excel('/data/dzialy_roznica.xlsx')
dzialy_ids = set(dzialy_roznica['Unnamed: 0'])
dzialy_ids_str = '(' + ','.join([str(e) for e in dzialy_roznica['Unnamed: 0']]) + ')'

pbl_query = f"""select distinct z.za_dz_dzial1_id, z.za_dz_dzial2_id, z.za_zapis_id, z.za_uzytk_wpis_data
                from IBL_OWNER.pbl_zapisy z 
                where z.za_dz_dzial1_id in {dzialy_ids_str} 
                or z.za_dz_dzial2_id in {dzialy_ids_str}"""
query_result = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

ids_with_recs = set(query_result['ZA_DZ_DZIAL1_ID'].dropna()) | set(query_result['ZA_DZ_DZIAL2_ID'].dropna().reset_index(drop=True))

dz1 = {k:v['ZA_UZYTK_WPIS_DATA'].min() for k,v in query_result.groupby(['ZA_DZ_DZIAL1_ID'])}
dz2 = {k:v['ZA_UZYTK_WPIS_DATA'].min() for k,v in query_result.groupby(['ZA_DZ_DZIAL2_ID'])}

dzialy_with_rec_and_dates = {}
for dzial_id in ids_with_recs:
    dzial_id = int(dzial_id)
    if dzial_id in dzialy_ids:
        date1 = dz1.get(dzial_id)
        date2 = dz2.get(dzial_id)
        if date1 and date2:
            date = min([date1, date2])
        elif date1 and not date2:
            date = date1
        else:
            date = date2
        dzialy_with_rec_and_dates[dzial_id] = date   

out_df = pd.DataFrame.from_dict(dzialy_with_rec_and_dates, orient='index')

out_df.to_excel('/data/dzialy_recs_dates.xlsx')

#%% hash md5

# hashlib.md5('GeeksforGeeks'.encode('utf-8')).hexdigest()

def simplify_str(path):
    simplified_str = ''
    path_splitted = path.split('.')
    for idx, key in enumerate(path_splitted):
        if key:
            full_key = '.'.join(path_splitted[:idx+1] + [''])
            simplified_str += '/' + struktura_dz_dict[full_key].lower()
    return simplified_str

def get_md5_hash(string):
    return hashlib.md5(string.encode('utf-8')).hexdigest()


struktura_dzialow = pd.read_excel('/data/Mapowanie działów_15-12-2022.xlsx', sheet_name='SPUB_Nowa struktura działów')[['nr działu', 'nazwa działu', 'MD5']]
struktura_dz_dict = {row['nr działu']:row['nazwa działu'] for _, row in struktura_dzialow.iterrows()}

struktura_dzialow['simplified_str'] = struktura_dzialow['nr działu'].apply(lambda x: simplify_str(x))
struktura_dzialow['new_md5'] = struktura_dzialow['simplified_str'].apply(lambda x: get_md5_hash(x))
struktura_dzialow['md5_compare'] = struktura_dzialow[['MD5', 'new_md5']].apply(lambda x: str(x['MD5'] == x['new_md5']), axis=1)

struktura_dzialow.to_excel('/data/dzialy_new_md5.xlsx')








