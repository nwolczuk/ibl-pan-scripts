#%% modules
import cx_Oracle
import pandas as pd
from dotenv import load_dotenv
import os
from tqdm import tqdm

load_dotenv()
user = os.getenv('USER')
password = os.getenv('PASSWORD')

#%% pbl oracle connection
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns, encoding='windows-1250')

#%%
sources_df = pd.read_excel('pbl_zrodla.xlsx')

issues_df = pd.DataFrame()
for idx, row in tqdm(sources_df.iterrows()):
    source_id = row['ZR_ZRODLO_ID']
    query = f"""
                select zrr_zr_zrodlo_id, zrr_rocznik, zrr_numery, zrr_podtytul, zrr_rzr_symbol from
                (select * from IBL_OWNER.pbl_zrodla_roczniki zrr
                where zrr.zrr_zr_zrodlo_id = {source_id}
                order by zrr.zrr_rocznik desc)
                where ROWNUM = 1
            """
    pbl_df = pd.read_sql(query, con=connection)
    issues_df = pd.concat([issues_df, pbl_df], ignore_index=True)

output_df = pd.merge(sources_df, issues_df, how='left', left_on='ZR_ZRODLO_ID', right_on='ZRR_ZR_ZRODLO_ID')

output_df.to_excel('pbl_zrodla_wzbogacone.xlsx')
