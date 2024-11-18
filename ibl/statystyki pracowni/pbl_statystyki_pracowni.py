#%% modules
import cx_Oracle
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import os

load_dotenv()
user = os.getenv('USER')
password = os.getenv('PASSWORD')

#%% pbl oracle connection
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns, encoding='windows-1250')

#%%
end_date = '2024-04-15' # !!! ZMIANA DATY
start_date = '2020-09-15'

pbl_query = f"""select tab1.za_uzytk_wpisal Pracownik, tab1.zapisy, tab2.imprezy, tab3.filmy, tab4.sztuki 
from
(select z.za_uzytk_wpisal, count(*) as zapisy
 from IBL_OWNER.pbl_zapisy z
 where z.za_uzytk_wpis_data between TO_DATE('{start_date}','YYYY-MM-DD') and TO_DATE('{end_date}','YYYY-MM-DD')
 and z.za_type in ('IZA', 'PU')
 group by z.za_uzytk_wpisal) tab1
left join
(select z.za_uzytk_wpisal, count(*) as imprezy
 from IBL_OWNER.pbl_zapisy z
 where z.za_uzytk_wpis_data between TO_DATE('{start_date}','YYYY-MM-DD') and TO_DATE('{end_date}','YYYY-MM-DD')
 and z.za_type like 'IR'
 group by z.za_uzytk_wpisal) tab2
on tab1.za_uzytk_wpisal = tab2.za_uzytk_wpisal
left join
(select z.za_uzytk_wpisal, count(*) as filmy
 from IBL_OWNER.pbl_zapisy z
 where z.za_uzytk_wpis_data between TO_DATE('{start_date}','YYYY-MM-DD') and TO_DATE('{end_date}','YYYY-MM-DD')
 and z.za_type like 'FI'
 group by z.za_uzytk_wpisal) tab3
on tab1.za_uzytk_wpisal = tab3.za_uzytk_wpisal
left join
(select z.za_uzytk_wpisal, count(*) as sztuki
 from IBL_OWNER.pbl_zapisy z
 where z.za_uzytk_wpis_data between TO_DATE('{start_date}','YYYY-MM-DD') and TO_DATE('{end_date}','YYYY-MM-DD')
 and z.za_type like 'SZ'
 group by z.za_uzytk_wpisal) tab4
on tab1.za_uzytk_wpisal = tab4.za_uzytk_wpisal"""                

pbl_df = pd.read_sql(pbl_query, con=connection)

new_start_date = (datetime.fromisoformat(end_date) - relativedelta(months=1)).strftime('%Y-%m-%d')
pbl_miesieczne_zapisy_query = f"""select tab1.za_uzytk_wpisal Pracownik, tab1.zapisy 
from
(select z.za_uzytk_wpisal, count(*) as zapisy
 from IBL_OWNER.pbl_zapisy z
 where z.za_uzytk_wpis_data between TO_DATE('{new_start_date}','YYYY-MM-DD') and TO_DATE('{end_date}','YYYY-MM-DD')
 and z.za_type in ('IZA', 'PU')
 group by z.za_uzytk_wpisal) tab1"""

pbl_miesieczne_zapisy_df = pd.read_sql(pbl_miesieczne_zapisy_query, con=connection).rename(columns={'PRACOWNIK': 'Pracownik', 'ZAPISY': 'Miesięczna liczba rekordów z czasopism'.upper()}).set_index('Pracownik')

#%%
gc = gspread.oauth()
przypisane_czasopisma_sheet = gc.open_by_key('12zFWOJwn9Sac5F1Tb1vfX4DossOOa6HUb0hIsRYzTt8')
przypisane_czasopisma_df = get_as_dataframe(przypisane_czasopisma_sheet.worksheet('zrodla'), evaluate_formulas=True).dropna(0, how='all').dropna(1, how='all')

#%% poprzedni miesiac
previous_end_date = (datetime.fromisoformat(end_date) - relativedelta(months=1)).strftime('%Y-%m-%d')

previous_stats = pd.read_excel(f'./data/Statystyki_{start_date}_{previous_end_date}.xlsx')[['PRACOWNIK', 'ZAPISY']].rename(columns={'ZAPISY': 'POPRZEDNIE ZAPISY', 'PRACOWNIK': 'Pracownik'})

#%%

szacunek = przypisane_czasopisma_df.groupby(['Pracownik']).sum()['Szacunek'].to_frame(name='PRZYDZIELONA LICZBA ZAPISÓW (SZACUNKOWA)')
przypisane_roczniki = przypisane_czasopisma_df.groupby(['Pracownik']).size().to_frame(name='PRZYDZIELONE ROCZNIKI')
karty_ewidencyjne = przypisane_czasopisma_df.groupby(['Pracownik'])['Karta ewidencyjna'].count().to_frame(name='OPRACOWANE ROCZNIKI (KARTY EWIDENCYJNE)')

# czy dodac pbl_miesieczne_zapisy_df?
for series in [szacunek, przypisane_roczniki, karty_ewidencyjne, previous_stats]:
    pbl_df = pbl_df.merge(series, how='left', left_on='PRACOWNIK', right_on='Pracownik')

pbl_df.loc['suma'] = pbl_df.sum(numeric_only=True)
pbl_df['% ZREALIZOWANEJ LICZBY ZAPISÓW'] = pbl_df['ZAPISY']/pbl_df['PRZYDZIELONA LICZBA ZAPISÓW (SZACUNKOWA)']*100
pbl_df['% OPRACOWANYCH ROCZNIKÓW'] = pbl_df['OPRACOWANE ROCZNIKI (KARTY EWIDENCYJNE)']/pbl_df['PRZYDZIELONE ROCZNIKI']*100
pbl_df['LICZBA ZAPISÓW W MIESIĄCU'] = pbl_df['ZAPISY'] - pbl_df['POPRZEDNIE ZAPISY']
pbl_df.at['suma', 'PRACOWNIK'] = 'suma'

pbl_df = pbl_df[['PRACOWNIK', 'ZAPISY', 'PRZYDZIELONA LICZBA ZAPISÓW (SZACUNKOWA)', '% ZREALIZOWANEJ LICZBY ZAPISÓW', 'PRZYDZIELONE ROCZNIKI', 'OPRACOWANE ROCZNIKI (KARTY EWIDENCYJNE)','% OPRACOWANYCH ROCZNIKÓW', 'IMPREZY', 'FILMY', 'SZTUKI', 'LICZBA ZAPISÓW W MIESIĄCU']].round(2)

pbl_df.to_excel(f'./data/Statystyki_{start_date}_{end_date}.xlsx', index=False)


