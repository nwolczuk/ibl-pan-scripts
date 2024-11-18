import pandas as pd
import cx_Oracle
import gspread as gs
from gspread_dataframe import set_with_dataframe
from tqdm import tqdm
import time
from dotenv import load_dotenv
import os

load_dotenv()
user = os.getenv('USER')
password = os.getenv('PASSWORD')


#%% pbl oracle connection
pd.options.display.float_format = '{:.0f}'.format

# cx_Oracle.init_oracle_client(r'C:\ORANT\BIN\instantclient_19_11')
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns, encoding='windows-1250')

#%% set authors and date
folder_pracownika_id = input('ID folderu pracownika: ')
lista_path = input('Ścieżka do listy: ')
dict_of_authors = {}
authors_df = pd.read_excel(lista_path)
pracownik = authors_df['osoba opracowująca'][0]

for index, row in authors_df.iterrows():
    dict_of_authors['auth'+str(index)] = {'name': row['Imię pisarza/pisarki'], 'surname': row['Nazwisko pisarza/pisarki'], 'date': None}
    if isinstance(row['Data wpisania rekordu do bazy OD'], str):
        dict_of_authors['auth'+str(index)]['date'] = '1988-01-01'
    else: dict_of_authors['auth'+str(index)]['date'] = row['Data wpisania rekordu do bazy OD']

#%%

for auth, value in tqdm(dict_of_authors.items()):
    name = value['name'].strip()
    surname = value['surname'].strip()
    date = value['date']
    
    # get ids
    tworca_query = f"""
                    select * 
                    from IBL_OWNER.pbl_tworcy tw
                    where tw.tw_nazwisko like '{surname}' and tw.tw_imie like '{name}'
                    """
    autor_query = f"""
                    select * 
                    from IBL_OWNER.pbl_autorzy am
                    where am.am_nazwisko like '{surname}' and am.am_imie like '{name}'
                    """
    
    osoba_query =  f"""
                    select * 
                    from IBL_OWNER.pbl_osoby os
                    where os.os_nazwisko like '{surname}' and os.os_imie like '{name}'
                    """
    try:
        tworca_id = pd.read_sql(tworca_query, connection)['TW_TWORCA_ID'][0]
    except IndexError: tworca_id = None
    
    try:
        autor_id = pd.read_sql(autor_query, connection)['AM_AUTOR_ID'][0]
    except IndexError: autor_id = None   
    
    try:
        osoba_id = pd.read_sql(osoba_query, connection)['OS_OSOBA_ID'][0]
    except IndexError: osoba_id = None
    
    # podmiotowa
    if autor_id:
        podmiotowa_query =  f"""
                            select distinct cz1.za_zapis_id zapis_id, cz1.rz_nazwa rodzaj, cz1.za_opis_odwolania odwolanie, cz1.za_za_zapis_id odwolanie_id, cz1.za_ro_rok rok, cz1.za_tytul tytul, cz1.za_tytul_oryginalu tytul_oryginalu, cz1.za_wydanie wydanie, cz1.wy_nazwa wydawnictwo, cz1.wy_miasto miejsce_wydania, cz1.za_rok_wydania rok_wydania, cz1.za_tomy tomy, cz1.za_opis_fizyczny_ksiazki opis_fizyczny, cz1.za_seria_wydawnicza seria_wydawnicza, cz1.za_adnotacje adnotacje1, cz1.za_adnotacje2 adnotacje2, cz1.za_adnotacje3 adnotacje3, cz1.za_opis_wspoltworcow wspoltworcy, cz1.zr_tytul zrodlo, cz1.za_zrodlo_rok zrodlo_rok, cz1.za_zrodlo_nr numer, cz1.za_zrodlo_str strony,  aut2.am_imie imie_autora, aut2.am_nazwisko nazwisko_autora, cz1.tw_imie imie_tworcy, cz1.tw_nazwisko nazwisko_tworcy, cz1.za_uzytk_wpis_data
                            from
                            (select *
                            from IBL_OWNER.pbl_zapisy z
                            left join IBL_OWNER.pbl_zapisy_tworcy ztw on z.za_zapis_id = ztw.zatw_za_zapis_id
                            left join IBL_OWNER.pbl_zapisy_autorzy zau on z.za_zapis_id = zau.zaam_za_zapis_id
                            left join IBL_OWNER.pbl_udzialy_osob uo on z.za_zapis_id = uo.uo_za_zapis_id
                            left join IBL_OWNER.pbl_rodzaje_zapisow rzz on z.za_rz_rodzaj1_id = rzz.rz_rodzaj_id
                            left join IBL_OWNER.pbl_zapisy_wydawnictwa zawy on z.za_zapis_id = zawy.zawy_za_zapis_id
                            left join IBL_OWNER.pbl_wydawnictwa wyd on zawy.zawy_wy_wydawnictwo_id = wyd.wy_wydawnictwo_id
                            left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
                            left join IBL_OWNER.pbl_tworcy tw on ztw.zatw_tw_tworca_id = tw.tw_tworca_id
                            where z.za_proweniencja is null
                            and z.za_status_imp is null
                            and zau.zaam_am_autor_id = {autor_id}) cz1
                            left join IBL_OWNER.pbl_zapisy_autorzy zau2 on cz1.za_zapis_id = zau2.zaam_za_zapis_id
                            left join IBL_OWNER.pbl_autorzy aut2 on zau2.zaam_am_autor_id = aut2.am_autor_id                   
                            """
        
        podmiotowa_response = pd.read_sql(podmiotowa_query, connection)             
        podmiotowa_response = podmiotowa_response[podmiotowa_response['ZA_UZYTK_WPIS_DATA'] > date]
    else: podmiotowa_response = pd.DataFrame(columns=['ZAPIS_ID', 'RODZAJ', 'ODWOLANIE', 'ODWOLANIE_ID', 'ROK', 'TYTUL', 'TYTUL_ORYGINALU', 'WYDANIE', 'WYDAWNICTWO', 'MIEJSCE_WYDANIA', 'ROK_WYDANIA', 'TOMY', 'OPIS_FIZYCZNY', 'SERIA_WYDAWNICZA', 'ADNOTACJE1', 'ADNOTACJE2', 'ADNOTACJE3', 'WSPOLTWORCY', 'ZRODLO', 'ZRODLO_ROK', 'NUMER', 'STRONY', 'IMIE_AUTORA', 'NAZWISKO_AUTORA', 'IMIE_TWORCY', 'NAZWISKO_TWORCY', 'ZA_UZYTK_WPIS_DATA'])
    
    # przedmiotowa
    if tworca_id:
        przedmiotowa_query =  f"""
                            select distinct cz1.za_zapis_id zapis_id, cz1.rz_nazwa rodzaj, cz1.za_opis_odwolania odwolanie, cz1.za_za_zapis_id odwolanie_id, cz1.za_ro_rok rok, cz1.za_tytul tytul, cz1.za_tytul_oryginalu tytul_oryginalu, cz1.za_wydanie wydanie, cz1.wy_nazwa wydawnictwo, cz1.wy_miasto miejsce_wydania, cz1.za_rok_wydania rok_wydania, cz1.za_tomy tomy, cz1.za_opis_fizyczny_ksiazki opis_fizyczny, cz1.za_seria_wydawnicza seria_wydawnicza, cz1.za_adnotacje adnotacje1, cz1.za_adnotacje2 adnotacje2, cz1.za_adnotacje3 adnotacje3, cz1.za_opis_wspoltworcow wspoltworcy, cz1.zr_tytul zrodlo, cz1.za_zrodlo_rok zrodlo_rok, cz1.za_zrodlo_nr numer, cz1.za_zrodlo_str strony,  aut2.am_imie imie_autora, aut2.am_nazwisko nazwisko_autora, cz1.tw_imie imie_tworcy, cz1.tw_nazwisko nazwisko_tworcy, cz1.za_uzytk_wpis_data
                            from
                            (select *
                            from IBL_OWNER.pbl_zapisy z
                            left join IBL_OWNER.pbl_zapisy_tworcy ztw on z.za_zapis_id = ztw.zatw_za_zapis_id
                            left join IBL_OWNER.pbl_zapisy_autorzy zau on z.za_zapis_id = zau.zaam_za_zapis_id
                            left join IBL_OWNER.pbl_udzialy_osob uo on z.za_zapis_id = uo.uo_za_zapis_id
                            left join IBL_OWNER.pbl_rodzaje_zapisow rzz on z.za_rz_rodzaj1_id = rzz.rz_rodzaj_id
                            left join IBL_OWNER.pbl_zapisy_wydawnictwa zawy on z.za_zapis_id = zawy.zawy_za_zapis_id
                            left join IBL_OWNER.pbl_wydawnictwa wyd on zawy.zawy_wy_wydawnictwo_id = wyd.wy_wydawnictwo_id
                            left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
                            left join IBL_OWNER.pbl_tworcy tw on ztw.zatw_tw_tworca_id = tw.tw_tworca_id
                            where z.za_proweniencja is null
                            and z.za_status_imp is null
                            and ztw.zatw_tw_tworca_id = {tworca_id}) cz1
                            left join IBL_OWNER.pbl_zapisy_autorzy zau2 on cz1.za_zapis_id = zau2.zaam_za_zapis_id
                            left join IBL_OWNER.pbl_autorzy aut2 on zau2.zaam_am_autor_id = aut2.am_autor_id                   
                            """
        
        przedmiotowa_response = pd.read_sql(przedmiotowa_query, connection)
        przedmiotowa_response = przedmiotowa_response[przedmiotowa_response['ZA_UZYTK_WPIS_DATA'] > date]
        przedmiotowa_response = przedmiotowa_response[~przedmiotowa_response['ZAPIS_ID'].isin(podmiotowa_response['ZAPIS_ID'])]
    else: przedmiotowa_response = pd.DataFrame(columns=['ZAPIS_ID', 'RODZAJ', 'ODWOLANIE', 'ODWOLANIE_ID', 'ROK', 'TYTUL', 'TYTUL_ORYGINALU', 'WYDANIE', 'WYDAWNICTWO', 'MIEJSCE_WYDANIA', 'ROK_WYDANIA', 'TOMY', 'OPIS_FIZYCZNY', 'SERIA_WYDAWNICZA', 'ADNOTACJE1', 'ADNOTACJE2', 'ADNOTACJE3', 'WSPOLTWORCY', 'ZRODLO', 'ZRODLO_ROK', 'NUMER', 'STRONY', 'IMIE_AUTORA', 'NAZWISKO_AUTORA', 'IMIE_TWORCY', 'NAZWISKO_TWORCY', 'ZA_UZYTK_WPIS_DATA'])
    
    # wspoltworca
    if osoba_id:
        wspoltworca_query =  f"""
                            select distinct cz1.za_zapis_id zapis_id, cz1.rz_nazwa rodzaj, cz1.za_opis_odwolania odwolanie, cz1.za_za_zapis_id odwolanie_id, cz1.za_ro_rok rok, cz1.za_tytul tytul, cz1.za_tytul_oryginalu tytul_oryginalu, cz1.za_wydanie wydanie, cz1.wy_nazwa wydawnictwo, cz1.wy_miasto miejsce_wydania, cz1.za_rok_wydania rok_wydania, cz1.za_tomy tomy, cz1.za_opis_fizyczny_ksiazki opis_fizyczny, cz1.za_seria_wydawnicza seria_wydawnicza, cz1.za_adnotacje adnotacje1, cz1.za_adnotacje2 adnotacje2, cz1.za_adnotacje3 adnotacje3, cz1.za_opis_wspoltworcow wspoltworcy, cz1.zr_tytul zrodlo, cz1.za_zrodlo_rok zrodlo_rok, cz1.za_zrodlo_nr numer, cz1.za_zrodlo_str strony,  aut2.am_imie imie_autora, aut2.am_nazwisko nazwisko_autora, cz1.tw_imie imie_tworcy, cz1.tw_nazwisko nazwisko_tworcy, cz1.za_uzytk_wpis_data
                            from
                            (select *
                            from IBL_OWNER.pbl_zapisy z
                            left join IBL_OWNER.pbl_zapisy_tworcy ztw on z.za_zapis_id = ztw.zatw_za_zapis_id
                            left join IBL_OWNER.pbl_zapisy_autorzy zau on z.za_zapis_id = zau.zaam_za_zapis_id
                            left join IBL_OWNER.pbl_udzialy_osob uo on z.za_zapis_id = uo.uo_za_zapis_id
                            left join IBL_OWNER.pbl_rodzaje_zapisow rzz on z.za_rz_rodzaj1_id = rzz.rz_rodzaj_id
                            left join IBL_OWNER.pbl_zapisy_wydawnictwa zawy on z.za_zapis_id = zawy.zawy_za_zapis_id
                            left join IBL_OWNER.pbl_wydawnictwa wyd on zawy.zawy_wy_wydawnictwo_id = wyd.wy_wydawnictwo_id
                            left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
                            left join IBL_OWNER.pbl_tworcy tw on ztw.zatw_tw_tworca_id = tw.tw_tworca_id
                            where z.za_proweniencja is null
                            and z.za_status_imp is null
                            and uo.uo_os_osoba_id = {osoba_id}) cz1
                            left join IBL_OWNER.pbl_zapisy_autorzy zau2 on cz1.za_zapis_id = zau2.zaam_za_zapis_id
                            left join IBL_OWNER.pbl_autorzy aut2 on zau2.zaam_am_autor_id = aut2.am_autor_id                   
                            """
        
        wspoltworca_response = pd.read_sql(wspoltworca_query, connection)
        wspoltworca_response = wspoltworca_response[wspoltworca_response['ZA_UZYTK_WPIS_DATA'] > date]
        wspoltworca_response = wspoltworca_response[~wspoltworca_response['ZAPIS_ID'].isin(podmiotowa_response['ZAPIS_ID'])]
        wspoltworca_response = wspoltworca_response[~wspoltworca_response['ZAPIS_ID'].isin(przedmiotowa_response['ZAPIS_ID'])]
    else: wspoltworca_response = pd.DataFrame(columns=['ZAPIS_ID', 'RODZAJ', 'ODWOLANIE', 'ODWOLANIE_ID', 'ROK', 'TYTUL', 'TYTUL_ORYGINALU', 'WYDANIE', 'WYDAWNICTWO', 'MIEJSCE_WYDANIA', 'ROK_WYDANIA', 'TOMY', 'OPIS_FIZYCZNY', 'SERIA_WYDAWNICZA', 'ADNOTACJE1', 'ADNOTACJE2', 'ADNOTACJE3', 'WSPOLTWORCY', 'ZRODLO', 'ZRODLO_ROK', 'NUMER', 'STRONY', 'IMIE_AUTORA', 'NAZWISKO_AUTORA', 'IMIE_TWORCY', 'NAZWISKO_TWORCY', 'ZA_UZYTK_WPIS_DATA'])
    
    # indeks
    
    indeks_query =  f"""
                    select distinct cz1.za_zapis_id zapis_id, cz1.rz_nazwa rodzaj, cz1.za_opis_odwolania odwolanie, cz1.za_za_zapis_id odwolanie_id, cz1.za_ro_rok rok, cz1.za_tytul tytul, cz1.za_tytul_oryginalu tytul_oryginalu, cz1.za_wydanie wydanie, cz1.wy_nazwa wydawnictwo, cz1.wy_miasto miejsce_wydania, cz1.za_rok_wydania rok_wydania, cz1.za_tomy tomy, cz1.za_opis_fizyczny_ksiazki opis_fizyczny, cz1.za_seria_wydawnicza seria_wydawnicza, cz1.za_adnotacje adnotacje1, cz1.za_adnotacje2 adnotacje2, cz1.za_adnotacje3 adnotacje3, cz1.za_opis_wspoltworcow wspoltworcy, cz1.zr_tytul zrodlo, cz1.za_zrodlo_rok zrodlo_rok, cz1.za_zrodlo_nr numer, cz1.za_zrodlo_str strony,  aut2.am_imie imie_autora, aut2.am_nazwisko nazwisko_autora, cz1.tw_imie imie_tworcy, cz1.tw_nazwisko nazwisko_tworcy, cz1.za_uzytk_wpis_data
                    from
                    (select *
                    from IBL_OWNER.pbl_zapisy z
                    left join IBL_OWNER.pbl_zapisy_tworcy ztw on z.za_zapis_id = ztw.zatw_za_zapis_id
                    left join IBL_OWNER.pbl_zapisy_autorzy zau2 on z.za_zapis_id = zau2.zaam_za_zapis_id
                    left join IBL_OWNER.pbl_autorzy aut2 on zau2.zaam_am_autor_id = aut2.am_autor_id
                    left join IBL_OWNER.pbl_udzialy_osob uo on z.za_zapis_id = uo.uo_za_zapis_id
                    left join IBL_OWNER.pbl_rodzaje_zapisow rzz on z.za_rz_rodzaj1_id = rzz.rz_rodzaj_id
                    left join IBL_OWNER.pbl_zapisy_wydawnictwa zawy on z.za_zapis_id = zawy.zawy_za_zapis_id
                    left join IBL_OWNER.pbl_wydawnictwa wyd on zawy.zawy_wy_wydawnictwo_id = wyd.wy_wydawnictwo_id
                    left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
                    left join IBL_OWNER.pbl_osoby_do_indeksu odi on z.za_zapis_id = odi.odi_za_zapis_id
                    left join IBL_OWNER.pbl_tworcy tw on ztw.zatw_tw_tworca_id = tw.tw_tworca_id
                    where z.za_proweniencja is null
                    and z.za_status_imp is null
                    and odi.odi_nazwisko like '{surname}'
                    and odi.odi_imie like '{name}') cz1
                    left join IBL_OWNER.pbl_zapisy_autorzy zau2 on cz1.za_zapis_id = zau2.zaam_za_zapis_id
                    left join IBL_OWNER.pbl_autorzy aut2 on zau2.zaam_am_autor_id = aut2.am_autor_id
                        """
    
    indeks_response = pd.read_sql(indeks_query, connection) 
    indeks_response = indeks_response[indeks_response['ZA_UZYTK_WPIS_DATA'] > date]
    indeks_response = indeks_response[~indeks_response['ZAPIS_ID'].isin(podmiotowa_response['ZAPIS_ID'])]
    indeks_response = indeks_response[~indeks_response['ZAPIS_ID'].isin(przedmiotowa_response['ZAPIS_ID'])]
    indeks_response = indeks_response[~indeks_response['ZAPIS_ID'].isin(wspoltworca_response['ZAPIS_ID'])]
    
    
    # cleaning duplicates 
    podmiotowa_response = podmiotowa_response.fillna('')
    podmiotowa_response['autor'] = podmiotowa_response['IMIE_AUTORA'] + ' ' + podmiotowa_response['NAZWISKO_AUTORA']
    podmiotowa_response['tworca'] = podmiotowa_response['IMIE_TWORCY'] + ' ' + podmiotowa_response['NAZWISKO_TWORCY']
    podmiotowa_response['miejsce wydania'] = podmiotowa_response['WYDAWNICTWO'] + ': ' + podmiotowa_response['MIEJSCE_WYDANIA']
    podmiotowa_response['ADNOTACJE'] = podmiotowa_response['ADNOTACJE1'] + ' ' + podmiotowa_response['ADNOTACJE2'] + ' ' + podmiotowa_response['ADNOTACJE3']
    podmiotowa_response = podmiotowa_response.drop(columns=['IMIE_AUTORA', 'NAZWISKO_AUTORA', 'IMIE_TWORCY', 'NAZWISKO_TWORCY', 'MIEJSCE_WYDANIA', 'WYDAWNICTWO', 'ADNOTACJE1', 'ADNOTACJE2', 'ADNOTACJE3'])
    podmiotowa_response['autor'] = podmiotowa_response[['ZAPIS_ID', 'autor']].groupby(['ZAPIS_ID'])['autor'].transform(lambda x: ' | '.join(list(set(x))))
    podmiotowa_response['tworca'] = podmiotowa_response[['ZAPIS_ID', 'tworca']].groupby(['ZAPIS_ID'])['tworca'].transform(lambda x: ' | '.join(list(set(x))))
    podmiotowa_response['miejsce wydania'] = podmiotowa_response[['ZAPIS_ID', 'miejsce wydania']].groupby(['ZAPIS_ID'])['miejsce wydania'].transform(lambda x: ' | '.join(list(set(x))))
    podmiotowa_response = podmiotowa_response.drop(columns=['ZAPIS_ID']).drop_duplicates()
    podmiotowa_response = podmiotowa_response.replace([': ', ' | '], '')
    
    przedmiotowa_response = przedmiotowa_response.fillna('')
    przedmiotowa_response['autor'] = przedmiotowa_response['IMIE_AUTORA'] + ' ' + przedmiotowa_response['NAZWISKO_AUTORA']
    przedmiotowa_response['tworca'] = przedmiotowa_response['IMIE_TWORCY'] + ' ' + przedmiotowa_response['NAZWISKO_TWORCY']
    przedmiotowa_response['miejsce wydania'] = przedmiotowa_response['WYDAWNICTWO'] + ': ' + przedmiotowa_response['MIEJSCE_WYDANIA']
    przedmiotowa_response['ADNOTACJE'] = przedmiotowa_response['ADNOTACJE1'] + ' ' + przedmiotowa_response['ADNOTACJE2'] + ' ' + przedmiotowa_response['ADNOTACJE3']
    przedmiotowa_response = przedmiotowa_response.drop(columns=['IMIE_AUTORA', 'NAZWISKO_AUTORA', 'IMIE_TWORCY', 'NAZWISKO_TWORCY', 'MIEJSCE_WYDANIA', 'WYDAWNICTWO', 'ADNOTACJE1', 'ADNOTACJE2', 'ADNOTACJE3'])
    przedmiotowa_response['autor'] = przedmiotowa_response[['ZAPIS_ID', 'autor']].groupby(['ZAPIS_ID'])['autor'].transform(lambda x: ' | '.join(list(set(x))))
    przedmiotowa_response['tworca'] = przedmiotowa_response[['ZAPIS_ID', 'tworca']].groupby(['ZAPIS_ID'])['tworca'].transform(lambda x: ' | '.join(list(set(x))))
    przedmiotowa_response['miejsce wydania'] = przedmiotowa_response[['ZAPIS_ID', 'miejsce wydania']].groupby(['ZAPIS_ID'])['miejsce wydania'].transform(lambda x: ' | '.join(list(set(x))))
    przedmiotowa_response = przedmiotowa_response.drop(columns=['ZAPIS_ID']).drop_duplicates()
    przedmiotowa_response = przedmiotowa_response.replace([': ', ' | '], '')
    
    wspoltworca_response = wspoltworca_response.fillna('')
    wspoltworca_response['autor'] = wspoltworca_response['IMIE_AUTORA'] + ' ' + wspoltworca_response['NAZWISKO_AUTORA']
    wspoltworca_response['tworca'] = wspoltworca_response['IMIE_TWORCY'] + ' ' + wspoltworca_response['NAZWISKO_TWORCY']
    wspoltworca_response['miejsce wydania'] = wspoltworca_response['WYDAWNICTWO'] + ': ' + wspoltworca_response['MIEJSCE_WYDANIA']
    wspoltworca_response['ADNOTACJE'] = wspoltworca_response['ADNOTACJE1'] + ' ' + wspoltworca_response['ADNOTACJE2'] + ' ' + wspoltworca_response['ADNOTACJE3']
    wspoltworca_response = wspoltworca_response.drop(columns=['IMIE_AUTORA', 'NAZWISKO_AUTORA', 'IMIE_TWORCY', 'NAZWISKO_TWORCY', 'MIEJSCE_WYDANIA', 'WYDAWNICTWO', 'ADNOTACJE1', 'ADNOTACJE2', 'ADNOTACJE3'])
    wspoltworca_response['autor'] = wspoltworca_response[['ZAPIS_ID', 'autor']].groupby(['ZAPIS_ID'])['autor'].transform(lambda x: ' | '.join(list(set(x))))
    wspoltworca_response['tworca'] = wspoltworca_response[['ZAPIS_ID', 'tworca']].groupby(['ZAPIS_ID'])['tworca'].transform(lambda x: ' | '.join(list(set(x))))
    wspoltworca_response['miejsce wydania'] = wspoltworca_response[['ZAPIS_ID', 'miejsce wydania']].groupby(['ZAPIS_ID'])['miejsce wydania'].transform(lambda x: ' | '.join(list(set(x))))
    wspoltworca_response = wspoltworca_response.drop(columns=['ZAPIS_ID']).drop_duplicates()
    wspoltworca_response = wspoltworca_response.replace([': ', ' | '], '')
    
    indeks_response = indeks_response.fillna('')
    indeks_response['autor'] = indeks_response['IMIE_AUTORA'] + ' ' + indeks_response['NAZWISKO_AUTORA']
    indeks_response['tworca'] = indeks_response['IMIE_TWORCY'] + ' ' + indeks_response['NAZWISKO_TWORCY']
    indeks_response['miejsce wydania'] = indeks_response['WYDAWNICTWO'] + ': ' + indeks_response['MIEJSCE_WYDANIA']
    indeks_response['ADNOTACJE'] = indeks_response['ADNOTACJE1'] + ' ' + indeks_response['ADNOTACJE2'] + ' ' + indeks_response['ADNOTACJE3']
    indeks_response = indeks_response.drop(columns=['IMIE_AUTORA', 'NAZWISKO_AUTORA', 'IMIE_TWORCY', 'NAZWISKO_TWORCY', 'MIEJSCE_WYDANIA', 'WYDAWNICTWO', 'ADNOTACJE1', 'ADNOTACJE2', 'ADNOTACJE3'])
    indeks_response['autor'] = indeks_response[['ZAPIS_ID', 'autor']].groupby(['ZAPIS_ID'])['autor'].transform(lambda x: ' | '.join(list(set(x))))
    indeks_response['tworca'] = indeks_response[['ZAPIS_ID', 'tworca']].groupby(['ZAPIS_ID'])['tworca'].transform(lambda x: ' | '.join(list(set(x))))
    indeks_response['miejsce wydania'] = indeks_response[['ZAPIS_ID', 'miejsce wydania']].groupby(['ZAPIS_ID'])['miejsce wydania'].transform(lambda x: ' | '.join(list(set(x))))
    indeks_response = indeks_response.drop(columns=['ZAPIS_ID']).drop_duplicates()
    indeks_response = indeks_response.replace([': ', ' | '], '')
    
    # columns order
    
    columns_order = ['RODZAJ', 'ODWOLANIE', 'ODWOLANIE_ID', 'ROK', 'tworca', 'autor', 'TYTUL', 'TYTUL_ORYGINALU', 'WSPOLTWORCY', 'miejsce wydania', 'WYDANIE', 'ROK_WYDANIA', 'OPIS_FIZYCZNY', 'SERIA_WYDAWNICZA', 'ADNOTACJE', 'ZRODLO', 'ZRODLO_ROK', 'NUMER', 'STRONY', 'ZA_UZYTK_WPIS_DATA']
    
    podmiotowa_response = podmiotowa_response[columns_order]
    podmiotowa_response.rename(columns = {'ZA_UZYTK_WPIS_DATA': 'DATA_WPISU'}, inplace = True)
    przedmiotowa_response = przedmiotowa_response[columns_order]
    przedmiotowa_response.rename(columns = {'ZA_UZYTK_WPIS_DATA': 'DATA_WPISU'}, inplace = True)
    wspoltworca_response = wspoltworca_response[columns_order]
    wspoltworca_response.rename(columns = {'ZA_UZYTK_WPIS_DATA': 'DATA_WPISU'}, inplace = True)
    indeks_response = indeks_response[columns_order]
    indeks_response.rename(columns = {'ZA_UZYTK_WPIS_DATA': 'DATA_WPISU'}, inplace = True)
    
    # send to google drive
    gc = gs.oauth()
    
    sheet_name = pracownik + ' - ' + name + ' ' + surname
    sheet = gc.create(sheet_name, folder_pracownika_id)
    
    worksheets_dict = {'podmiotowa': podmiotowa_response, 'przedmiotowa': przedmiotowa_response, 'współtwórca': wspoltworca_response, 'adnotacje_indeks': indeks_response}
    
    for key, value in worksheets_dict.items():
        time.sleep(5)
        worksheet = sheet.add_worksheet(title=key, rows="100", cols="20")
        set_with_dataframe(worksheet, value)
        worksheet.sort((3, 'des'))
        worksheet.format('A1:Z1', {'textFormat': {'bold': True}})
        worksheet.format('A:Z', {"wrapStrategy": 'CLIP'})
        
        sheet.batch_update({
        "requests": [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet._properties['sheetId'],
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": 100
                    },
                    "properties": {
                        "pixelSize": 20
                    },
                    "fields": "pixelSize"
                        }
                    }
                ]
            })
    
    
    sheet.del_worksheet(sheet.get_worksheet(0))
    
    












