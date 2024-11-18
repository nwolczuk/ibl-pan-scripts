import pandas as pd 

df = pd.read_excel('KDL_polona_dziela.xlsx')

polona_dct = {}
for idx, row in df.iterrows():
    simple_id = row['identifier'].split('_')[0]
    polona_dct.setdefault(simple_id, list()).append(row)
    
# by title and year
polona_titles_dct = {}
for key,value in polona_dct.items():
    for row in value:
        polona_title = row['polona_title'].replace(',', '').replace('.', '').replace(' ', '')
        polona_year = row['polona_year']
        polona_titles_dct.setdefault(key, dict()).setdefault((polona_title, polona_year), list()).append(row)
        

rows = []
for key,value in polona_titles_dct.items():
    for k,v in value.items():
        polona_links = []
        odrzucenie = False
        first_row = v[0]
        for row in v:
            if row['do_odrzucenia']:
               odrzucenie = True 
            pol_lnk = row['polona_link']
            if not isinstance(pol_lnk, float):
                polona_links.append(pol_lnk)      
        polona_links = ' | '.join(polona_links)
        first_row['polona_link'] = polona_links
        first_row['do_odrzucenia'] = odrzucenie
        rows.append(first_row)

out_df = pd.DataFrame(rows)
out_df.to_excel('polona_dziela_by_title-year.xlsx')



# only by title
polona_titles_dct = {}
for key,value in polona_dct.items():
    for row in value:
        polona_title = row['polona_title'].replace(',', '').replace('.', '').replace(' ', '')
        polona_titles_dct.setdefault(key, dict()).setdefault(polona_title, list()).append(row)
        
rows = []
for key,value in polona_titles_dct.items():
    for k,v in value.items():
        polona_links = []
        odrzucenie = False
        first_row = v[0]
        for row in v:
            if row['do_odrzucenia']:
               odrzucenie = True 
            pol_lnk = row['polona_link']
            if not isinstance(pol_lnk, float):
                polona_links.append(pol_lnk)      
        polona_links = ' | '.join(polona_links)
        first_row['polona_link'] = polona_links
        first_row['do_odrzucenia'] = odrzucenie
        rows.append(first_row)

out_df = pd.DataFrame(rows)
out_df.to_excel('polona_dziela_by_title.xlsx')