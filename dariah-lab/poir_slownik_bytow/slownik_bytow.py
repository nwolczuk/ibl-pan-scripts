#%% import modules
import pandas as pd
import string
from tqdm import tqdm

#%% load vocabulary

variable_names = {
    'autor': 'autor',
    'tłumacz': 'tlumacz',
    'badacz': 'badacz',
    'postać rzeczywista': 'postac_rzeczywista',
    'postać fikcyjna': 'postac_fikcyjna',
    'miejsce rzeczywiste': 'miejsce_rzeczywiste',
    'miejsce fikcyjne': 'miejsce_fikcyjne',
    'wydarzenie': 'wydarzenie',
    'instytucja': 'instytucja',
    'czasopismo': 'czasopismo',
    'grupa literacka': 'grupa_literacka',
    'utwór': 'utwor'
    }

xlsx = pd.ExcelFile('słownik bytów v0.2.xlsx')

for sheet_name in xlsx.sheet_names:
    var_name = variable_names[sheet_name]
    exec(f"{var_name} = pd.read_excel(xlsx, sheet_name='{sheet_name}')")

xlsx.close()

#%%

def simplify_str(input_string):
    output_string = input_string.translate(str.maketrans('', '', string.punctuation)).replace(' ', '')
    return output_string.lower()

for var in tqdm(variable_names.values()):
    exec(f'temp = {var}')
    temp_dict = {}

    for idx, row in temp.iterrows():
        entity = str(row['byt'])
        if var in ('postac_fikcyjna') and entity.isupper():
            entity = entity.title() 
        provenance = str(row['źródło'])
        simply_entity = simplify_str(entity) if simplify_str(entity) else entity
        if simply_entity in temp_dict:
            temp_dict[simply_entity]['entities'].append(entity)
            temp_dict[simply_entity]['provenances'].add(provenance)
        else:
            temp_dict[simply_entity] = {'entities': [entity], 'provenances': {provenance}}
        
    temp = pd.DataFrame([(v['entities'][0], ' | '.join(list(v['provenances']))) for k,v in temp_dict.items()], columns=['byt','źródła'])
    exec(f'{var} = temp')
    
with pd.ExcelWriter('słownik bytów v0.3.xlsx') as writer:
    for sheet_name, var_name in variable_names.items():     
        exec(f"{var_name}.to_excel(writer, sheet_name='{sheet_name}', index=False)")

