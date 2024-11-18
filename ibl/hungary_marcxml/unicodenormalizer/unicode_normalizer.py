'''
Supported data types:
    - str
    - pandas dataframe
    - list
    - dict
    
Normalization modes:
    - NFD - canonical decomposition
    - NFC - first applies a canonical decomposition, then composes pre-combined characters again
    - NFKD - compatibility decomposition
    - NFKC -  applies the compatibility decomposition, followed by the canonical composition
'''

import unicodedata as ud
import pandas as pd


def normalize_str(str_input: str, mode = "NFC") -> str:
    try:
        return ud.normalize(mode, str_input)
    except TypeError:
        return str_input
  
    
def normalize_df(df_input: pd.core.frame.DataFrame, mode = "NFC") -> pd.core.frame.DataFrame:
    output_df = pd.DataFrame()
    for col in df_input.columns:
        col_normalized = [normalize_str(value, mode) for value in df_input[col]]
        output_df[col] = col_normalized
    return output_df


def normalize_list(list_input: list, mode = "NFC") -> list:
    output_list = []
    for elem in list_input:
        if type(elem) == str:
            output_list.append(normalize_str(elem))
        elif type(elem) == list:
            output_list.append(normalize_list(elem))
        elif type(elem) == dict:
            output_list.append(normalize_dict(elem))
        elif type(elem) == pd.core.frame.DataFrame:
            output_list.append(normalize_df(elem))   
        else: output_list.append(elem)
    return output_list
      

def normalize_dict(dict_input: dict, mode = "NFC") -> dict:
    output_dict = {}
    for key, value in dict_input.items():
        if type(value) == str:
            output_dict[normalize_str(key)] = normalize_str(value)
        elif type(value) == list:
            output_dict[normalize_str(key)] = normalize_list(value)
        elif type(value) == dict:
            output_dict[normalize_str(key)] = normalize_dict(value)
        elif type(value) == pd.core.frame.DataFrame:
            output_dict[normalize_str(key)] = normalize_df(value)
        else: output_dict[normalize_str(key)] = value
    return output_dict


def normalize_data(data, mode = "NFC"):
    data_type = type(data)
    if data_type == str:
        return normalize_str(data)
    elif data_type == list:
        return normalize_list(data)
    elif data_type == dict:
        return normalize_dict(data)
    elif data_type == pd.core.frame.DataFrame:
        return normalize_df(data)
    else:
        raise Exception("Not supported data type!")


def remove_control_characters(s):
    return ''.join(c for c in s if ud.category(c)[0] != 'C')