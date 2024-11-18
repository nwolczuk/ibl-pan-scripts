from tqdm import tqdm
import spacy
import json
import requests

# from heapq import nlargest as _nlargest
from collections import namedtuple as _namedtuple
from difflib import SequenceMatcher

#%% 

nlp = spacy.load("pl_core_news_lg")

def open_data(path):
    with open (path, "r", encoding='utf-8') as f:
        data = json.load(f)
    return data


def get_data(url: str) -> list:
    responses = []
    while url:
        url = requests.get(url)
        if url.status_code == 200:
            url = url.json()
            responses.append(url)
            url = url["nextPage"]
            print(f"Downloading: {url}")
        else:
            print("Error while accessing API")
    print("Download complete")
    return responses


def get_subj(sub: str, header: str, field_numbers: list) -> dict:  
    responses = get_data(f"http://data.bn.org.pl/api/authorities.json?{header}={sub}")
    subjects = []
    for response in responses:
        for authority in response["authorities"]:
            for field in authority["marc"]["fields"]:
                for field_number in field_numbers:
                    if field_number in field:
                        for i in field:
                            subjects.append(list(field[i]["subfields"][-1].values())[0])
                
    subjects_dict = {}
    subjects_dict[sub] = subjects
    return subjects_dict


def lemmatize(term):
    lemmas = " ".join([w.lemma_ for w in nlp(term)])
    return lemmas

Match = _namedtuple('Match', 'a b size')

def get_close_matches(word, possibilities, cutoff=0.86):
    """Use SequenceMatcher to return list of the best "good enough" matches.
    word is a sequence for which close matches are desired (typically a
    string).
    possibilities is a list of sequences against which to match word
    (typically a list of strings).
    Optional arg n (default 3) is the maximum number of close matches to
    return.  n must be > 0.
    Optional arg cutoff (default 0.6) is a float in [0, 1].  Possibilities
    that don't score at least that similar to word are ignored.
    The best (no more than n) matches among the possibilities are returned
    in a list, sorted by similarity score, most similar first.
    >>> get_close_matches("appel", ["ape", "apple", "peach", "puppy"])
    ['apple', 'ape']
    >>> import keyword as _keyword
    >>> get_close_matches("wheel", _keyword.kwlist)
    ['while']
    >>> get_close_matches("Apple", _keyword.kwlist)
    []
    >>> get_close_matches("accept", _keyword.kwlist)
    ['except']
    """

    # if not n >  0:
    #     raise ValueError("n must be > 0: %r" % (n,))
    # if not 0.0 <= cutoff <= 1.0:
    #     raise ValueError("cutoff must be in [0.0, 1.0]: %r" % (cutoff,))
    result = []
    s = SequenceMatcher()
    s.set_seq2(word)
    for x in possibilities:
        s.set_seq1(x)
        if s.real_quick_ratio() >= cutoff and \
           s.quick_ratio() >= cutoff and \
           s.ratio() >= cutoff:
            result.append((s.ratio(), x))
            
    # Move the best scorers to head of list
    # result = _nlargest(n, result)
    result = sorted(result)[::-1]
    
    # Strip scores for the best n matches
    #return [x for score, x in result]
    final_result = []
    if result:
        highest_score = result[0][0]
        for elem in result:
            if elem[0] < highest_score: break
            else: final_result.append(elem[1])
    return final_result

def identify_dbn(term: str):
    term = lemmatize(term)
    result = []
    
    # for key, value in data_bn.items():
    #     if term == value['150_lemma']:
    #         result.append((data_bn[key], ('150_lemma', value['150_lemma'])))
            
    # if not result:
    #     for k,v in data_bn.items():
    #         if "450a_lemma" in v:
    #             for elem in v["450a_lemma"]:
    #                 if term == elem:
    #                     po_czym_zlapalo = ("450a", elem)
    #                     result = [(v, po_czym_zlapalo)]

    if not result:
        part_results_dict = {}
        for k,v in data_bn.items():
            if "150_lemma" in v:
                part_results_dict[v["150_lemma"]] = (k, v["150_lemma"])
        keys = get_close_matches(term, part_results_dict.keys())
        for key in keys:
            key150 = part_results_dict[key][0]
            po_czym_zlapalo = part_results_dict[key][1]
            result.append((data_bn[key150], ("150_lemma_leven", po_czym_zlapalo)))
            
    if not result:
        part_results_dict = {}
        for k,v in data_bn.items():
            if "450a_lemma" in v:
                part_result = get_close_matches(term, v["450a_lemma"])
                if part_result:
                    part_results_dict[part_result[0]] = (k, v["450a_lemma"])
        keys = get_close_matches(term, part_results_dict.keys())
        for key in keys:
            key150 = part_results_dict[key][0]
            po_czym_zlapalo = part_results_dict[key][1]
            result.append((data_bn[key150], ("450_lemma_leven", po_czym_zlapalo)))
            
    return result

#%%

data_bn = open_data("/data/sample.json")
bib_nau_kwds = open_data("/data/bib_nau_kwds.json")

output = {}
for keyword in tqdm(bib_nau_kwds):
    try:
        output[keyword] = identify_dbn(keyword)
    except KeyboardInterrupt:
        break
    except Exception as ex:
        output[keyword] = repr(ex)

with_results = {}
no_results = []
errors = {}

for key, value in output.items():
    if not value:
        no_results.append(key)
    elif isinstance(value, str):
        errors[key] = value
    else:
        with_results[key] = value
         
with open('/data/bib_nau_kwds_bn_exact_150_450.json', 'w', encoding='utf-8') as file:
    json.dump(with_results, file, indent=4, ensure_ascii=False)
with open('/data/bib_nau_kwds_bn_no_results_150_450.json', 'w', encoding='utf-8') as file:
    json.dump(no_results, file, indent=4, ensure_ascii=False)  
    
if errors:
    with open('/data/bib_nau_kwds_bn_errors.json', 'w', encoding='utf-8') as file:
        json.dump(errors, file, indent=4, ensure_ascii=False)
        
#%%
data_bn = open_data("/data/sample.json")
bib_nau_kwds = open_data("/data/bib_nau_kwds_bn_no_results_150_450.json")

output = {}
for keyword in tqdm(bib_nau_kwds):
    try:
        output[keyword] = identify_dbn(keyword)
    except KeyboardInterrupt:
        break
    except Exception as ex:
        output[keyword] = repr(ex)

with_results = {}
no_results = []
errors = {}

for key, value in output.items():
    if not value:
        no_results.append(key)
    elif isinstance(value, str):
        errors[key] = value
    else:
        with_results[key] = value
         
with open('/data/bib_nau_kwds_bn_leven.json', 'w', encoding='utf-8') as file:
    json.dump(with_results, file, indent=4, ensure_ascii=False)
with open('/data/bib_nau_kwds_bn_leven_no_results.json', 'w', encoding='utf-8') as file:
    json.dump(no_results, file, indent=4, ensure_ascii=False)  
    
if errors:
    with open('/data/bib_nau_kwds_bn_errors.json', 'w', encoding='utf-8') as file:
        json.dump(errors, file, indent=4, ensure_ascii=False)
        
