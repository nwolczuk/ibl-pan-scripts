import regex as re
import os
from tqdm import tqdm

#%%
def regex_on_file(path, patterns):
    with open(path, 'r', encoding='utf-8') as xml:
        content = xml.read()
    for pattern in patterns:
        while re.search(pattern[0], content):
            content = re.sub(pattern[0], pattern[1], content)
    with open(path, 'w', encoding='utf-8') as xml:
        xml.write(content)
        
    
#%%
main_dirs = ['retro_2023-10-05', 'xml_output_2023-10-05']
patterns = [(r'(>.*?)’(.*?<\/.+?>)', r'\g<1>ʼ\g<2>'),
            (r'(>.*?)"(.*?<\/.+?>)', r'\g<1>ʺ\g<2>'),
            (r'(<.+?)’(.+?>)', r'\g<1>ʼ\g<2>')]

for root in main_dirs:
    if root=='xml_output_2023-10-05':
        files = os.listdir(root)
        for file in tqdm(files):
            regex_on_file(root + '/' + file, patterns)
    elif root=='retro_2023-10-05':
        for subdir in os.listdir(root):
            files = os.listdir(root + '/' + subdir)
            for file in tqdm(files):
                regex_on_file(root + '/' + subdir + '/' + file, patterns)


#%%

# def regex_on_file(path):
#     with open(path, 'r', encoding='utf-8') as xml:
#         content = xml.read()
#     list_of_ids = re.findall(r'(?<=id=").+?(?=">|" )', content)
#     list_of_ids = list(set(list_of_ids))
#     ids_with_subs = [('id="'+elem+'"', 'id="'+re.sub(r'[^a-zA-Z\d_]', '', elem)+'"') for elem in list_of_ids]
#     ids_with_subs = [e for e in ids_with_subs if e[0] != e[1]]
#     for identifier in ids_with_subs:
#         while identifier[0] in content:
#             content = content.replace(identifier[0], identifier[1])
#     with open(path, 'w', encoding='utf-8') as xml:
#         xml.write(content)
        
main_dirs = ['retro_2023-10-05', 'xml_output_2023-10-05']
patterns = [(r'(<.+id=\".+?)[^a-zA-Z\d_\"\-❦]([^\"]*?\" |[^\"]*?\">)', r'\g<1>\g<2>')]

for root in main_dirs:
    if root=='xml_output_2023-10-05':
        files = os.listdir(root)
        for file in tqdm(files):
            regex_on_file(root + '/' + file, patterns)
    elif root=='retro_2023-10-05':
        for subdir in os.listdir(root):
            files = os.listdir(root + '/' + subdir)
            for file in tqdm(files):
                regex_on_file(root + '/' + subdir + '/' + file, patterns)
                

#%%

def regex_on_file(path):
    with open(path, 'r', encoding='utf-8') as xml:
        content = xml.read()
    list_of_ids = re.findall(r'(?<=id=\")[^ ]*?(?=\">|\" )', content)
    list_of_ids = [e for e in list_of_ids if not e.startswith('http') and not e.startswith('fake_id')]
    list_of_ids = list(set(list_of_ids))
    ids_with_subs = [('id="'+elem+'"', 'id="'+re.sub(r'[^a-zA-Z\d_\-]', '', elem)+'"') for elem in list_of_ids]
    ids_with_subs = [e for e in ids_with_subs if e[0] != e[1]]
    for identifier in ids_with_subs:
        content = content.replace(identifier[0], identifier[1])
    with open(path, 'w', encoding='utf-8') as xml:
        xml.write(content)
        
main_dirs = ['retro_2023-10-05', 'xml_output_2023-10-05']

for root in main_dirs:
    if root=='xml_output_2023-10-05':
        files = os.listdir(root)
        for file in tqdm(files):
            regex_on_file(root + '/' + file)
    elif root=='retro_2023-10-05':
        for subdir in os.listdir(root):
            files = os.listdir(root + '/' + subdir)
            for file in tqdm(files):
                regex_on_file(root + '/' + subdir + '/' + file)
