#%%
from pymarc import MARCReader
from tqdm import tqdm
import pandas as pd

#%%
values_372 = set()
values_374 = set()

with open('authorities-all.marc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in tqdm(reader):
        if 'osobowe' in [field.value() for field in record.get_fields('667')]:
            for key in ('372', '374'):
                for field in record.get_fields(key):
                    match key:
                        case '372':
                            values_372.add(field.value())
                        case '374':
                            values_374.add(field.value())

    



print(record)
record.get_fields('667')
record
for fi in record.get_fields('667'):
    print(fi.value())
