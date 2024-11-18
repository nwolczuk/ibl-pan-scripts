def read_mrk(path):
    records = []
    with open(path, 'r', encoding='utf-8') as mrk:
        record_dict = {}
        for line in mrk.readlines():
            line = line.replace('\n', '')
            if line.startswith('=LDR'):
                if record_dict:
                    records.append(record_dict)
                    record_dict = {}
                record_dict[line[1:4]] = [line[6:]]
            elif line.startswith('='):
                key = line[1:4]
                if key in record_dict:
                    record_dict[key] += [line[6:]]
                else:
                    record_dict[key] = [line[6:]]
        records.append(record_dict)
    return records

def records_list_to_dict(records_list):
    records_dict = {}
    for rec in records_list:
        identifier = rec.get('001')[0]
        records_dict[identifier] = rec
    return records_dict

def sort_lines(lines):
    lines.sort()
    for elem in lines:
        if elem.startswith('=LDR'):
            lines.remove(elem)
            lines.insert(0, elem)
    return lines

def list_to_lines(records_list):
    lines = []
    for rec in records_list:
        rec_lines = []
        for key, value in rec.items():
            for field in value:
                line_to_write = f'={key}  {field}\n'
                rec_lines.append(line_to_write)
        rec_lines = sort_lines(rec_lines)
        lines.extend(rec_lines)
        lines.append('\n')   
    return lines

def dict_to_lines(records_dict):
    lines = []
    for key, value in records_dict.items():
        rec_lines = []
        for k, v in value.items():
            for field in v:
                line_to_write = f'={k}  {field}\n'
                rec_lines.append(line_to_write) 
        rec_lines = sort_lines(rec_lines)
        lines.extend(rec_lines)
        lines.append('\n')
    return lines

def write_mrk(path, value):
    if isinstance(value, list):
        lines = list_to_lines(value)
    elif isinstance(value, dict):
        lines = dict_to_lines(value)
    else:
        raise Exception('Object type is not supported.')
    with open(path, 'w', encoding='utf-8') as mrk:
        mrk.writelines(lines)