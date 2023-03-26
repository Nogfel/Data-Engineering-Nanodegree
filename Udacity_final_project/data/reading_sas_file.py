import csv

print('Generating `sas_descriptive_inforation.csv` file...')

with open('I94_SAS_Labels_Descriptions.SAS') as file:
    f_content = file.read()
    f_content.replace('\t', '')

def code_mapper(file, idx):
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

i94cit_res = code_mapper(f_content, "i94cntyl")
i94port = code_mapper(f_content, "i94prtl")
i94mode = code_mapper(f_content, "i94model")
i94addr = code_mapper(f_content, "i94addrl")
i94visa = {'1':'Business',
'2': 'Pleasure',
'3' : 'Student'}


all_raw_data_dicts = {'i94cit_res':i94cit_res,
                      'i94port':i94port,
                      'i94mode':i94mode,
                      'i94addr':i94addr,
                      'i94visa':i94visa}

with open('sas_descriptive_information.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['id', 'description', 'column'], delimiter='|')
    writer.writeheader()
    for key_table, dicts in all_raw_data_dicts.items():
        for key, value in dicts.items():
            f.write('"{0}"|"{1}"|"{2}"\n'.format(key, value, key_table))

print('File `sas_descriptive_inforation.csv` generated with success.')
