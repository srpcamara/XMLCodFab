import xml.etree.ElementTree as ET
from os import path, listdir

files_path = path.abspath('') + '/xml'
xmls = [f"{files_path}/{xml_file}" for xml_file in listdir(files_path) if xml_file.endswith('.xml')]

namespace = "{http://www.portalfiscal.inf.br/nfe}"
dataset = []
cProd = 0
cEan = 1
xProd = 2
uCom = 6
sql = ''

f= open("sql.txt","w+")

for xml_file in xmls:
    tree = ET.parse(xml_file)
    root = tree.getroot() 

    cnpj_origem = root[0][0][1][0].text
    for x in root[0][0].findall(f'{namespace}det'):     
        dataset.append([cnpj_origem, x[0][cProd].text, x[0][cEan].text, x[0][xProd].text, x[0][uCom].text])
        sql = f'INSERT INTO SCCODFABRICA(CNPJ, CODFABRICA, EAN, DESCRICAO, UNIDADE) VALUES ({cnpj_origem}, "{x[0][cProd].text}", {x[0][cEan].text}, "{x[0][xProd].text}", "{x[0][uCom].text}");'
        f.write(sql + '\n')
        
f.close() 


# for x in dataset:
#     print(x[0]) 

#print(dataset)
