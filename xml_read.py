import xml.etree.ElementTree as ET
from os import path, listdir

def read(xml_folder):
    
    xmls_files = [f"{xml_folder}/{xml_file}" for xml_file in listdir(xml_folder) if xml_file.endswith('.xml')]

    namespace = "{http://www.portalfiscal.inf.br/nfe}"
    dataset = []
    cProd = 0
    cEan = 1
    xProd = 2
    uCom = 6 

    for xml_file in xmls_files:
        tree = ET.parse(xml_file)
        root = tree.getroot() 

        cnpj_origem = root[0][0][1][0].text
        for x in root[0][0].findall(f'{namespace}det'):     
            dataset.append([cnpj_origem, x[0][cProd].text, x[0][cEan].text, x[0][xProd].text, x[0][uCom].text])
    
    return dataset