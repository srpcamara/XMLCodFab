import xml.etree.ElementTree as ET
from os import path, listdir
from logger_settings import app_logger
import sqlite3

connection = sqlite3.connect('database.db')
c = connection.cursor()

def read_xml(xml_folder):    
    xmls_files = [f"{xml_folder}/{xml_file}" for xml_file in listdir(xml_folder) if xml_file.endswith('.xml')]

    namespace = "{http://www.portalfiscal.inf.br/nfe}"
    dataset = []

    for xml_file in xmls_files:
        tree = ET.parse(xml_file)
        root = tree.getroot() 

        try:
            cnpj_origem = root[0][0][1][0].text

            for x in root[0][0].findall(f'{namespace}det'):              
                dataset.append([
                    cnpj_origem,
                    x[0].find(f'{namespace}cProd').text,
                    x[0].find(f'{namespace}cEAN').text,
                    x[0].find(f'{namespace}xProd').text,
                    x[0].find(f'{namespace}uCom').text
                ])                
        except:
            app_logger.error(f"Arquivo não processado: {xml_file}")

    return dataset

def write_internal_db(dataset):   
    c.execute('DELETE FROM xml')

    for linha in dataset:        
        sql = f'INSERT INTO xml(cnpj, codfabrica, ean, descricao, unidade) values ({linha[0]}, "{linha[1]}", {linha[2]}, "{linha[3]}", "{linha[4]}")'
        c.execute(sql)

    connection.commit()

write_internal_db(read_xml('xml'))
connection.close()