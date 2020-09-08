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
    row_count = 0

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
                row_count =+ 1
                print(f'Lendo arquivo {row_count} de {len(xmls_files)}', end = '\r')                
        except:
            app_logger.error(f"Arquivo não processado: {xml_file}")
    print('Leitura finalizada')
    return dataset


def clean_dataset(dataset):
    for linha in dataset:        
        if linha[2] == 'SEM GTIN':
            linha[2] = 0
        if linha[2] is None:
            linha[2] = 0
        linha[3] = linha[3].replace('\"','')


def export_dataset(dataset):
    with open('dataset.txt', 'w+') as arquivo_texto:
        for linha in dataset:            
            arquivo_texto.write(str(linha) + '\n')                        
            
    arquivo_texto.close()


def export_sql_command(dataset):

    with open('sql.txt', 'w+') as arquivo_texto:
        for linha in dataset:
            sql = f'INSERT INTO xml(cnpj, codfabrica, ean, descricao, unidade) values ({linha[0]}, "{linha[1]}", {linha[2]}, "{linha[3]}", "{linha[4]}")'
            arquivo_texto.write(str(sql) + '\n')                        
            
    arquivo_texto.close()


def write_internal_db(dataset):   
    c.execute('DELETE FROM xml')

    for linha in dataset:        
        sql = f'INSERT INTO xml(cnpj, codfabrica, ean, descricao, unidade) values ({linha[0]}, "{linha[1]}", {linha[2]}, "{linha[3]}", "{linha[4]}")'
        c.execute(sql)
        print(f'Processando linha {c.lastrowid} de {len(dataset)}', end = '\r')

    print('Gravação finalizada')
    connection.commit()

dataset = read_xml('xml')
clean_dataset(dataset)
write_internal_db(dataset)
connection.close()