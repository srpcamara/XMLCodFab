import xml.etree.ElementTree as ET
import sqlite3
import cx_Oracle
from os import path, listdir
from logger_settings import app_logger


def connect_db_local():
    try:
        global connection_local

        connection_local = sqlite3.connect('database.db')        
        app_logger.info('Conectado ao banco de dados local.')

    except Exception as e:
        app_logger.error(f'Erro ao conectar no banco de dados. Erro original: {e}')


def connect_db_ora():
    try:
        global connection_ora

        dsn_tns = cx_Oracle.makedsn('10.10.0.15', '1521', service_name='WINTHOR') 
        connection_ora = cx_Oracle.connect(user=r'WINTHOR', password='WINTHOR', dsn=dsn_tns)
        app_logger.info('Conectado ao banco de dados Oracle')

    except Exception as e:
        app_logger.error(f'Erro ao conectar no banco de dados. Erro original: {e}')


def read_xml_db():    
    cursor_ora = connection_ora.cursor()

    data_inicio = '04/09/2020'
    data_fim = '04/09/2020'

    cursor_ora.execute(f"SELECT COUNT(0) FROM PCNFENTXML WHERE dtemissao BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY')")
    total_linhas = cursor_ora.fetchone()[0]

    cursor_ora.execute(f"SELECT numnota, dadosxml FROM PCNFENTXML WHERE dtemissao BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY') ORDER BY dtemissao ASC")
    
    namespace = "{http://www.portalfiscal.inf.br/nfe}"
    dataset = []
    row_count = 0
    
    try:
        for row in cursor_ora:
            xml_blob = row[1].read()
            row_count += 1
            num_nota = row[0]

            if len(xml_blob) > 50:
                tree = ET.ElementTree(ET.fromstring(xml_blob))
                root = tree.getroot()               

                try:
                    cnpj_origem = root[0][0][1][0].text

                    for x in root[0][0].findall(f'{namespace}det'):              
                        dataset.append([
                            num_nota,
                            cnpj_origem,
                            x[0].find(f'{namespace}cProd').text,
                            x[0].find(f'{namespace}cEAN').text,
                            x[0].find(f'{namespace}xProd').text,
                            x[0].find(f'{namespace}uCom').text                            
                        ])                                                 
                except:
                    app_logger.error(f"Nota não processada: {row[0]}")

            print(f'Lendo arquivo {row_count} de {total_linhas}', end = '\r')  
    except:
        app_logger.error(f"Nota não processada: {row[0]}")
        
    print('\r')
    app_logger.info("Leitura finalizada")

    return dataset


def clean_dataset(dataset):
    for linha in dataset:        
        if linha[3] == 'SEM GTIN':
            linha[3] = 0
        if linha[3] is None:
            linha[3] = 0
        linha[4] = linha[4].replace('\"','')


def export_dataset(dataset):
    with open('dataset.txt', 'w+') as arquivo_texto:
        for linha in dataset:            
            arquivo_texto.write(str(linha) + '\n')                        
            
    arquivo_texto.close()


def export_sql_command(dataset):

    with open('sql.txt', 'w+') as arquivo_texto:
        for linha in dataset:
            sql = f'INSERT INTO xml(numnota, cnpj, codfabrica, ean, descricao, unidade) values ({linha[0]}, "{linha[1]}", {linha[2]}, "{linha[3]}", "{linha[4]}")'
            arquivo_texto.write(str(sql) + '\n')                        
            
    arquivo_texto.close()


def write_internal_db(dataset):   
    cursor_local = connection_local.cursor()
    cursor_local.execute('DELETE FROM xml')

    for linha in dataset:        
        sql = f'INSERT INTO xml(numnota, cnpj, codfabrica, ean, descricao, unidade) values ({linha[0]}, {linha[1]}, "{linha[2]}", {linha[3]}, "{linha[4]}", "{linha[5]}")'
        cursor_local.execute(sql)
        print(f'Processando linha {cursor_local.lastrowid} de {len(dataset)}', end = '\r')

    print('\r')
    app_logger.info("Gravação no banco interno finalizada")
    connection_local.commit()


def execute():
    connect_db_local()
    connect_db_ora()

    dataset = read_xml_db()

    clean_dataset(dataset)
    write_internal_db(dataset)
    #export_sql_command(dataset)

execute()