import cx_Oracle
import sqlite3
import re
from logger_settings import app_logger

try:
    dsn_tns = cx_Oracle.makedsn('10.10.0.15', '1521', service_name='WINTHOR') 
    connection_ora = cx_Oracle.connect(user=r'WINTHOR', password='WINTHOR', dsn=dsn_tns)
    c_ora = connection_ora.cursor()
    print('Conectado ao banco Oracle')
except Exception as e:
    app_logger.error(f'Erro ao conectar no banco de dados. Erro original: {e}')

connection_local = sqlite3.connect('database.db')
c_local = connection_local.cursor()

dataset = []

def read_db():
    
    c_ora.execute('SELECT COUNT(0) FROM pcprodut p, pcfornec f WHERE p.codfornec = f.codfornec') 
    total_row = c_ora.fetchone()
    
    c_ora.execute('SELECT f.codfornec, f.cgc, f.fornecedor, p.codprod, p.codfab, p.descricao, nvl(p.codauxiliar,0), p.unidade FROM pcprodut p, pcfornec f WHERE p.codfornec = f.codfornec') 
       
    app_logger.info('Importando dados do banco Oracle')
    for row in c_ora:
        dataset.append([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]])   
        print(f'Processando linha {len(dataset)} de {total_row[0]}', end = '\r')

    print('\r')
    app_logger.info('Leitura da tabela Oracle concluída')
    return dataset
    
def clean_dataset(dataset):
    for linha in dataset:
        linha[1] = re.sub(u'[^0-9 ]', '', linha[1])
        linha[2] = linha[2].replace('\"','')
        linha[5] = linha[5].replace('\"','')


def write_internal_db(dataset):   
    c_local.execute('DELETE FROM prod_fornec')

    for linha in dataset:        
        sql = f'INSERT INTO prod_fornec(codfornec, cgc, fornecedor, codprod, codfab, descricao, codauxiliar, unidade) values ({linha[0]}, {linha[1]}, "{linha[2]}", {linha[3]}, "{linha[4]}", "{linha[5]}", {linha[6]}, "{linha[7]}")'
        try:
            c_local.execute(sql)
            print(f'Gravando linha {c_local.lastrowid} de {len(dataset)}', end = '\r')
        except Exception as e:
            app_logger.error(f'Erro ao conectar no banco de dados. Erro original: {e}')
    
    connection_local.commit()

    print('\r')
    app_logger.info("Dados gravados com sucesso no banco de dados interno")

dataset = read_db()
clean_dataset(dataset)
write_internal_db(dataset)
connection_ora.close()
connection_local.close()