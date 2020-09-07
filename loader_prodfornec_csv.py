import csv
import sqlite3
import re
from logger_settings import app_logger

connection = sqlite3.connect('database.db')
c = connection.cursor()

dataset = []

def read_csv(file_name):
    with open(file_name, newline='') as csv_file:
        file_content = csv.reader(csv_file, delimiter=',')
        for linha in file_content:
            dataset.append(linha)
    
    return dataset

def clean_csv(dataset):
    for linha in dataset:
        linha[1] = re.sub(u'[^0-9 ]', '', linha[1])
        linha[2] = linha[2].replace('\"','')
        linha[5] = linha[5].replace('\"','')
        if linha[4] == '':
           linha[4] = 0
        if linha[6] == '':
           linha[6] = 0
        if linha[7] == '':
           linha[7] = 0

def export_sql_command(dataset):
    with open('sql.txt', 'w+') as arquivo_texto:
        for linha in dataset:
            sql = f'INSERT INTO db(codfornec, cgc, fornecedor, codprod, codfab, descricao, codauxiliar, unidade) values ({linha[0]}, {linha[1]}, "{linha[2]}", {linha[3]}, "{linha[4]}", "{linha[5]}", {linha[6]}, "{linha[7]}");'        
            arquivo_texto.write(str(sql) + '\n')            
            
    arquivo_texto.close()

def write_internal_db(dataset):   
    c.execute('DELETE FROM prod_fornec')

    for linha in dataset:        
        sql = f'INSERT INTO prod_fornec(codfornec, cgc, fornecedor, codprod, codfab, descricao, codauxiliar, unidade) values ({linha[0]}, {linha[1]}, "{linha[2]}", {linha[3]}, "{linha[4]}", "{linha[5]}", {linha[6]}, "{linha[7]}")'
        try:
            c.execute(sql)
        except:
            app_logger.error(f"Erro executando comando: {sql}")    
    
    connection.commit()

    app_logger.info("Dados gravados com sucesso no banco de dados interno")     

dataset = read_csv('prodfornec.csv')
clean_csv(dataset)
#export_sql_command(dataset)
write_internal_db(dataset)
connection.close()

'''
Extract da PCPRODUT + PCFORNEC
SELECT f.codfornec
      ,f.cgc
      ,f.fornecedor
      ,p.codprod
      ,p.codfab
      ,p.descricao
      ,p.codauxiliar
      ,p.unidade
FROM pcprodut p, pcfornec f
WHERE p.codfornec = f.codfornec
'''