from loader_xml import read_xml
from logger_settings import app_logger

dataset = read_xml('C:\\Users\\sergio.camara\\Documents\\OneDrive\\Coding\\python\\XMLCodFab\\xml')

with open('sql.txt', 'w+') as arquivo_texto:
    for linha in dataset:
        sql = f'INSERT INTO SCCODFABRICA(CNPJ, CODFABRICA, EAN, DESCRICAO, UNIDADE) VALUES ({linha[0]}, "{linha[1]}", {linha[2]}, "{linha[3]}", "{linha[4]}");'
        arquivo_texto.write(str(sql) + '\n')            
            
arquivo_texto.close()

app_logger.info(f"Arquivo gerado: sql.txt")