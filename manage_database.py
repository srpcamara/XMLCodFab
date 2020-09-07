import sqlite3

connection = sqlite3.connect('database.db')
c = connection.cursor()

def create_tables():
    c.execute('CREATE TABLE IF NOT EXISTS xml (cnpj integer, codfabrica text, ean integer, descricao text, unidade text)')
    c.execute('CREATE TABLE IF NOT EXISTS db (codfornec integer, cgc integer, fornecedor text, codprod integer, codfab text, descricao text, codauxiliar integer, unidade text)')
    c.execute('CREATE TABLE IF NOT EXISTS prod_fornec (codfornec integer, cgc integer, fornecedor text, codprod integer, codfab text, descricao text, codauxiliar integer, unidade text)')
    
def drop_table(tabela):
    c.execute(f'drop table if exists {tabela}')

def create_view_match():
    c.execute("CREATE VIEW IF NOT EXISTS vw_match as SELECT xml.cnpj AS '(XML) CNPJ', db.cgc AS '(DB) CNPJ', db.codfornec AS '(DB) Cod.Fornec', db.fornecedor AS '(DB) Fornecedor', xml.codfabrica AS '(XML) Cod.Fab', db.codfab AS '(DB) Cod.Fab', db.codprod AS '(DB) Cod.Prod', xml.descricao AS '(XML) Descrição', db.descricao AS '(DB) Descrição', xml.ean AS '(XML) EAN', db.codauxiliar AS '(DB) CODAUXILIAR', xml.unidade AS '(XML) Unidade', db.unidade AS '(DB) Unidade' FROM xml, db WHERE xml.cnpj = db.cgc AND xml.codfabrica = db.codfab")

def create_view_unmatch():
    c.execute("CREATE VIEW IF NOT EXISTS vw_unmatch as SELECT xml.cnpj AS '(XML) CNPJ', db.cgc AS '(DB) CNPJ', db.codfornec AS '(DB) Cod.Fornec', db.fornecedor AS '(DB) Fornecedor', xml.codfabrica AS '(XML) Cod.Fab', db.codfab AS '(DB) Cod.Fab', db.codprod AS '(DB) Cod.Prod', xml.descricao AS '(XML) Descrição', db.descricao AS '(DB) Descrição', xml.ean AS '(XML) EAN', db.codauxiliar AS '(DB) CODAUXILIAR', xml.unidade AS '(XML) Unidade', db.unidade AS '(DB) Unidade' FROM xml LEFT JOIN DB ON xml.cnpj = db.cgc AND xml.codfabrica = db.codfab WHERE (db.cgc IS NULL OR db.codprod IS NULL)")

create_tables()
create_view_match()
create_view_unmatch()
connection.close()