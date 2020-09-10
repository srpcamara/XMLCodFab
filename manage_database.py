import sqlite3

connection = sqlite3.connect('database.db')
c = connection.cursor()

def create_tables():
    c.execute('CREATE TABLE IF NOT EXISTS xml (numnota integer, cnpj integer, codfabrica text, ean integer, descricao text, unidade text)')

    c.execute('CREATE TABLE IF NOT EXISTS db (codfornec integer, cgc integer, fornecedor text, codprod integer, codfab text, descricao text, codauxiliar integer, unidade text, fator integer)')

    c.execute('CREATE TABLE IF NOT EXISTS prod_fornec (codfornec integer, cgc integer, fornecedor text, codprod integer, codfab text, descricao text, codauxiliar integer, unidade text)')
    
def drop_table(tabela):
    c.execute(f'drop table if exists {tabela}')

def create_view_match():
    c.execute("CREATE VIEW IF NOT EXISTS vw_match as SELECT xml.cnpj AS xml_cnpj, db.cgc AS db_cgc, db.codfornec AS db_codfornec, db.fornecedor AS db_fornecedor, xml.codfabrica AS xml_codfabrica, db.codfab AS db_codfab, db.codprod AS db_codprod, xml.descricao AS xml_descricao, db.descricao AS db_descricao, xml.ean AS xml_ean, db.codauxiliar AS db_codauxiliar, xml.unidade AS xml_unidade, db.unidade AS db_unidade, db.fator as db_fator FROM xml, db WHERE xml.cnpj = db.cgc AND xml.codfabrica = db.codfab")

def create_view_unmatch():
    c.execute("CREATE VIEW IF NOT EXISTS vw_unmatch as SELECT xml.cnpj AS xml_cnpj, db.cgc AS db_cgc, db.codfornec AS db_codfornec, db.fornecedor AS db_fornecedor, xml.codfabrica AS xml_codfabrica, db.codfab AS db_codfab, db.codprod AS db_codprod, xml.descricao AS xml_descricao, db.descricao AS db_descricao, xml.ean AS xml_ean, db.codauxiliar AS db_codauxiliar, xml.unidade AS xml_unidade, db.unidade AS db_unidade, db.fator as db_fator FROM xml LEFT JOIN DB ON xml.cnpj = db.cgc AND xml.codfabrica = db.codfab WHERE (db.cgc IS NULL OR db.codprod IS NULL)")


def drop_view(view):
    c.execute(f'drop view if exists {view}')

create_tables()
create_view_match()
create_view_unmatch()
connection.close()

