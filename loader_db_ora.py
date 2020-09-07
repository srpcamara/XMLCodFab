import cx_Oracle

dsn_tns = cx_Oracle.makedsn('10.10.0.15', '1521', service_name='WINTHOR') 
conn = cx_Oracle.connect(user=r'WINTHOR', password='WINTHOR', dsn=dsn_tns)

dataset = []

def read():
    c = conn.cursor()

    c.execute('SELECT (CODPROD, CODFORNEC, CODFAB) FROM PCCODFABRICA') 
    for row in c:
        dataset.append(row[0], row[1], row[2])

    conn.close()

    return dataset