import cx_Oracle


dsn_tns = cx_Oracle.makedsn('10.10.0.15', '1521', service_name='WINTHOR') 
conn = cx_Oracle.connect(user=r'WINTHOR', password='WINTHOR', dsn=dsn_tns)

c = conn.cursor()

c.execute('select * from pccaixa') 
for row in c:
    #print (row[0], '-', row[1]) 
    print (type(row)) 

conn.close()