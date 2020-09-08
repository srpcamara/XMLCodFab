import xlsxwriter
import sqlite3

def export_db(view_name):
    connection = sqlite3.connect('database.db')
    c = connection.cursor()
    dataset = []    

    c.execute(f'SELECT * from {view_name}')
    dataset.append(list(map(lambda x: x[0], c.description)))

    for linha in c:        
        dataset.append([linha[0], linha[1], linha[2], linha[3], linha[4], linha[5], linha[6]
                    , linha[7], linha[8], linha[9], linha[10], linha[11], linha[12], linha[13]])   
        print(f'Processando linha {len(dataset)}', end = '\r')

    with xlsxwriter.Workbook(f'{view_name}.xlsx') as workbook:
        worksheet = workbook.add_worksheet()

        for linha_num, data in enumerate(dataset):
            worksheet.write_row(linha_num, 0, data)
        
        print('Arquivo gerado.')
    
    c = connection.close()

export_db('vw_unmatch')
