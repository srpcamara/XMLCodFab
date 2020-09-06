
import xml.etree.ElementTree as ET

tree = ET.parse('26200837714567000130550010000000311921428000_nfeViz.xml')
root = tree.getroot()
namespace = "{http://www.portalfiscal.inf.br/nfe}"

'''
def elementos(i):
    switcher = {
        'cProd': '{http://www.portalfiscal.inf.br/nfe}cProd',
        'cEAN':  '{http://www.portalfiscal.inf.br/nfe}cEAN',
        'xProd': '{http://www.portalfiscal.inf.br/nfe}xProd',
        'uCom': '{http://www.portalfiscal.inf.br/nfe}uCom',
        'qCom': '{http://www.portalfiscal.inf.br/nfe}qCom',
        'vUnCom': '{http://www.portalfiscal.inf.br/nfe}vUnCom'
    }   
'''

dataset = {}
codfab = ''
cnpj = ''
ean = ''
desc = ''
unidade = ''
qtd = ''
valor_unit = ''

cpf = ET.parse('26200837714567000130550010000000311921428000_nfeViz.xml').findall('.//emit')
print(cpf)

for xml_prod in root.findall(f'./{namespace}NFe/{namespace}infNFe/{namespace}emit/{namespace}CNPJ'):
    cnpj = xml_prod.text    

for xml_prod in root.findall(f'./{namespace}NFe/{namespace}infNFe/{namespace}det/{namespace}prod/'):
    
    if xml_prod.tag == f'{namespace}cProd': 
        codfab = xml_prod.text

    if xml_prod.tag == f'{namespace}cEAN': 
        ean = xml_prod.text    

    if xml_prod.tag == f'{namespace}xProd': 
        desc = xml_prod.text

    if xml_prod.tag == f'{namespace}uCom': 
        unidade = xml_prod.text

    if xml_prod.tag == f'{namespace}qCom': 
        qtd = xml_prod.text

    if xml_prod.tag == f'{namespace}vUnCom': 
        valor_unit = xml_prod.text
    
    dataset[codfab] = [cnpj, ean, desc, unidade, qtd, valor_unit]   


#print(dataset)



