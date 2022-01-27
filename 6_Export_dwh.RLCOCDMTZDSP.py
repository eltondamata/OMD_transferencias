#elton.mata@martins.com.br
'''
Exporta a tabela de carga OMD (dwh.RLCOCDMTZDSP)
'''
meses = (202201) #Selecionar os meses a exportar
#meses = (202201, 202202, 202203, 202204, 202205, 202206, 202207, 202208, 202209, 202210, 202211, 202212)

#Importa as Bibliotecas
import pandas as pd
from _parametros import WorkFolder
pd.options.display.float_format = '{:,.2f}'.format
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn

#Unifica base backup com base atual DWH
df = pd.read_sql(f'SELECT * from dwh.RLCOCDMTZDSP WHERE NUMANOMES IN {meses}', con=conn)

#Relacao Pacote x Conta
mysql = (""" SELECT CODGRPLIVCTB, CODCNTCTB, CODPCTOCD, DATATURGT FROM DWH.EGIRLCPCTOCD WHERE CODGRPLIVCTB = 462 """)
rlccntpct = pd.read_sql(mysql, con=conn)
mysql = ("""SELECT DISTINCT CODPCTOCD, DESPCTOCD, DATDSTPCTOCD, DATATURGT FROM DWH.DIMPCTOCD""")
dimpct = pd.read_sql(mysql, con=conn)
dimcntpct = pd.merge(rlccntpct, dimpct, how='inner', on='CODPCTOCD')[['CODCNTCTB','CODPCTOCD','DESPCTOCD']]

#inclui pacote na base de conferencia
df = pd.merge(df, dimcntpct, how='left', on='CODCNTCTB')

#Inclui diretoria na base de conferencia
mysql = ("SELECT CODEDEOCDATU as CODEDEOCD, CODDRTORZATU, DESDRTORZATU FROM DWH.DIMEDEOCDATU")
dimedeocd = pd.read_sql(mysql, con=conn)
df = pd.merge(df, dimedeocd, how='left', on='CODEDEOCD')
conn.close()

#Concatena codigo com descricao pacote e diretoria
df['PACOTE'] = list((''.join([x.zfill(3), '.', y])) for x, y in zip(df['CODPCTOCD'].astype(str),df['DESPCTOCD']))
df['DIRETORIA'] = list((''.join([x.zfill(3), '.', y])) for x, y in zip(df['CODDRTORZATU'].astype(str),df['DESDRTORZATU']))
df.drop(columns=['DESPCTOCD', 'DESDRTORZATU', 'TIPAPRCTB', 'CODUNDNGCCTB', 'INDVGRCNOOCD'], inplace=True)

df = pd.pivot_table(df, values='VLRMOVCTB', index=['NUMANOMES', 'CODEDEOCD', 'CODGRPLIVCTB', 'CODCNTCTB', 'CODPCTOCD', 'CODDRTORZATU', 'PACOTE', 'DIRETORIA'],  columns=['CODCNOOCD'], aggfunc=sum).reset_index()
df.columns.name = None

df.to_csv(f'{WorkFolder}/dwh.RLCOCDMTZDSP.csv', sep=";", encoding="iso-8859-1", decimal=",", float_format='%.2f', date_format='%d/%m/%Y', index=False)
print(f'Arquivo salvo em {WorkFolder}/dwh.RLCOCDMTZDSP.csv' )