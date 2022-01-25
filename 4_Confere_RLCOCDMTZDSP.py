#elton.mata@martins.com.br
'''
Confere os valores por Pacote, Diretoria e Mes da tabela de carga do OMD (dwh.RLCOCDMTZDSP) com a base de backup
'''

#Importa as Bibliotecas
import pandas as pd
from _parametros import WorkFolder
pd.options.display.float_format = '{:,.2f}'.format
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn

#importa arquivo de backup
df = pd.read_feather(WorkFolder + '/Datasets/bkp.RLCOCDMTZDSP_20220120.ft')
df.rename(columns={'VLRMOVCTB':'VLRBKP'}, inplace=True)

#Unifica base backup com base atual DWH
df = pd.merge(pd.read_sql('SELECT * from dwh.RLCOCDMTZDSP', con=conn), df, how='outer').fillna(0)
df.rename(columns={'VLRMOVCTB':'VLRATU'}, inplace=True)

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
df.drop(columns=['CODPCTOCD', 'DESPCTOCD', 'CODDRTORZATU', 'DESDRTORZATU'], inplace=True)

#==> CONFERENCIA TABELA DWH VERSUS BACKUP
cenarios = list(df['CODCNOOCD'].unique())
for i in range(len(cenarios)):
    print('='*100, '\n')
    #Conferencia alterações por PACOTE
    print('==> ALTERACOES POR PACOTE ( CENARIO =', cenarios[i], ')')
    confere = df.query(f'CODCNOOCD==CODCNOOCD[{i}]').groupby(['PACOTE'])[['VLRATU', 'VLRBKP']].sum()
    confere.eval('VARIACAO=VLRBKP-VLRATU', inplace=True)
    confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
    confere = confere.reset_index()
    print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')
    
    #Conferencia alterações por DIRETORIA
    print('==> ALTERACOES POR DIRETORIA ( CENARIO =', cenarios[i], ')')
    confere = df.query(f'CODCNOOCD==CODCNOOCD[{i}]').groupby(['DIRETORIA'])[['VLRATU', 'VLRBKP']].sum()
    confere.eval('VARIACAO=VLRBKP-VLRATU', inplace=True)
    confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
    confere = confere.reset_index()
    print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')
    
    #Conferencia alterações por MES
    print('==> ALTERACOES POR MES ( CENARIO =', cenarios[i], ')')
    confere = df.query(f'CODCNOOCD==CODCNOOCD[{i}]').groupby(['NUMANOMES'])[['VLRATU', 'VLRBKP']].sum()
    confere.eval('VARIACAO=VLRBKP-VLRATU', inplace=True)
    confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
    confere = confere.reset_index()
    print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')
    print('\n')
    
