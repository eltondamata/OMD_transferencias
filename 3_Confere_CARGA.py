#elton.mata@martins.com.br

#Importa as Bibliotecas
import pandas as pd
from _parametros import WorkFolder
pd.options.display.float_format = '{:,.2f}'.format
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn

#Dados atuais
mysql = ("""     
SELECT * from dwh.RLCOCDMTZDSP
  """)
df = pd.read_sql(mysql, con=conn)
#Relacao Pacote x Conta
mysql = (""" SELECT CODGRPLIVCTB, CODCNTCTB, CODPCTOCD, DATATURGT FROM DWH.EGIRLCPCTOCD WHERE CODGRPLIVCTB = 462 """)
rlccntpct = pd.read_sql(mysql, con=conn)
mysql = ("""SELECT DISTINCT CODPCTOCD, DESPCTOCD, DATDSTPCTOCD, DATATURGT FROM DWH.DIMPCTOCD""")
dimpct = pd.read_sql(mysql, con=conn)
dimcntpct = pd.merge(rlccntpct, dimpct, how='inner', on='CODPCTOCD')[['CODCNTCTB','CODPCTOCD','DESPCTOCD']]
df = pd.merge(df, dimcntpct, how='left', on='CODCNTCTB')
conn.close()
df.rename(columns={'VLRMOVCTB':'VALOR_ATUAL'}, inplace=True)

dfcarga = pd.read_feather(WorkFolder + '/Datasets/RLCOCDMTZDSP_CARGA.ft')
dfcarga = pd.merge(dfcarga, dimcntpct, how='left', on='CODCNTCTB')
dfcarga.rename(columns={'VLRMOVCTB':'VALOR_CARGA'}, inplace=True)

confere1 = dfcarga.groupby(['CODCNOOCD', 'NUMANOMES', 'DESPCTOCD'])[['VALOR_CARGA']].sum().reset_index()
confere2 = df.groupby(['CODCNOOCD', 'NUMANOMES', 'DESPCTOCD'])[['VALOR_ATUAL']].sum().reset_index()
confere = pd.merge(confere1, confere2, how='left')
confere.eval('VARIACAO=VALOR_CARGA-VALOR_ATUAL', inplace=True)

print('==> TRANSFERENCIAS -- PACOTES')
confere_pacote = confere.groupby(['CODCNOOCD', 'DESPCTOCD'])[['VALOR_ATUAL', 'VALOR_CARGA', 'VARIACAO']].sum().reset_index()
print(confere_pacote.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')

print('==> TRANSFERENCIAS -- MENSAL')
confere_mes = confere.groupby(['CODCNOOCD', 'NUMANOMES'])[['VALOR_ATUAL', 'VALOR_CARGA', 'VARIACAO']].sum().reset_index()
print(confere_mes.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')

print('==> TOTAL OMD -- PACOTES')
confere = df.groupby(['DESPCTOCD', 'CODCNOOCD'])[['VALOR_ATUAL']].sum().unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
confere = confere.reset_index()
print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')

print('==> TOTAL OMD -- MENSAL')
confere = df.groupby(['NUMANOMES', 'CODCNOOCD'])[['VALOR_ATUAL']].sum().unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
confere = confere.reset_index()
print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')
