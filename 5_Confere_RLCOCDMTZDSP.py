#elton.mata@martins.com.br

#Importa as Bibliotecas
import pandas as pd
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

print('TOTAL PACOTES')
confere = df.groupby(['DESPCTOCD', 'CODCNOOCD'])[['VLRMOVCTB']].sum().unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
confere = confere.reset_index()
print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')

print('TOTAL MES')
confere = df.groupby(['NUMANOMES', 'CODCNOOCD'])[['VLRMOVCTB']].sum().unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
confere = confere.reset_index()
print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')