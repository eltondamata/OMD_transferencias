#elton.mata@martins.com.br

import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format
from _parametros import WorkFolder, ANOMES, CODCNOOCD, ArquivoExcel
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn

#==> PROCESSO DE IMPORTACAO DO ARQUIVO DE CARGA
df=pd.read_excel(WorkFolder + '/' + ArquivoExcel[0], sheet_name=ArquivoExcel[1], usecols = "A,C:P")

#Formata arquivo no mesmo layout da tabela dwh
df = pd.melt(df, id_vars=['pacote', 'CONTA', 'ENTIDADE'], #variaveis continuam nas colunas
  value_vars=list(df.columns[-12:]), #colunas NUMANOMES transpostas para linhas
  var_name='NUMANOMES', #nome da nova coluna
  value_name='VLRMOVCTB') #valor das colunas transpostas
df.rename(columns={'CONTA':'CODCNTCTB', 'ENTIDADE':'CODEDEOCD'}, inplace=True)
df = df.query(f'NUMANOMES in {ANOMES}')
df = df[~df['CODCNTCTB'].isnull()]

#replica os dados para todos os cenarios
dfcno = pd.DataFrame({'CODCNOOCD': pd.Series(CODCNOOCD)})
df = pd.merge(df, dfcno, how='cross')

#inclui parametros fixos na base de carga
df['TIPAPRCTB'] = 3
df['CODGRPLIVCTB'] = 36
df['CODUNDNGCCTB'] = 1
df['INDVGRCNOOCD'] = 1
df = df[['pacote', 'NUMANOMES', 'CODCNOOCD', 'CODEDEOCD', 'TIPAPRCTB', 'CODGRPLIVCTB', 'CODCNTCTB', 'CODUNDNGCCTB', 'INDVGRCNOOCD', 'VLRMOVCTB']]

#==>PROCESSO DE CONFERENCIA VERSUS A BASE ATUAL
#Arquivo de carga
dfcarga = df.copy()
dfcarga.rename(columns={'VLRMOVCTB':'VALOR_CARGA'}, inplace=True)

#Relacao Conta x  Pacote
mysql = (""" SELECT CODGRPLIVCTB, CODCNTCTB, CODPCTOCD, DATATURGT FROM DWH.EGIRLCPCTOCD WHERE CODGRPLIVCTB = 462 """)
rlccntpct = pd.read_sql(mysql, con=conn)
mysql = ("""SELECT DISTINCT CODPCTOCD, DESPCTOCD, DATDSTPCTOCD, DATATURGT FROM DWH.DIMPCTOCD""")
dimpct = pd.read_sql(mysql, con=conn)
dimcntpct = pd.merge(rlccntpct, dimpct, how='inner', on='CODPCTOCD')[['CODCNTCTB','CODPCTOCD','DESPCTOCD']]

#Inclui Pacote na base de carga
dfcarga = pd.merge(dfcarga, dimcntpct, how='left', on='CODCNTCTB')
pacotes = tuple(dfcarga['CODPCTOCD'].unique()) #seleciona os pacotes que estao no aruqivo de carga
pacotes += (0,)

#Inclui Diretoria na base de carga
mysql = ("SELECT CODEDEOCDATU as CODEDEOCD, CODDRTORZATU, DESDRTORZATU FROM DWH.DIMEDEOCDATU")
dimedeocd = pd.read_sql(mysql, con=conn)
dfcarga = pd.merge(dfcarga, dimedeocd, how='left', on='CODEDEOCD')
diretorias = tuple(dfcarga['CODDRTORZATU'].unique()) #seleciona as diretorias que estao no aruqivo de carga
diretorias += (0,)

#seleciona os cenarios e os meses que estão na base de carga
cenarios = tuple(dfcarga['CODCNOOCD'].unique()) #cenarios que estao na base de carga
cenarios += ('NA',)
meses = tuple(dfcarga['NUMANOMES'].unique()) #meses da base de carga
meses += (0,)

#Base atual que sera substituida pelo arquivo de carga
mysql = (f""" 
            SELECT t1.* 
              FROM DWH.RLCOCDMTZDSP t1 INNER JOIN 
                   DWH.DIMEDEOCDATU ede ON t1.CODEDEOCD = ede.CODEDEOCDATU INNER JOIN 
                   DWH.EGIRLCPCTOCD pct ON t1.CODCNTCTB = pct.CODCNTCTB
             WHERE pct.CODGRPLIVCTB = 462
               AND t1.CODCNOOCD in {cenarios}  
               AND t1.NUMANOMES in {meses}
               AND pct.CODPCTOCD in {pacotes}
               AND ede.CODDRTORZATU in {diretorias}
           """)
dfatual = pd.read_sql(mysql, con=conn)
dfatual.rename(columns={'VLRMOVCTB':'VALOR_ATUAL'}, inplace=True)
conn.close()

#Unifica base de carga com base atual para conferencia
dfconfere = pd.merge(dfcarga, dfatual, how='outer').fillna(0)

#Atualiza Descricao Diretoria e Pacotes na base de conferencia
dfconfere.drop(columns=['CODPCTOCD', 'DESPCTOCD', 'CODDRTORZATU', 'DESDRTORZATU'], inplace=True)
dfconfere = pd.merge(dfconfere, dimcntpct, how='left', on='CODCNTCTB')
dfconfere = pd.merge(dfconfere, dimedeocd, how='left', on='CODEDEOCD')

#Concatena codigo com descricao pacote e diretoria
dfconfere['PACOTE'] = list((''.join([x.zfill(3), '.', y])) for x, y in zip(dfconfere['CODPCTOCD'].astype(str),dfconfere['DESPCTOCD']))
dfconfere['DIRETORIA'] = list((''.join([x.zfill(3), '.', y])) for x, y in zip(dfconfere['CODDRTORZATU'].astype(str),dfconfere['DESDRTORZATU']))
dfconfere.drop(columns=['CODPCTOCD', 'DESPCTOCD', 'CODDRTORZATU', 'DESDRTORZATU'], inplace=True)

#Conferencia alterações por PACOTE
print('==> ALTERACOES POR PACOTE')
confere = dfconfere.groupby(['PACOTE', 'CODCNOOCD'])[['VALOR_ATUAL', 'VALOR_CARGA']].sum()
confere.eval('VARIACAO=VALOR_CARGA-VALOR_ATUAL', inplace=True)
confere = confere.unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
confere = confere.reset_index()
print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')

#Conferencia alterações por DIRETORIA
print('==> ALTERACOES POR DIRETORIA')
confere = dfconfere.groupby(['DIRETORIA', 'CODCNOOCD'])[['VALOR_ATUAL', 'VALOR_CARGA']].sum()
confere.eval('VARIACAO=VALOR_CARGA-VALOR_ATUAL', inplace=True)
confere = confere.unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
confere = confere.reset_index()
print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')

#Conferencia alterações por MES
print('==> ALTERACOES POR MES')
confere = dfconfere.groupby(['NUMANOMES', 'CODCNOOCD'])[['VALOR_ATUAL', 'VALOR_CARGA']].sum()
confere.eval('VARIACAO=VALOR_CARGA-VALOR_ATUAL', inplace=True)
confere = confere.unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
confere = confere.reset_index()
print(confere.to_markdown(index=False, tablefmt='github', floatfmt=',.2f', numalign='right'), '\n')

#EXPORTA TABELA DE CARGA
#['NUMANOMES', 'CODCNOOCD', 'CODEDEOCD', 'TIPAPRCTB', 'CODGRPLIVCTB', 'CODCNTCTB', 'CODUNDNGCCTB', 'INDVGRCNOOCD', 'VLRMOVCTB']
df.to_feather(WorkFolder + '/Datasets/RLCOCDMTZDSP_CARGA.ft')