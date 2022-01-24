#elton.mata@martins.com.br

import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format
from _parametros import WorkFolder, ANOMES, CODCNOOCD, ArquivoExcel

#['pacote', 'DESC.CONTA', 'CONTA', 'ENTIDADE', 'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
df=pd.read_excel(WorkFolder + '/' + ArquivoExcel[0], sheet_name=ArquivoExcel[1], usecols = "A,C:P")

df = pd.melt(df, id_vars=['pacote', 'CONTA', 'ENTIDADE'], #variaveis continuam nas colunas
  value_vars=list(df.columns[-12:]), #colunas NUMANOMES transpostas para linhas
  var_name='NUMANOMES', #nome da nova coluna
  value_name='VLRMOVCTB') #valor das colunas transpostas
df.rename(columns={'CONTA':'CODCNTCTB', 'ENTIDADE':'CODEDEOCD'}, inplace=True)
df = df.query(f'NUMANOMES in {ANOMES}')
df = df[~df['CODCNTCTB'].isnull()]

#Replica os dados para todos os cenarios definidos na parametrizacao
dfcno = pd.DataFrame({'CODCNOOCD': pd.Series(CODCNOOCD)})
df = pd.merge(df, dfcno, how='cross')

#define parametros fixos na carga
df['TIPAPRCTB'] = 3
df['CODGRPLIVCTB'] = 36
df['CODUNDNGCCTB'] = 1
df['INDVGRCNOOCD'] = 1

df = df[['pacote', 'NUMANOMES', 'CODCNOOCD', 'CODEDEOCD', 'TIPAPRCTB', 'CODGRPLIVCTB', 'CODCNTCTB', 'CODUNDNGCCTB', 'INDVGRCNOOCD', 'VLRMOVCTB']]

#Conferencia
print('Conferencia TOTAL ARQUIVO DE CARGA')
confere = df.groupby(['pacote', 'NUMANOMES', 'CODCNOOCD'])[['VLRMOVCTB']].sum().reset_index()
#confere.eval('MESCNOOCD=NUMANOMES.astype(str) + CODCNOOCD', inplace=True)
confere['MESCNOOCD'] = confere['NUMANOMES'].astype(str) + "_" + confere['CODCNOOCD']
confere = confere.groupby(['pacote', 'MESCNOOCD'])[['VLRMOVCTB']].sum().unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
print(confere)

#Export dataset carga
#['NUMANOMES', 'CODCNOOCD', 'CODEDEOCD', 'TIPAPRCTB', 'CODGRPLIVCTB', 'CODCNTCTB', 'CODUNDNGCCTB', 'INDVGRCNOOCD', 'VLRMOVCTB']
df.to_feather(WorkFolder + '/Datasets/RLCOCDMTZDSP_CARGA.ft')
