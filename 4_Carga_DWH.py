#Atualiza tabela DWH.RLCOCDMTZDSP

import pandas as pd
from _parametros import WorkFolder
from sqlalchemy.types import String, Integer, Float
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn, connx
import time

start = time.strftime("%b %d %Y %H:%M:%S")
arquivo_ft = WorkFolder + '/Datasets/RLCOCDMTZDSP_CARGA.ft'
df = pd.read_feather(arquivo_ft)

#Relacao Pacote x Conta
mysql = (""" SELECT CODGRPLIVCTB, CODCNTCTB, CODPCTOCD, DATATURGT FROM DWH.EGIRLCPCTOCD WHERE CODGRPLIVCTB = 462 """)
rlccntpct = pd.read_sql(mysql, con=conn)
mysql = ("""SELECT DISTINCT CODPCTOCD, DESPCTOCD, DATDSTPCTOCD, DATATURGT FROM DWH.DIMPCTOCD""")
dimpct = pd.read_sql(mysql, con=conn)
dimcntpct = pd.merge(rlccntpct, dimpct, how='inner', on='CODPCTOCD')[['CODCNTCTB','CODPCTOCD','DESPCTOCD']]
df = pd.merge(df, dimcntpct, how='left', on='CODCNTCTB')

#Filtra todas as contas dos pacotes que serao excluidos e carregados no DWH
pacotes = list(df['CODPCTOCD'].unique())
contas = tuple(dimcntpct.query(f'CODPCTOCD in {pacotes}')['CODCNTCTB'])
contas_delete = tuple(contas[x:x+10] for x in range(0, len(contas), 10)) #split contas em grupos de 10 em 10
cenarios = tuple(df['CODCNOOCD'].unique())
cenarios += ('',)
meses = tuple(df['NUMANOMES'].unique())
meses += (0,)

#Conferencia Quantidade de registros que serao excluidos da tabela
#print('Qde de Registros na tabela antes das exclusoes')
mysql = (f"""
        SELECT * FROM DWH.RLCOCDMTZDSP 
         WHERE NUMANOMES in {meses}
          AND CODCNOOCD in {cenarios}
          AND CODCNTCTB in {contas}     
		""")
df = pd.read_sql(mysql, con=conn)
print(df.nunique())
df.reset_index(drop=True, inplace=True)
df.to_feather(WorkFolder + '/Datasets/RLCOCDMTZDSP_DELETADOS.ft')
qderegdel = len(df)

##Deleta os registros da tabela
print('DELETE TABLE: DWH.RLCOCDMTZDSP')
print('  CENARIOS:', cenarios)
print('  MESES:', meses)
cursor = conn.cursor()
for GrupoContas in contas_delete:
    print('  CONTAS:', GrupoContas)
    sqldel = (f"""DELETE FROM DWH.RLCOCDMTZDSP
                   WHERE NUMANOMES in {meses}
                     AND CODCNOOCD in {cenarios}
                     AND CODCNTCTB in {GrupoContas}
               """)
    cursor.execute(sqldel)
    conn.commit()
cursor.close()
print('Quantidade Registros excluidos:', qderegdel)
print('registros excluidos salvos em:', WorkFolder + '/Datasets/RLCOCDMTZDSP_DELETADOS.ft', '\n')

#Conferencia da tabela apos exclusao dos registros      
#print('\n Qde de Registros na tabela apos exclusoes')
mysql = (f"""
        SELECT * FROM DWH.RLCOCDMTZDSP 
         WHERE NUMANOMES in {meses}
          AND CODCNOOCD in {cenarios}
          AND CODCNTCTB in {contas}     
		""")
df = pd.read_sql(mysql, con=conn)
conn.close()
print(df.nunique(), '\n')

#['NUMANOMES', 'CODCNOOCD', 'CODEDEOCD', 'TIPAPRCTB', 'CODGRPLIVCTB', 'CODCNTCTB', 'CODUNDNGCCTB', 'INDVGRCNOOCD', 'VLRMOVCTB']
tipo_dados = {"NUMANOMES": Integer,
"CODCNOOCD": String,
"CODEDEOCD": Integer,
"TIPAPRCTB": Integer,
"CODGRPLIVCTB": Integer,
"CODCNTCTB": Integer,
"CODUNDNGCCTB": Integer,
"INDVGRCNOOCD": Integer,
"VLRMOVCTB": Float}

#Carrega os registros do arquivo no DWH
df.to_sql('rlcocdmtzdsp', connx, schema='dwh', if_exists='append', index=False, chunksize=100000, dtype=tipo_dados)
print('carga efetuada com sucesso!')
connx.dispose()
print(start, '--', time.strftime("%H:%M:%S"))