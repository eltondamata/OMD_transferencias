#elton.mata@martins.com.br
'''
Exclui os registros do DWH (Tabela RLCOCDMTZDSP) filtrando (cenarios, meses, pacotes e diretorias) que estao na base de carga (RLCOCDMTZDSP_CARGA.ft) 
e carrega os registros da base de carga
'''

import pandas as pd
from _parametros import WorkFolder
from sqlalchemy.types import String, Integer, Float
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn, connx
import time
start = time.strftime("%b %d %Y %H:%M:%S")

#importa a base de carga
dfcarga = pd.read_feather(WorkFolder + '/Datasets/RLCOCDMTZDSP_CARGA.ft')

#Inclui pacote na base de carga
mysql = ("SELECT CODCNTCTB, CODPCTOCD FROM DWH.EGIRLCPCTOCD WHERE CODGRPLIVCTB=462")
rlccntpct = pd.read_sql(mysql, con=conn)
dfcarga = pd.merge(dfcarga, rlccntpct, how='left', on='CODCNTCTB')
pacotes = tuple(dfcarga['CODPCTOCD'].unique()) #seleciona os pacotes que estao na base de carga
pacotes += (0,)

#Inclui Diretoria na base de carga
mysql = ("SELECT CODEDEOCDATU as CODEDEOCD, CODDRTORZATU, DESDRTORZATU FROM DWH.DIMEDEOCDATU")
dimedeocd = pd.read_sql(mysql, con=conn)
dfcarga = pd.merge(dfcarga, dimedeocd, how='left', on='CODEDEOCD')
diretorias = tuple(dfcarga['CODDRTORZATU'].unique()) #seleciona as diretorias que estao na base de carga
diretorias += (0,)

#seleciona os cenarios e os meses que estão na base de carga
cenarios = tuple(dfcarga['CODCNOOCD'].unique())
cenarios += ('NA',)
meses = tuple(dfcarga['NUMANOMES'].unique())
meses += (0,)

#DELETA OS REGISTROS DA TABELA DWH filtrando (cenarios, meses, pacotes e diretorias) que estao na base de carga
print('DELETE TABLE: DWH.RLCOCDMTZDSP')
print('  CENARIOS:', cenarios)
print('  MESES:', meses)
print('  PACOTES:', pacotes)
print('  DIRETORIAS:', diretorias)
cursor = conn.cursor()
sqldel = (f"""DELETE t1 
              FROM DWH.RLCOCDMTZDSP t1 INNER JOIN 
                   DWH.DIMEDEOCDATU ede ON t1.CODEDEOCD = ede.CODEDEOCDATU INNER JOIN 
                   DWH.EGIRLCPCTOCD pct ON t1.CODCNTCTB = pct.CODCNTCTB
             WHERE pct.CODGRPLIVCTB = 462
               AND t1.CODCNOOCD in {cenarios}  
               AND t1.NUMANOMES in {meses}
               AND pct.CODPCTOCD in {pacotes}
               AND ede.CODDRTORZATU in {diretorias}
           """)
cursor.execute(sqldel)
conn.commit()
cursor.close()
conn.close()
print('registros excluidos com sucesso!', '\n')

#Campos da tabela de carga (DWH.RLCOCDMTZDSP)
tipo_dados = {"NUMANOMES": Integer,
"CODCNOOCD": String,
"CODEDEOCD": Integer,
"TIPAPRCTB": Integer,
"CODGRPLIVCTB": Integer,
"CODCNTCTB": Integer,
"CODUNDNGCCTB": Integer,
"INDVGRCNOOCD": Integer,
"VLRMOVCTB": Float}

#Carrega os registros da base de carga no DWH
dfcarga = dfcarga[['NUMANOMES', 'CODCNOOCD', 'CODEDEOCD', 'TIPAPRCTB', 'CODGRPLIVCTB', 'CODCNTCTB', 'CODUNDNGCCTB', 'INDVGRCNOOCD', 'VLRMOVCTB']]
dfcarga.to_sql('rlcocdmtzdsp', connx, schema='dwh', if_exists='append', index=False, chunksize=100000, dtype=tipo_dados)
print('carga efetuada com sucesso!')
connx.dispose()
print(start, '--', time.strftime("%H:%M:%S"))