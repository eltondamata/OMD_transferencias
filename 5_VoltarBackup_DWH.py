#elton.mata@martins.com.br
'''
Substitui todos os dados da tabela DWH.RLCOCDMTZDSP pelo arquivo de backup
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
dfcarga = pd.read_feather(WorkFolder + '/Datasets/bkp.RLCOCDMTZDSP_20220120.ft')

#seleciona os cenarios e os meses que estão na base de carga
cenarios = tuple(dfcarga['CODCNOOCD'].unique())
cenarios += ('NA',)
meses = tuple(dfcarga['NUMANOMES'].unique())
meses += (0,)

#DELETA OS REGISTROS DA TABELA DWH filtrando (cenarios, meses) que estao na base de carga
print('DELETE TABLE: DWH.RLCOCDMTZDSP')
print('  CENARIOS:', cenarios)
print('  MESES:', meses)
cursor = conn.cursor()
sqldel = (f"""DELETE FROM DWH.RLCOCDMTZDSP 
               WHERE CODCNOOCD in {cenarios}  
                 AND NUMANOMES in {meses}
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