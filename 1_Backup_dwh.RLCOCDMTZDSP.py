#elton.mata@martins.com.br
'''
Salva arquivo de backup completo da tabela dwh.RLCOCDMTZDSP
'''

#Importa as Bibliotecas
import pandas as pd
from datetime import date
from _parametros import WorkFolder
import os
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn

ANOMESDIAATU = (date.today()).strftime("%Y%m%d") #Data Atual AAAAMMDD

filname = WorkFolder + f'/Datasets/bkp.RLCOCDMTZDSP_{ANOMESDIAATU}.ft'

if not os.path.exists(WorkFolder + '/Datasets'):
    os.makedirs(WorkFolder + '/Datasets')

#Código SQL
my_sql_query = ("""
SELECT * from dwh.RLCOCDMTZDSP
		""")
pd.read_sql(my_sql_query, con=conn).to_feather(filname)
conn.close()

print('Backup concluído:', filname)