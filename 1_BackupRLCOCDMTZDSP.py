#elton.mata@martins.com.br

#Importa as Bibliotecas
import pandas as pd
from _parametros import WorkFolder
import os
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn

filname = WorkFolder + '/Datasets/RLCOCDMTZDSP_bkp.ft'

if not os.path.exists(WorkFolder + '/Datasets'):
    os.makedirs(WorkFolder + '/Datasets')

#Código SQL
my_sql_query = ("""
SELECT * from dwh.RLCOCDMTZDSP
		""")
pd.read_sql(my_sql_query, con=conn).to_feather(filname)
conn.close()

print('Backup concluído:', filname)