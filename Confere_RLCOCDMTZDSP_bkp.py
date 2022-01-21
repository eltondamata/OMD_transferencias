#elton.mata@martins.com.br

import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format
from _parametros import WorkFolder
    
#Confere dataset backup
df=pd.read_feather(WorkFolder + '/Datasets/RLCOCDMTZDSP_bkp.ft')
confere = df.groupby(['NUMANOMES', 'CODCNOOCD'])[['VLRMOVCTB']].sum().unstack()
confere.loc['TOTAL'] = confere.sum(axis=0, numeric_only=True)
print(confere) #confere total por mes

