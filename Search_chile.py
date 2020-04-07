import pandas as pd
import os
from pathlib import Path
import live_data as ld
import Requests_CL as rcl

#############



#############
#Testing how pandas and the table works

#start='2018/03'
datafold='/Data/Chile/'
file_name='Database_Chile_Since_03-2018.csv'

df=rcl.CL.read_data(file_name,datafold)
print(df.loc[:, ['revenue','Date','TICKER']])
