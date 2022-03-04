# rn i'm using this to generate a list and copy paste it to processing.py sketch

import csv_to_pandas as ctp
import numpy as np
from matplotlib import pyplot as plt
import pandas as pd
import pprint
import pyperclip
pd.set_option('display.expand_frame_repr', False)

smt = ctp.smtData(pd.read_csv("Data\\09 5 June 2019.csv"))
smt = smt.get_time_sheet_data(('Rest', 'Sleep'))

print(smt)


# a = []
# for i in range(0,340):
#     x = smt[smt['day_from_start'] == i]['start_end_min']
#     if(x.empty):
#         a.append([])
#     else:
#         a.append(x[0])
#     pass

# assert (len(a)==smt['day_from_start'].max()+1)