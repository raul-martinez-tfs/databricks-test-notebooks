from os import system, listdir
import numpy as np
import json
import pandas as pd

fp = '/Users/raul.martinez3/Documents/outputs/'

# for off in np.arange(0,10000, 1000):
# 	print(off)
# 	system(f'databricks runs list --output JSON --limit 1000 --offset {off} > {fp}/runs-output-{off}.json')

df = pd.DataFrame()
for i in [i for i in listdir(fp) if i.endswith('.json')]:
	print(i)
	with open(f'{fp}{i}') as json_file:
	    data = json.load(json_file)
	 
	    # print(len(data['runs']))
	    for j in data['runs']:
	    	df = pd.concat([df, pd.DataFrame(j)])

	    break
	    
print(df.head())

