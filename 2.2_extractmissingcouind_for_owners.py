import pandas as pd
import os
import pickle

path = 'Data_actual_out/FINAL_nongranted_owners_country_industry.csv'

data = pd.read_csv(path)

list_all = data['owner'].unique().tolist()
print(len(list_all))

with open('owners_summary_stats.pkl', 'wb') as f:
    pickle.dump(list_all, f)


missing = data[data['ctryiso'].isnull() | data['nace2d'].isnull()]

path_out = 'Data_actual_out/tech/missing_owncouind.csv'

os.makedirs(os.path.dirname(path_out), exist_ok=True)

missing.to_csv(path_out, index=False)


