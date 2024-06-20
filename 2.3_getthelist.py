import pandas as pd
import pickle
from numpy import savetxt

# Define the paths
csv_path = 'Data_actual_out/tech/missing_owncouind.csv'
pickle_path = 'owners_summary_stats.pkl'

# Load the missing data CSV file
missing_couind = pd.read_csv(csv_path)

# Get the unique owners from the missing data
list_missing = missing_couind['owner'].unique().tolist()
print(len(list_missing))

# Load the original list from the pickle file
with open(pickle_path, 'rb') as f:
    original_list = pickle.load(f)

# Calculate the ratio
ratio = len(list_missing) / len(original_list)
print(f"Ratio of new list length to original list length: {ratio}")

path_out = 'Data_actual_out/tech/owners_list_missing.csv'

df_missing = pd.DataFrame(list_missing, columns=['owner'])
df_missing.to_csv(path_out, index=False)
