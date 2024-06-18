import pandas as pd
import os

# Define the directory path
directory_path = r'C:\Users\Arsenev\Desktop\Orbis\orbis_STATA_stock_aggregation\Data_out'

# Get the list of all files in the directory and subdirectories
all_files = []
for root, dirs, files in os.walk(directory_path):
    for file in files:
        if file.endswith('.csv'):
            all_files.append(os.path.join(root, file))

# Read the first three CSV files and get their dimensions
file_dimensions = {}
for file in all_files[:3]:  # Limiting to first 3 CSV files found
    df = pd.read_csv(file)
    file_dimensions[file] = df.shape

print(file_dimensions)
