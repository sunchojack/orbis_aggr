import dask.dataframe as dd

# Define the file path to your dataset
file_path = r'Data/subtracted_csv_totalfile.csv'

# Read the dataset with Dask
data = dd.read_csv(file_path)

# Identify columns that start with 'diff_'
diff_columns = [col for col in data.columns if col.startswith('diff_')]

# Apply a filter to keep rows where any 'diff_' column has a non-zero value
filtered_data = data[data[diff_columns].any(axis=1)]

# Persist the filtered data to a new CSV file
filtered_data.to_csv('Data/only_diffs.csv', single_file=True, index=False)
