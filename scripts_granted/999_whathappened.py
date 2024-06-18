import dask.dataframe as dd

# Define the file path and the owner ID to filter
file_path = r'C:\Users\Arsenev\Desktop\Orbis\orbis_STATA_stock_aggregation\Data\POSTPROD_FIN_combined_INFO.csv'
owner_id = 'AE0000068842'
o2 = ',AE0000068842'
o3 = 'AE0000068842,'

dtypes = {
    'Publication number': 'str',
    'Publication date': 'str',
    'Application/filing date': 'str',
    'Priority date': 'str',
    'Grant date': 'str',
    'Expiration date': 'str',
    'Application number': 'str',
    'Current direct owner(s) BvD ID Number(s)': 'str',
    'Inventor(s) country code(s)': 'str'
}

usecols = list(dtypes.keys())[0:9]

# Read the CSV file using Dask with specific filtering
data = dd.read_csv(file_path, assume_missing=True, dtype=dtypes, usecols=usecols)

# Ensure the column names are correct
print(data.columns)

# Strip leading and trailing whitespaces from the 'Current direct owner(s) BvD ID Number(s)' column
data['Current direct owner(s) BvD ID Number(s)'] = data['Current direct owner(s) BvD ID Number(s)'].str.strip()

# Debugging: Check the unique values in the 'Current direct owner(s) BvD ID Number(s)' column
unique_values = data['Current direct owner(s) BvD ID Number(s)'].unique().compute()
print(f"Unique values in 'Current direct owner(s) BvD ID Number(s)': {unique_values}")

# Filter the dataframe to include only rows where the 'Current direct owner(s) BvD ID Number(s)' column contains the owner_id
filtered_data = data[data['Current direct owner(s) BvD ID Number(s)'].str.contains(owner_id | o2 | o3, na=False)]

# Check if there are any results after filtering
filtered_count = filtered_data.shape[0].compute()
print(f"Number of rows after filtering: {filtered_count}")

if filtered_count > 0:
    # Persist the filtered data to a new CSV file
    filtered_data.to_csv('Data/AE0000068842.csv', single_file=True, index=False)
else:
    print("No matching rows found for the given owner ID.")
