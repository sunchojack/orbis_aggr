import dask.dataframe as dd
import pandas as pd

# Define file paths
csv_file_path = 'Data/onlyAs.csv'

# Define the data types for the columns
dtype = {
    'Publication number': 'str',
    'Application/filing date': 'str',
    'Priority date': 'str',
    'Publication date': 'str',
    'Grant date': 'str',
    'Expiration date': 'str',
    'owner': 'str',
    'ownership_share': 'float64',
    'Inventor(s) country code(s)': 'str',
}

# Load the CSV file into a Pandas DataFrame (Using pandas for simplicity)
df_filtered = pd.read_csv(csv_file_path, dtype=dtype)

# Ensure 'owner' is of type string
df_filtered['owner'] = df_filtered['owner'].astype('str')

# Melting the DataFrame to have 'owner', 'year', and 'type_year'
df_melted = df_filtered.melt(
    id_vars=['owner', 'ownership_share'],
    value_vars=['Application/filing date', 'Priority date', 'Publication date', 'Grant date', 'Expiration date'],
    var_name='type_year',
    value_name='year'
)

# Ensure 'year' is numeric, coercing errors and dropping NaNs
df_melted['year'] = pd.to_numeric(df_melted['year'], errors='coerce').dropna()

# Convert 'year' to integer if needed
df_melted['year'] = df_melted['year'].astype('str')
df_melted['type_year'] = df_melted['type_year'].astype('str')

# Pivot the table to have 'year' and 'owner' as indices, 'type_year' as columns, and the sum of 'ownership_share' as values
df_wide = df_melted.pivot_table(index=['owner', 'year'], columns='type_year', values='ownership_share', aggfunc='sum', fill_value=0)

# Reset index to make sure 'owner' and 'year' are columns
df_wide.reset_index(inplace=True)

# Filter output by owner alphabetically, then by years ascending
df_wide = df_wide.sort_values(by=['owner', 'year'], ascending=[True, True])

# Reorder the columns
columns_order = ['owner', 'year', 'Application/filing date', 'Priority date', 'Publication date', 'Grant date', 'Expiration date']
df_wide = df_wide.reindex(columns=columns_order)

# Save the result to a CSV file
df_wide.to_csv('Data/final00.csv', index=False)

# Display the first few rows to confirm 'owner' and 'year' columns are present and correctly ordered
print(df_wide.head())

# Remove rows where all year values are 0
# columns_to_check = ['Application/filing date', 'Priority date', 'Publication date', 'Grant date', 'Expiration date']
# result_wide = result[(result[columns_to_check] != 0).any(axis=1)]
#
# # Reorder the columns
# result_wide = result_wide[['owner', 'year', 'Application/filing date', 'Priority date', 'Publication date', 'Grant date', 'Expiration date']]

# Save the result to a CSV file
# df_wide.to_csv('Data/final00.csv', index=False)