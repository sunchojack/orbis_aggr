import pandas as pd

# Read the data
csv = pd.read_csv(r'Data_out\SERVER_GRANTED_ACTUAL_TOTALFILE.csv')
dta = pd.read_stata(r'C:\Users\Arsenev\Desktop\Orbis\orbis_STATA_stock_aggregation\Data\_dtas\fixed\owner_year_long_stock_granted_v2\owner_year_long_stock_granted_v2.dta')

# Rename columns in csv
csv.rename(columns={
    "Application/filing date": "applied",
    "Priority date": "priority",
    "Publication date": 'published',
    "Grant date": 'granted',
    'Expiration date': 'expired'
}, inplace=True)

# Convert appropriate columns in csv to numeric, errors='coerce' will convert non-numeric values to NaN
csv[['applied', 'priority', 'published', 'granted', 'expired']] = csv[['applied', 'priority', 'published', 'granted', 'expired']].apply(pd.to_numeric, errors='coerce')

# Convert appropriate columns in dta to numeric
dta[['applied', 'priority', 'published', 'granted', 'expired']] = dta[['applied', 'priority', 'published', 'granted', 'expired']].apply(pd.to_numeric, errors='coerce')

# Ensure the 'year' and 'owner' columns are of the correct type
# csv['year'] = csv['year'].astype(int) + 1900
# csv = csv[csv['year'] != 1900]
csv['owner'] = csv['owner'].astype(str)
csv['year'] = csv['year'].astype(int)

dta['year'] = dta['year'].astype(int)
dta['owner'] = dta['owner'].astype(str)

# Merge the DataFrames on 'owner' and 'year'
merged_df = pd.merge(csv, dta, on=['owner', 'year'], how='outer', suffixes=('_csv', '_dta'))

# Perform element-wise subtraction for numeric columns
for col in ['applied', 'priority', 'published', 'granted', 'expired']:
    merged_df[f'diff_{col}'] = merged_df[f'{col}_csv'] - merged_df[f'{col}_dta']

# Keep only the diff columns that are non-zero, plus the owner and year columns
non_zero_diff_columns = ['owner', 'year'] + [col for col in merged_df.columns if col.startswith('diff_') and (merged_df[col] != 0).any()]

# Filter the merged DataFrame to keep only the non-zero diff columns plus owner and year
filtered_df = merged_df[non_zero_diff_columns]

# Save the resulting DataFrame to a new CSV file
filtered_df.to_csv(r'C:\Users\Arsenev\Desktop\Orbis\orbis_STATA_stock_aggregation\Data\_dtas\fixed\fixed_subtracted_csv_ACTUAL_TOTALFILE.csv', index=False)

# Display the resulting DataFrame for verification
print(filtered_df.head())
