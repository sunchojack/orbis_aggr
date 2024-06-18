import pandas as pd

csv = pd.read_csv('Data/final00.csv')
dta = pd.read_stata('Data/owner_year_granted_v2.dta')

csv.columns = ['owner', 'year', 'applied', 'priority', 'published', 'granted', 'expired']

# Convert appropriate columns to numeric, errors='coerce' will convert non-numeric values to NaN
csv[['applied', 'priority', 'published', 'granted', 'expired']] = csv[['applied', 'priority', 'published', 'granted', 'expired']].apply(pd.to_numeric, errors='coerce')
dta[['applied', 'priority', 'published', 'granted', 'expired']] = dta[['applied', 'priority', 'published', 'granted', 'expired']].apply(pd.to_numeric, errors='coerce')

# Merge the DataFrames on 'owner' and 'year'
merged_df = pd.merge(csv, dta, on=['owner', 'year'], suffixes=('_csv', '_dta'))
merged_df = merged_df[merged_df['year'].astype('int') < 2025]

# Perform element-wise subtraction for numeric columns
for col in ['applied', 'priority', 'published', 'granted', 'expired']:
    merged_df[f'diff_{col}'] = merged_df[f'{col}_csv'] - merged_df[f'{col}_dta']

columns_to_round = ['applied_csv', 'priority_csv', 'published_csv', 'granted_csv', 'expired_csv',
                    'applied_dta', 'priority_dta', 'published_dta', 'granted_dta', 'expired_dta',
                    'diff_applied', 'diff_priority', 'diff_published', 'diff_granted', 'diff_expired']

merged_df[columns_to_round] = merged_df[columns_to_round].round(2)

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv('Data/subtracted_csv_TOTALFILE.csv', index=False)