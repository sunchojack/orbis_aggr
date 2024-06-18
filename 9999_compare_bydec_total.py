import pandas as pd
import os

# Define the directory path
directory_path = r'C:\Users\Arsenev\Desktop\Orbis\orbis_STATA_stock_aggregation\Data_out'

bydec = pd.read_csv(os.path.join(directory_path, 'SERVER_GRANTED_BY_DECADE.csv'))
totalfile = pd.read_csv(os.path.join(directory_path, 'SERVER_GRANTED_TOTALFILE.csv'))

bydec['year'] = bydec['year'].astype(float)
bydec['year'] = bydec['year'].astype(int) + 1900

merged_df = pd.merge(bydec, totalfile, on=['owner', 'year'], suffixes=('_bydec', '_total'))

dates = ['Publication date', 'Priority date',
                                  'Grant date',
                                  'Expiration date',
                                  'Application/filing date']

for col in dates:
    merged_df[f'diff_{col}'] = merged_df[f'{col}_bydec'] - merged_df[f'{col}_total']

merged_df.to_csv('Data_out/bydec_vs_totalfile.csv', index=False)
