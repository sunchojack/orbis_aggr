import dask.dataframe as dd
import pandas as pd
import datetime
from datetime import timedelta
import logging

# Specify the columns that need to be parsed as dates by their names
date_col_names = [
    'Publication date',
    'Application/filing date',
    'Grant date',
    'Priority date',
    'Expiration date'
]

# Specify the dtypes for the columns to avoid mismatched dtype issues
dtypes = {
    'Application number': 'object',
    'Current direct owner(s) BvD ID Number(s)': 'object',
    'Publication number': 'object',
    'Publication date': 'object',
    'Application/filing date': 'object',
    'Priority date': 'object',
    'Grant date': 'object',
    'Expiration date': 'object',
    'Inventor(s) country code(s)': 'object',
}

# Define the columns to read (first 9 columns)
usecols = list(dtypes.keys())[0:9]

# Read the CSV file with Dask
ddf = dd.read_csv("Data/POSTPROD_FIN_combined_INFO.csv", dtype=dtypes, usecols=usecols)

# Convert Dask DataFrame to Pandas DataFrame to use head and sample
df_sample = ddf.compute()

# Function to convert date strings to datetime for 'yyyy-mm-dd' format
def convert_days_to_date(days_since):
    if pd.isnull(days_since):
        return None
    try:
        days_since = int(float(days_since))
        base_date = datetime.datetime(1899, 12, 30)
        return base_date + timedelta(days=days_since)
    except ValueError as e:
        logging.error(f"ValueError: {e} for days_since: {days_since}")
        return None

# Convert 'Publication date', 'Application/filing date', 'Priority date', 'Grant date', 'Expiration date' to date format
date_cols = ['Publication date', 'Application/filing date', 'Priority date', 'Grant date', 'Expiration date']
for col in date_cols:
    try:
        if col in df_sample.columns:
            df_sample[col] = df_sample[col].apply(convert_days_to_date)
        else:
            logging.info(f"Column '{col}' not found in the DataFrame. Skipping...")
    except Exception as e:
        logging.error(f"Error processing column '{col}': {e}")

print(df_sample.dtypes)

pd.set_option('display.max_columns', None)

# Print all columns but only some random entries
print(df_sample.sample(n=10))  # Adjust 'n' to the desired number of random entries

# Filter rows where 'Grant date' is empty, null, NaN, or NA
filtered_df = df_sample[df_sample['Grant date'].isnull() | (df_sample['Grant date'] == '')]

# Save the filtered DataFrame to a new CSV file
filtered_df.to_csv('Data/FILTERED_formatted_nongranted.csv', index=False)

print(filtered_df.dtypes)
print(filtered_df.sample(n=10))  # Adjust 'n' to the desired number of random entries
