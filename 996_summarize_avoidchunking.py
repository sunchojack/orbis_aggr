import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import os
from datetime import datetime, timedelta
import logging

def convert_days_to_date(days_since):
    if pd.isnull(days_since):
        return None
    try:
        days_since = int(float(days_since))
        base_date = datetime(1899, 12, 30)
        return base_date + timedelta(days=days_since)
    except ValueError as e:
        logging.error(f"ValueError: {e} for days_since: {days_since}")
        return None

def process_and_save_df(df, output_dir):
    # Convert 'Application/filing date' to datetime using the custom function
    df['Application/filing date'] = df['Application/filing date'].map_partitions(lambda s: s.apply(convert_days_to_date))

    # Convert date columns to datetime and extract the year
    date_columns = ['Publication date', 'Application/filing date', 'Priority date', 'Grant date', 'Expiration date']
    for col in date_columns:
        df[col] = dd.to_datetime(df[col], errors='coerce').dt.year

    # Melt the dataframe to have a 'year_type' and 'year' column
    melted_df = dd.melt(
        df,
        id_vars=['owner', 'ownership_share'],
        value_vars=date_columns,
        var_name='year_type',
        value_name='year'
    )

    # Remove rows with missing year values
    melted_df = melted_df.dropna(subset=['year'])

    # Set the value column based on the ownership_share
    melted_df['value'] = melted_df['ownership_share']

    # Print column names and first few rows for debugging
    print("Column names before summarizing:", melted_df.columns)
    print("First few rows before summarizing:")
    print(melted_df.head())

    # Summarize the data by taking ownership shares into account
    summary = melted_df.groupby(['owner', 'year', 'year_type']).agg({'value': 'sum'}).reset_index()

    # Print column names and first few rows after groupby for debugging
    print("Column names after groupby:", summary.columns)
    print("First few rows after groupby:")
    print(summary.head())

    # Pivot the DataFrame to get the desired format
    summary = summary.pivot_table(index=['owner', 'year'], columns='year_type', values='value').reset_index().fillna(0)

    # Print column names and first few rows after pivot for debugging
    print("Column names after pivot:", summary.columns)
    print("First few rows after pivot:")
    print(summary.head())

    # Sort the summary dataframe based on 'owner' and then 'year'
    summary = summary.sort_values(by=['owner', 'year'])

    # Save the transformed DataFrame to a new CSV file
    output_file = os.path.join(output_dir, 'transformed_data.csv')
    summary.compute().to_csv(output_file, index=False)
    print(f'Saved {output_file} with size {os.path.getsize(output_file) / (1024 * 1024)} MB')

def main(input_file, output_dir):
    # Set up the Dask client
    client = Client(n_workers=16, threads_per_worker=2, memory_limit='8GB')
    print(client)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Specify data types for problematic columns
    dtypes = {
        'Publication number': 'object',
        'Publication date': 'object',
        'Application/filing date': 'object',
        'Priority date': 'object',
        'Grant date': 'object',
        'Expiration date': 'object',
        'Application number': 'object',
        'owner': 'object',
        'Inventor(s) country code(s)': 'object'
    }

    # Read the CSV using Dask
    df = dd.read_csv(input_file, dtype=dtypes)

    # Process and save the dataframe
    process_and_save_df(df, output_dir)

if __name__ == "__main__":
    # Example usage
    input_file = 'Data/owners_split_granted.csv'
    output_dir = 'Data/final_dask'
    main(input_file, output_dir)
