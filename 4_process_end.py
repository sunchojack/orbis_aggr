import dask.dataframe as dd
import pandas as pd
import os
from dask.distributed import Client, LocalCluster

def transform_and_save_csv(input_file, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Set up Dask cluster to use multiple cores, adjusted for CPU constraints
    cluster = LocalCluster(n_workers=16, threads_per_worker=1, memory_limit='24GB')
    client = Client(cluster)
    print(client)

    # Specify data types to avoid mismatched dtypes errors
    dtype = {
        'Publication date': 'object',
        'Application/filing date': 'object',
        'Priority date': 'object',
        'Grant date': 'object',
        'Expiration date': 'object',
        'owner': 'str',
        'year': 'str',
        'Inventor(s) country code(s)': 'str',
        'Publication number': 'str',
        'Application number': 'str'
    }

    # Read the entire CSV file using Dask with a specified block size to manage memory usage
    df = dd.read_csv(input_file, dtype=dtype, blocksize="64MB")

    # Convert date columns to datetime and extract the year
    date_columns = [
        'Publication date',
        'Application/filing date',
        'Priority date',
        'Grant date',
        'Expiration date'
    ]

    # Create a list to collect errors
    errors_list = []

    for col in date_columns:
        df[col] = dd.to_datetime(df[col], errors='coerce')
        errors_list.append(df[df[col].isna()][['owner', col]])

    # Concatenate all error DataFrames
    errors_df = dd.concat(errors_list, axis=0).compute()

    # Extract year from datetime columns
    for col in date_columns:
        df[col] = df[col].map_partitions(lambda x: x.dt.year, meta=('x', 'f8'))

    # Save errors to a separate CSV
    errors_file = os.path.join(output_dir, 'step5_errors.csv')
    errors_df.to_csv(errors_file, index=False)

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

    # Summarize the data by taking ownership shares into account
    summary = melted_df.groupby(['owner', 'year', 'year_type']).agg({'value': 'sum'}).reset_index()

    # Convert summary to pandas DataFrame for pivoting
    summary_pd = summary.compute()

    # Pivot the table
    summary_pivoted = summary_pd.pivot_table(index=['owner', 'year'], columns='year_type', values='value', fill_value=0).reset_index()

    # Sort the summary dataframe based on 'owner' and then 'year'
    summary_pivoted = summary_pivoted.sort_values(by=['owner', 'year'])

    # Save the transformed DataFrame to a new CSV file
    final_file = os.path.join(output_dir, 'nongranted_transformed.csv')
    summary_pivoted.to_csv(final_file, index=False)
    print(f'Saved {final_file} with size {os.path.getsize(final_file) / (1024 * 1024)} MB')

# Example usage
input_file = 'Data/owners_split_nongranted.csv'
output_dir = 'Data/final'

def main():
    transform_and_save_csv(input_file, output_dir)

if __name__ == '__main__':
    main()
