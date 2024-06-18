import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client


def initialize_dask_client(memory_limit_gb):
    memory_limit = f"{memory_limit_gb}GB"
    client = Client(memory_limit=memory_limit)
    print(client)
    return client


def process_full_formatted_file(input_file, output_file, memory_limit_gb):
    # Initialize Dask client
    client = initialize_dask_client(memory_limit_gb)

    # Define the correct dtypes for the columns
    dtypes = {
        'Publication number': 'object',
        'Publication date': 'object',
        'Application/filing date': 'object',
        'Priority date': 'object',
        'Grant date': 'object',
        'Expiration date': 'object',
        'Application number': 'object',
        'Current direct owner(s) BvD ID Number(s)': 'object',
        'Inventor(s) country code(s)': 'object'
    }

    # Load the data from the CSV file using Dask
    ddf = dd.read_csv(input_file, dtype=dtypes, assume_missing=True)

    # Filter out rows where 'Current direct owner(s) BvD ID Number(s)' is NaN or empty
    ddf = ddf[ddf['Current direct owner(s) BvD ID Number(s)'].notnull() & (
                ddf['Current direct owner(s) BvD ID Number(s)'] != '')]

    # Filter out rows where 'Current direct owner(s) BvD ID Number(s)' is 'NaN'
    long_format_corrected = ddf[ddf['Current direct owner(s) BvD ID Number(s)'] != 'NaN']

    # Rename the 'Current direct owner(s) BvD ID Number(s)' column to 'owner' for simplicity
    long_format_corrected = long_format_corrected.rename(columns={'Current direct owner(s) BvD ID Number(s)': 'owner'})

    # Check the intermediate result before saving
    print("Number of rows in intermediate dataframe:", long_format_corrected.shape[0].compute())

    # Save the intermediate result to a new CSV file
    long_format_corrected.to_csv(output_file, single_file=True, index=False)


def main():
    # Define file paths
    input_file = 'Data/FILTERED_formatted_granted.csv'
    output_file = 'Data/intermediate_corrected_nonNAowners_granted.csv'

    # Set memory limit for Dask client (85% of 128GB RAM)
    memory_limit_gb = 128 * 0.85

    # Process the file
    process_full_formatted_file(input_file, output_file, memory_limit_gb)


if __name__ == "__main__":
    main()
