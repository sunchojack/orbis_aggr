import dask.dataframe as dd
import os
from dask.distributed import Client, LocalCluster

def process_errors(initial_file, errors_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Set up Dask cluster to use multiple cores, adjusted for CPU constraints
    cluster = LocalCluster(n_workers=8, threads_per_worker=2, memory_limit='12GB')
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

    # Read the initial CSV file using Dask with a specified block size to manage memory usage
    initial_df = dd.read_csv(initial_file, dtype=dtype, blocksize="64MB")

    # Read the error files
    error_files = [os.path.join(errors_dir, file) for file in os.listdir(errors_dir) if file.startswith('step5_errors')]
    error_dfs = [dd.read_csv(file, usecols=['owner']) for file in error_files]
    errors_df = dd.concat(error_dfs, axis=0).drop_duplicates()

    # Filter the initial file to get the rows corresponding to the owners in the errors file
    filtered_df = initial_df.merge(errors_df, on='owner', how='inner')

    # Save the filtered DataFrame to Parquet files in chunks of 20MB
    output_file_prefix = os.path.join(output_dir, 'full_error_rows')
    filtered_df.to_parquet(output_file_prefix, engine='pyarrow', compression='snappy', write_index=False)

    print(f'Saved error rows to {output_file_prefix} as Parquet files')

# Example usage
initial_file = 'Data/owners_split_nongranted.csv'
errors_dir = 'Data/final/errors'
output_dir = 'Data/full_error_rows'

def main():
    process_errors(initial_file, errors_dir, output_dir)

if __name__ == '__main__':
    main()
