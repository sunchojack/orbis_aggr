import dask.dataframe as dd
from dask.distributed import Client
import os

def summarize_chunks_with_dask(input_dir, output_file):
    # Set up the Dask client
    client = Client(n_workers=16, threads_per_worker=2, memory_limit='8GB')
    print(client)

    # List all the chunk files in the directory
    chunk_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('_transformed.csv')]

    # Read all chunk files into a Dask DataFrame
    ddf_list = [dd.read_csv(file) for file in chunk_files]

    # Concatenate all Dask DataFrames
    concatenated_ddf = dd.concat(ddf_list, axis=0)

    # Perform the final summarization
    summarized_ddf = concatenated_ddf.groupby(['owner', 'year']).sum().reset_index()

    # Compute and save the summarized DataFrame to a new CSV file
    summarized_ddf.compute().to_csv(output_file, index=False)
    print(f'Summarized data saved to {output_file}')

def main():
    input_dir = 'Data/final_chunked'
    output_file = 'Data/final_summary.csv'
    summarize_chunks_with_dask(input_dir, output_file)

if __name__ == "__main__":
    main()
