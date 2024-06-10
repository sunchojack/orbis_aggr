import pandas as pd
import os

def split_csv(input_file, output_dir, chunk_size=1 * 1024 * 1024 * 1024):  # 1GB chunks
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Initialize variables
    file_number = 0
    chunk_rows = []
    chunk_size_bytes = 0

    # Read the CSV in chunks
    for chunk in pd.read_csv(input_file, chunksize=10000):
        chunk_size_bytes += chunk.memory_usage(deep=True).sum()
        chunk_rows.append(chunk)

        # Check if the accumulated chunk size has reached the limit
        if chunk_size_bytes >= chunk_size:
            # Concatenate all rows and write to a new file
            chunk_df = pd.concat(chunk_rows)
            process_chunk(chunk_df, file_number, output_dir)

            # Reset variables for the next chunk
            file_number += 1
            chunk_rows = []
            chunk_size_bytes = 0

    # Write remaining rows if any
    if chunk_rows:
        chunk_df = pd.concat(chunk_rows)
        process_chunk(chunk_df, file_number, output_dir)


def process_chunk(chunk_df, file_number, output_dir):
    # Convert date columns to datetime and extract the year
    date_columns = ['Publication date',
                    # 'Application/filing date',
                    'Priority date', 'Grant date', 'Expiration date']
    for col in date_columns:
        chunk_df[col] = pd.to_datetime(chunk_df[col], errors='coerce').dt.year

    # Melt the dataframe to have a 'year_type' and 'year' column
    melted_df = chunk_df.melt(
        id_vars=['owner', 'ownership_share'],
        value_vars=date_columns,
        var_name='year_type',
        value_name='year'
    )

    # Remove rows with missing year values
    melted_df.dropna(subset=['year'], inplace=True)

    # Set the value column based on the ownership_share
    melted_df['value'] = melted_df['ownership_share']

    # Summarize the data by taking ownership shares into account
    summary = melted_df.groupby(['owner', 'year', 'year_type']).agg({'value': 'sum'}).unstack(fill_value=0).reset_index()
    summary.columns = ['owner', 'year'] + [f'{col[1]}_{col[0]}' for col in summary.columns if col[0] == 'value']

    # Sort the summary dataframe based on 'owner' and then 'year'
    summary = summary.sort_values(by=['owner', 'year'])

    # Save the transformed DataFrame to a new CSV file
    chunk_file = os.path.join(output_dir, f'chunk_{file_number}_transformed.csv')
    summary.to_csv(chunk_file, index=False)
    print(f'Saved {chunk_file} with size {os.path.getsize(chunk_file) / (1024 * 1024)} MB')


# Example usage
input_file = 'Data/owners_split_granted.csv'
output_dir = 'Data/final_chunked'
split_csv(input_file, output_dir)
