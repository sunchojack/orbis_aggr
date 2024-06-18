import dask.dataframe as dd
import pandas as pd
import datetime
from datetime import timedelta
import logging
import os
from dask.distributed import Client, LocalCluster

def main():
    # Set up a Dask cluster for parallel processing
    cluster = LocalCluster()
    client = Client(cluster)

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

    # Base directory containing the folders with CSV files
    base_dir = r"C:\Users\Arsenev\Desktop\Orbis\orbis_STATA_stock_aggregation\scripts_bydec\Data\bydec"

    # Function to convert date strings to datetime for 'yyyy-mm-dd' format
    def convert_days_to_date(date_str):
        if pd.isnull(date_str):
            return None
        try:
            # Attempt to convert days since 1899-12-30
            days_since = int(float(date_str))
            base_date = datetime.datetime(1899, 12, 30)
            return base_date + timedelta(days=days_since)
        except ValueError:
            try:
                # Attempt to parse the date directly
                return pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce')
            except Exception as e:
                logging.error(f"Error: {e} for date_str: {date_str}")
                return None

    # Initialize an empty list to hold DataFrames
    dataframes = []

    # Iterate over each subdirectory in the base directory
    for subdir, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(subdir, file)
                # Read the CSV file with Dask
                ddf = dd.read_csv(file_path, dtype=dtypes, usecols=usecols)
                ddf = ddf[~(ddf['Grant date'].isnull() | (ddf['Grant date'] == ''))]

                # Apply the date conversion function using Dask's map_partitions for parallel processing
                for col in date_col_names:
                    if col in ddf.columns:
                        ddf[col] = ddf[col].map_partitions(lambda s: s.apply(convert_days_to_date), meta=pd.Series([], dtype='datetime64[ns]'))

                # Append the filtered DataFrame to the list
                dataframes.append(ddf)

    # Concatenate all DataFrames
    combined_ddf = dd.concat(dataframes)

    # Filter out rows where 'Grant date' is 0
    combined_ddf = combined_ddf[combined_ddf['Grant date'] != 0]

    # Compute the combined DataFrame
    df_combined = combined_ddf.compute()

    # Set Pandas display options
    pd.set_option('display.max_columns', None)

    # Print data types of columns
    print(df_combined.dtypes)

    # Print a sample of the combined DataFrame
    print(df_combined.sample(n=10))  # Adjust 'n' to the desired number of random entries

    # Save the filtered DataFrame to a new CSV file
    output_path = 'Data/bydec_onlyAs_FILTERED_formatted_granted.csv'
    df_combined.to_csv(output_path, index=False)

    # Print data types of columns again
    print(df_combined.dtypes)

    # Print another sample of the combined DataFrame
    print(df_combined.sample(n=10))  # Adjust 'n' to the desired number of random entries

if __name__ == "__main__":
    main()
