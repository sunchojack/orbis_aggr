import dask.dataframe as dd
import os
import json
from dask.distributed import Client, LocalCluster


def analyze_errors(error_rows_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Set up Dask cluster to use multiple cores, adjusted for CPU constraints
    cluster = LocalCluster(n_workers=8, threads_per_worker=2, memory_limit='12GB')
    client = Client(cluster)
    print(client)

    # Check if there are any Parquet files in the error_rows_dir
    error_files = [os.path.join(error_rows_dir, file) for file in os.listdir(error_rows_dir) if
                   file.endswith('.parquet')]
    if not error_files:
        raise ValueError("No Parquet files found in the specified directory.")

    # Read the Parquet files containing the error rows
    error_df = dd.read_parquet(error_files)

    # Convert a few Parquet files to CSV for inspection
    sample_csv_dir = os.path.join(output_dir, 'sample_csvs')
    if not os.path.exists(sample_csv_dir):
        os.makedirs(sample_csv_dir)

    sample_files = error_files[:2]  # Select the first two files for sample conversion
    for i, file in enumerate(sample_files):
        sample_df = dd.read_parquet(file)
        sample_csv_path = os.path.join(sample_csv_dir, f'sample_{i}.csv')
        sample_df.compute().to_csv(sample_csv_path, index=False)
        print(f'Saved sample CSV to {sample_csv_path}')

    # Analyze the error rows
    analysis_results = {
        "total_rows": error_df.shape[0].compute(),
        "missing_values": error_df.isna().sum().compute().to_dict(),
        "invalid_dates": {},
    }

    # Check for invalid dates in each date column
    date_columns = [
        'Publication date',
        'Application/filing date',
        'Priority date',
        'Grant date',
        'Expiration date'
    ]
    for col in date_columns:
        invalid_dates_count = error_df[~error_df[col].str.match(r'\d{4}-\d{2}-\d{2}', na=True)].shape[0].compute()
        analysis_results["invalid_dates"][col] = invalid_dates_count

    # Save the analysis results to a JSON file
    analysis_file = os.path.join(output_dir, 'analysis_summary.json')
    with open(analysis_file, 'w') as f:
        json.dump(analysis_results, f, indent=4)
    print(f'Saved analysis summary to {analysis_file}')

    # Display the first few rows of the error DataFrame
    sample_data = error_df.head(10)
    print("Sample data from the errors:")
    print(sample_data)


# Example usage
error_rows_dir = 'Data/full_error_rows/full_error_rows'
output_dir = 'Data/full_error_rows/error_analysis'


def main():
    analyze_errors(error_rows_dir, output_dir)


if __name__ == '__main__':
    main()
