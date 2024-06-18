import pandas as pd

def process_patent_data(input_file, output_file):
    try:
        # Define data types
        dtype = {
            'Publication number': 'str',
            'Publication date': 'str',
            'Application/filing date': 'str',
            'Priority date': 'str',
            'Grant date': 'str',
            'Expiration date': 'str',
            'owner': 'str',
            'ownership_share': 'float64',
            'Inventor(s) country code(s)': 'str',
        }

        # Read the data
        df = pd.read_csv(input_file, dtype=dtype)

        # Extract the year from date columns
        date_columns = ['Publication date', 'Application/filing date', 'Priority date', 'Grant date', 'Expiration date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.year

        # Write to CSV
        df.to_csv(output_file, index=False)

        print(f"Data successfully processed and saved to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
input_file = 'Data/owners_split_granted.csv'
output_file = 'Data/granted_only_years.csv'

def main():
    process_patent_data(input_file, output_file)

if __name__ == '__main__':
    main()
