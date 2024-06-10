import dask.dataframe as dd


def process_full_formatted_file(input_file, output_file):
    # Load the data from the CSV file, specifying columns to parse as dates
    list_usecols = ['Publication number',
                    'Application number',
                    'owner',
                    'Inventor(s) country code(s)',
                    'Publication date',
                    'Application/filing date',
                    'Priority date',
                    'Grant date',
                    'Expiration date']

    df = dd.read_csv(input_file,
                     usecols=list_usecols,
                     dtype={
                         'Publication number': 'str',
                         'Application number': 'str',
                         'owner': 'str',
                         'Inventor(s) country code(s)': 'str'
                     },
                     parse_dates=['Publication date',
                                  'Priority date',
                                  'Grant date',
                                  'Expiration date',
                                  'Application/filing date'])

    # # Filter out rows where 'Grant date' is NaN or empty
    # df = df[df['Grant date'].notnull() & (df['Grant date'] != '')]

    # Count the number of owners (comma-separated)
    df['Number of owners'] = df['owner'].str.count(',') + 1

    # Split the owners and add 'ownership_share' column as 1/n
    df = df.assign(owner=df['owner'].str.split(',')).explode('owner')
    df['ownership_share'] = 1 / df['Number of owners']

    # Compute and save the intermediate result to a new CSV file
    df.compute().to_csv(output_file, index=False)


def main():
    # Define file paths
    input_file = 'Data/intermediate_corrected_nonNAowners_nongranted.csv'
    output_file = 'Data/owners_split_nongranted.csv'

    # Process the file
    process_full_formatted_file(input_file, output_file)


if __name__ == "__main__":
    main()
