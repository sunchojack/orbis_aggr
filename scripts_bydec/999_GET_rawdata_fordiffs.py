import pandas as pd

# Load the 'mine' data
mine = pd.read_csv('Data/subtracted_csv_BYDECADE.csv')

# Ensure 'year' column is treated as string
mine['year'] = mine['year'].astype(str)

# Filter the 'mine' data for rows where 'year' contains '198*'
mine = mine[mine['year'].str.contains("198")]

# Load the 'patents' data
patents = pd.read_csv('Data/bydec/1980s/1980s.csv', dtype=str)

# Rename the 'Current direct owner(s) BvD ID Number(s)' column to 'owner'
patents.rename(columns={'Current direct owner(s) BvD ID Number(s)': 'owner'}, inplace=True)

# Get the list of owners from the 'mine' data (assuming 'owner' column exists)
owners_list = mine['owner'].astype('str').dropna().unique().tolist()

# Ensure 'owner' column is treated as a list, handle NaNs
patents['owner'] = patents['owner'].apply(lambda x: x.split(',') if isinstance(x, str) else [])

# Filter the 'patents' data to keep rows where one or more owners match those found in the 'mine' data
patents['matches'] = patents['owner'].apply(lambda x: any(owner in owners_list for owner in x) if x else False)
filtered_patents = patents[patents['matches']].drop(columns=['matches'])

# Explode the 'owner' column to have one owner per row
exploded_patents = filtered_patents.explode('owner')

# Sort by 'owner' and 'Application/filing date'
sorted_patents = exploded_patents.sort_values(by=['owner', 'Application/filing date'])

# Save the resulting DataFrame to a CSV file
sorted_patents.to_csv('Data/matching_raw_patents1980s.csv', index=False)

# Display size of the final DataFrame
print("Size of sorted_patents data:", sorted_patents.shape)
