import pandas as pd

# data = pd.read_csv('Data/final/owners_country_industry__nongranted.csv')
data = pd.read_csv('Data/final/owners_country_industry.csv')

data = data.drop(columns=['owner'])

# Group by the 'country' and 'industry' columns and sum the rest
aggregated_data = data.groupby(['ctryiso', 'nace2d', 'year']).sum().reset_index()

# Display the result
print(aggregated_data)

# Save to a CSV file
aggregated_data.to_csv('Data/final/couind_aggregated_nongranted.csv', index=False)