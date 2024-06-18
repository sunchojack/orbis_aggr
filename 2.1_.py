# get the owner file from steps 1-4
# join owners with countries and nace

import pandas as pd

owners = pd.read_csv('Data/final/nongranted_transformed.csv')
country_industry = pd.read_stata('Data/_dtas/industry_owners_missingDL_aded.dta')
country_industry.rename(columns={'bvdid': 'owner'}, inplace=True)

output = owners.merge(country_industry, on='owner', how='left')

output.to_csv('Data/final/owners_country_industry__nongranted.csv', index=False)
