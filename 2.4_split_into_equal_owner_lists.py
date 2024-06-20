import pandas as pd
import os

# Define the paths
input_path = 'Data_actual_out/tech/owners_list_missing.csv'
output_dir = 'Data_actual_out/tech/owners_list_missing_chunks'

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Load the owners list CSV file
owners_list = pd.read_csv(input_path)

# Get the list of owners
list_missing = owners_list['owner'].tolist()
print(f"Total owners: {len(list_missing)}")

# Split the list into 16 equal chunks
chunk_size = len(list_missing) // 16
chunks = [list_missing[i:i + chunk_size] for i in range(0, len(list_missing), chunk_size)]

# Ensure exactly 16 chunks by possibly adding one more chunk if necessary
if len(chunks) > 16:
    chunks[-2].extend(chunks[-1])
    chunks = chunks[:-1]

# Save each chunk to a separate CSV file
for i, chunk in enumerate(chunks, start=1):
    chunk_df = pd.DataFrame(chunk, columns=['owner'])
    chunk_df.to_csv(f'{output_dir}/owners_{i}.csv', index=False)
    print(f'Saved chunk {i} with {len(chunk)} owners')

print(f'Total chunks saved: {len(chunks)}')
