import dask.dataframe as dd
import matplotlib.pyplot as plt

# Define the file path to your dataset
file_path = r'Data/subtracted_csv_totalfile.csv'

data = dd.read_csv(file_path)

# Identify columns that start with 'diff_'
diff_columns = [col for col in data.columns if col.startswith('diff_')]

# Apply a filter to keep rows where any 'diff_' column has a non-zero value
filtered_data = data[data[diff_columns].any(axis=1)]

# Calculate sums of the difference columns
sums = filtered_data[diff_columns].sum().compute()

# Plot the distribution of the differences
# Convert filtered data to pandas DataFrame for plotting
filtered_data_pd = filtered_data[diff_columns].compute()

# Create histograms for each diff_ column with x-axis limited to -100 to 100
for col in diff_columns:
    plt.figure()
    filtered_data_pd[col].hist(bins=50, alpha=0.7, range=(-33, 33))
    plt.title(f'Distribution of {col}')
    plt.xlabel(col)
    plt.ylabel('Frequency')
    plt.grid(False)
    plt.xlim(-10, 10)  # Limit x-axis to -100 to 100
    plt.show()

# Display the sums of the differences
print(sums)