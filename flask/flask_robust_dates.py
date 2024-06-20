# import pandas as pd
# import datetime
# from datetime import timedelta
# import logging
#
# # Configure logging to capture errors in date conversion
# logging.basicConfig(level=logging.ERROR, filename='../date_conversion_errors.log', filemode='w')
#
# # Columns that need date conversion
# date_columns = [
#     "Publication date",
#     "Application/filing date",
#     "Priority date",
#     "Grant date",
#     "Expiration date"
# ]
#
# # Read the CSV file, ensuring that date columns are read as strings
# data = pd.read_csv('C:/data/1910s.csv', dtype={col: str for col in date_columns})
#
# # Function to convert numeric date strings or formatted date strings to datetime
# def convert_days_to_date(date_str):
#     if pd.isnull(date_str):
#         return None
#     try:
#         # Attempt to convert days since 1899-12-30
#         days_since = int(float(date_str))
#         base_date = datetime.datetime(1899, 12, 30)
#         return base_date + timedelta(days=days_since)
#     except ValueError:
#         try:
#             # Attempt to parse the date directly
#             return pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce')
#         except Exception as e:
#             logging.error(f"Error: {e} for date_str: {date_str}")
#             return None
#
# # Apply the conversion function to each date column
# for col in date_columns:
#     data[col] = data[col].apply(convert_days_to_date)
#
# # Save the cleaned data to a new CSV file
# data.to_csv('C:/data/orbis_out.csv', index=False)
