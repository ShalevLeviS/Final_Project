import pandas as pd

# Load the dataframe
file_path = 'C:\\Users\\Shalev\\PycharmProjects\\Fintech\\DataTable.csv'  # Replace with your actual file path
df = pd.read_csv(file_path)
# print(len(df))
day_mapping = {
    'Sunday': 1,
    'Monday': 2,
    'Tuesday': 3,
    'Wednesday': 4,
    'Thursday': 5,
    'Friday': 6,
    'Saturday': 7
}

# Convert the timestamp from nanoseconds to datetime
df['datetime'] = pd.to_datetime(df['Timestamp'], unit='ns')

# Extracting components
df['day'] = df['datetime'].dt.day
df['month'] = df['datetime'].dt.month
df['year'] = df['datetime'].dt.year
df['hour'] = df['datetime'].dt.hour
df['minute'] = df['datetime'].dt.minute
df['second'] = df['datetime'].dt.second
df['day_of_week'] = df['datetime'].dt.day_name()
df['day_of_week'] = df['day_of_week'].map(day_mapping)


df = df.drop(columns=['Timestamp', 'datetime', 'Transaction_ID'])

# Save the new DataFrame to a CSV file
output_file_path = 'processed_timestamp_data_Classification-new.csv'
df.to_csv(output_file_path, index=False)

# output_file_path