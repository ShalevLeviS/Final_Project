import pandas as pd

# Assuming `df` is your DataFrame loaded from the CSV file.
file_path = 'C:\\Users\\Shalev\\PycharmProjects\\Fintech\\DataTable.csv'  # Replace with your actual file path
df = pd.read_csv(file_path)

day_mapping = {
    'Sunday': 1,
    'Monday': 2,
    'Tuesday': 3,
    'Wednesday': 4,
    'Thursday': 5,
    'Friday': 6,
    'Saturday': 7
}

# Step 1: Drop unnecessary columns
df = df.drop(columns=['Transaction_ID', 'Name'])

# Step 2: Convert Timestamp to datetime format
df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ns')

# Step 3: Extract features from the last row's Timestamp before dropping it
df['Day'] = df['Timestamp'].dt.day
df['Month'] = df['Timestamp'].dt.month
df['Year'] = df['Timestamp'].dt.year
df['Hour'] = df['Timestamp'].dt.hour
df['Minute'] = df['Timestamp'].dt.minute
df['Second'] = df['Timestamp'].dt.second
df['Day_Of_Week'] = df['Timestamp'].dt.day_name()
df['Day_Of_Week'] = df['Day_Of_Week'].map(day_mapping)

# Step 4: Define a function for 15-minute aggregation
def aggregate_15min(group):
    if group.empty:
        return pd.Series({
            'Total_Money': 0,
            'Total_Volume': 0,
            'Avg_Stock_Price': 0,
            'Avg_Order_Executed': 0,
            'Target_0': 0,
            'Target_35': 0,
            'Target_65': 0,
            'Target_90': 0,
            'Day': None,
            'Month': None,
            'Year': None,
            'Hour': None,
            'Minute': None,
            'Second': None,
            'Day_Of_Week': None
        })

    # Total money = Sum of Price * Volume
    total_money = (group['Price'] * group['Volume']).sum() / 100

    # Total volume = Sum of Volume
    total_volume = group['Volume'].sum()

    # Avg stock price = Total money / Total volume
    avg_stock_price = total_money / total_volume / 100 if total_volume != 0 else 0

    # Order_Executed = Mean of Order_Executed
    avg_order_executed = group['Order_Executed'].mean()

    # Split and count Target values into 4 categories
    target_counts = group['Target'].value_counts().reindex([0, 35, 65, 90], fill_value=0)

    return pd.Series({
        'Total_Money': total_money,
        'Total_Volume': total_volume,
        'Avg_Stock_Price': avg_stock_price,
        'Avg_Order_Executed': avg_order_executed,
        'Target_0': target_counts[0],
        'Target_35': target_counts[35],
        'Target_65': target_counts[65],
        'Target_90': target_counts[90],
        'Day': group['Day'].iloc[-1],
        'Month': group['Month'].iloc[-1],
        'Year': group['Year'].iloc[-1],
        'Hour': group['Hour'].iloc[-1],
        'Minute': group['Minute'].iloc[-1],
        'Second': group['Second'].iloc[-1],
        'Day_Of_Week': group['Day_Of_Week'].iloc[-1]
    })

# Step 5: Aggregate rows into 15-minute intervals
df_aggregated = (
    df.groupby(pd.Grouper(key='Timestamp', freq='15min'))  # Updated frequency to avoid warning
    .apply(aggregate_15min)
    .reset_index()
)

# Step 6: Drop the Timestamp column (if required)
df_aggregated = df_aggregated.drop(columns=['Timestamp'], errors='ignore')
df_aggregated = df_aggregated.dropna()
output_file_path = 'C:\\Users\\Shalev\\PycharmProjects\\Fintech\\processed_timestamp_data_Prediction-new.csv'
df_aggregated.to_csv(output_file_path, index=False)

print(f"Processed data saved to: {output_file_path}")
