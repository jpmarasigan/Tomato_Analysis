import pandas as pd
import json

# Load the JSON data
with open('../private/mock_sample_data.json', 'r') as file:
    data = json.load(file)

# Convert JSON data to DataFrame
df = pd.DataFrame(data)

# Convert date column to datetime
df['date'] = pd.to_datetime(df['date'])

# Sort DataFrame by date
df = df.sort_values('date')

# Define a function to calculate moving window statistics
def moving_window_analysis(df, window_size, column, interval):
    df[f'{column}_{interval}_mean'] = df[column].rolling(window=window_size).mean()
    df[f'{column}_{interval}_std'] = df[column].rolling(window=window_size).std()
    df[f'{column}_{interval}_rate_change'] = df[column].pct_change(periods=window_size)
    return df

# Define a function to check temperature condition
def check_temperature_condition(rolling_mean):
    if rolling_mean < 18 or rolling_mean > 25:
        return 'Not Healthy'
    else:
        return 'Healthy'

# Apply moving window analysis for a 7-day window on temperature
df = moving_window_analysis(df, 1, 'temperature', 'daily')
df = moving_window_analysis(df, 7, 'temperature', 'weekly')
df = moving_window_analysis(df, 30, 'temperature', 'monthly')

# Mark every nth day per week and month (starts from 0)
df['is_week_end'] = (df.index % 7 == 6)   
df['is_month_end'] = (df.index % 30 == 29) 

# Sliding Window Interval of daily, weekly, monthly
df['temperature_condition'] = df.apply(
    lambda row: {
        'daily': check_temperature_condition(row['temperature_daily_mean']) if not pd.isna(row['temperature_daily_mean']) else None,
        'weekly': check_temperature_condition(row['temperature_weekly_mean']) if not pd.isna(row['temperature_weekly_mean']) else None,
        'monthly': check_temperature_condition(row['temperature_monthly_mean']) if not pd.isna(row['temperature_monthly_mean']) else None
    },
    axis=1
)

# Fixed Window Interval of 7 days
# df['temperature_condition'] = df.apply(
#     lambda row: check_temperature_condition(row['temperature_weekly_mean']) if row['is_week_end'] else None,
#     axis=1
# )

selected_columns = df[['temperature_condition', 'temperature_daily_mean', 'temperature_weekly_mean', 'temperature_monthly_mean']]
with open('fixed_stats_log.txt', 'w') as f:
    f.write(selected_columns.to_string())


print(df[['temperature_condition', 'temperature_weekly_mean', 'temperature_monthly_mean']])
