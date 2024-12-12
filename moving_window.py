import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import datetime
import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl

cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
firebase_admin.initialize_app(cred)

db = firestore.client()          # Initialize Firestore client
is_append = False
last_fetched_date = None

def get_last_fetched_timestamp():
    try:
        with open('./latest_timestamp.txt', 'r') as f:
            timestamp = f.read().strip()
            if not timestamp:
                return None
            return timestamp
    except FileNotFoundError:
        return None
    

def save_last_fetched_timestamp(timestamp):
    with open('./latest_timestamp.txt', 'w') as f:
        f.write(timestamp.isoformat())


def fetch_data(collection_name):
    global is_append, last_fetched_date

    try:
        now = datetime.date.today()
        date_now_string = now.strftime("%Y-%m-%d")
        last_fetched_date = get_last_fetched_timestamp()

        # Compare first last fetched date and date today if there is new data
        if last_fetched_date:
            if last_fetched_date.strip() == date_now_string.strip():
                print("No new data")
                is_append = False
                return 
        
        collection_ref = db.collection(collection_name)
    
        # Decision for fetching firestore data
        if last_fetched_date:
            docs = collection_ref.order_by('__name__').start_at([last_fetched_date]).stream()
            is_append = True
        else:
            docs = collection_ref.stream()
            is_append = False
        
        data_list = []  # Initialize an empty list to store data

        for doc in docs:
            data = doc.to_dict()
            data['date'] = doc.id     # Add the document ID (date) to the data
            data_list.append(data)

        if not data_list:
            return None  
        
        # Save the last timestamp from Firestore Database
        if data_list:
            last_timestamp = max(datetime.datetime.fromisoformat(data['date']).date() for data in data_list)
            save_last_fetched_timestamp(last_timestamp)

        return data_list
    
    except Exception as e:
        print(f"An unexpected error occured: {e}")


# Define a function to calculate moving window statistics
def ma_analysis(df, column, window_size, interval, is_append):
    global last_fetched_date

    # Ensure the column is numeric
    df[column] = pd.to_numeric(df[column], errors='coerce')

    if window_size == 1:
        df[f'{column}_{interval}_mean'] = df[column].rolling(window=1).mean()
        df[f'{column}_{interval}_rate_change'] = df[column].pct_change(periods=1)        
        return df
    
    if not is_append and not last_fetched_date:
        df[f'{column}_{interval}_mean'] = df[column].rolling(window=window_size).mean()
        df[f'{column}_{interval}_std'] = df[column].rolling(window=window_size).std()
        df[f'{column}_{interval}_rate_change'] = df[column].pct_change(periods=window_size)
    else:
        if last_fetched_date:
            date = datetime.datetime.strptime(last_fetched_date, "%Y-%m-%d").date()
            seven_days_ago = date - datetime.timedelta(days=window_size) 
            seven_days_ago = seven_days_ago.strftime('%Y-%m-%d')

            collection_ref = db.collection("CropDataPerDay")
            docs = collection_ref.order_by('__name__').start_at([seven_days_ago]).stream()
            
            append_data_list = []
            for doc in docs:
                data = doc.to_dict()
                data['date'] = doc.id
                append_data_list.append(data)

            append_df = pd.DataFrame(append_data_list)

            append_df[column] = pd.to_numeric(append_df[column], errors='coerce')

            append_df[f'{column}_{interval}_mean'] = append_df[column].rolling(window=window_size).mean()
            append_df[f'{column}_{interval}_std'] = append_df[column].rolling(window=window_size).std()
            append_df[f'{column}_{interval}_rate_change'] = append_df[column].pct_change(periods=window_size)

            # Append to the previous df
            df.set_index('date', inplace=True)
            append_df.set_index('date', inplace=True)

            # Use combine_first to merge and fill NaN values
            df = df.combine_first(append_df)

            df.reset_index(inplace=True)

            # df = pd.concat([df, append_df], ignore_index=True)
            # df.drop_duplicates(subset=['date'], keep='first', inplace=True)

            # # TROUBLESHOOT PURPOSES
            # with open("data.txt", "w") as f:
            #     f.write(df.to_string(index=False))

            # Filter the new data to last fetched time onwards
            df['date'] = pd.to_datetime(df['date']).dt.date
            cutoff_date = pd.to_datetime(last_fetched_date).date()
            df = df[df['date'] >= cutoff_date]

            # Convert to date only
            df['date'] = pd.to_datetime(df['date']).dt.date
            # Convert the date column back to string format
            df['date'] = df['date'].astype(str)

            # TROUBLESHOOT PURPOSES
            with open("combined.txt", "w") as f:
                f.write(df.to_string())
                
    return df


def reorder_and_filter_columns(df):
    columns_to_remove = [
        'humidity',
        'soilMoisture',
        'lightIntensity',      
        'temperature'
    ]

    preferred_column_order = [
        'date', 
        # Humidity
        'humidity_daily_mean', 'humidity_daily_rate_change', 'humidity_monthly_mean', 'humidity_monthly_rate_change', 'humidity_monthly_std', 
        'humidity_weekly_mean', 'humidity_weekly_rate_change', 'humidity_weekly_std', 
        # Light Intensity 
        'lightIntensity_daily_mean', 'lightIntensity_daily_rate_change',        
        'lightIntensity_monthly_mean', 'lightIntensity_monthly_rate_change', 'lightIntensity_monthly_std',
        'lightIntensity_weekly_mean', 'lightIntensity_weekly_rate_change', 'lightIntensity_weekly_std',
        # Soil Moisture 
        'soilMoisture_daily_mean', 'soilMoisture_daily_rate_change', 
        'soilMoisture_monthly_mean', 'soilMoisture_monthly_rate_change', 'soilMoisture_monthly_std',
        'soilMoisture_weekly_mean', 'soilMoisture_weekly_rate_change', 'soilMoisture_weekly_std', 
        # Temperature 
        'temperature_daily_mean', 'temperature_daily_rate_change',
        'temperature_monthly_mean', 'temperature_monthly_rate_change', 'temperature_monthly_std',
        'temperature_weekly_mean', 'temperature_weekly_rate_change', 'temperature_weekly_std',
        # Statuses
        'humidity_daily_status', 'humidity_weekly_status', 'humidity_monthly_status',
        'lightIntensity_daily_status', 'lightIntensity_weekly_status', 'lightIntensity_monthly_status',
        'soilMoisture_daily_status', 'soilMoisture_weekly_status', 'soilMoisture_monthly_status',
        'temperature_daily_status', 'temperature_weekly_status', 'temperature_monthly_status'   
    ]
    # Drop columns of choice
    df = df.drop(columns=columns_to_remove)    
    # Rearrange based on preferred column
    df = df[preferred_column_order]

    return df


def save_analysis(latest_df, is_append):
    # Save to firestore database
    for _, row in latest_df.iterrows():
        try:
            if is_append:   # Skip the last date to avoid NaN values in rate of change
                is_append = False
                continue
            doc_ref = db.collection("Analysis").document(row['date'])
            # drop date value
            row_data_without_date = row.drop(labels='date')
            doc_ref.set(row_data_without_date.to_dict(), merge=True)
        except Exception as e:
            print(f"An error occured: {e}")
        
    print(f"Data added to Document (Analysis): {latest_df}")


def compare_to_defined_values(value, column):
    if pd.isna(value):
        return np.nan
    
    if column == 'temperature':
        if 38 < value < 33:
            return 'good'
        else:
            return 'bad'
    elif column == 'humidity':
        if 38 < value < 33:
            return 'good'
        else:
            return 'bad'
    elif column == 'lightIntensity':
        if 38 < value < 33:
            return 'good'
        else:
            return 'bad'
    elif column == 'soilMoisture':
        if 38 < value < 33:
            return 'good'
        else:
            return 'bad'


def get_sensor_value_status(df, column):
    for interval in ['daily', 'weekly', 'monthly']:
        df[f'{column}_{interval}_status'] = df[f'{column}_{interval}_mean'].apply(lambda x: compare_to_defined_values(x, column))
    return df


def fuzzy_logic_overall_status(df):
    # Define fuzzy variables
    sensor_value = ctrl.Antecedent(np.arange(0, 101, 1), 'sensor_value')
    status = ctrl.Consequent(np.arange(0, 101, 1), 'status')

    # Define fuzzy membership functions
    sensor_value['low'] = fuzz.trimf(sensor_value.universe, [0, 0, 50])
    sensor_value['medium'] = fuzz.trimf(sensor_value.universe, [0, 50, 100])
    sensor_value['high'] = fuzz.trimf(sensor_value.universe, [50, 100, 100])

    status['unhealthy'] = fuzz.trimf(status.universe, [0, 0, 50])
    status['healthy'] = fuzz.trimf(status.universe, [50, 100, 100])

    # Define fuzzy rules
    rule1 = ctrl.Rule(sensor_value['low'], status['unhealthy'])
    rule2 = ctrl.Rule(sensor_value['medium'], status['healthy'])
    rule3 = ctrl.Rule(sensor_value['high'], status['healthy'])

    # Create control system
    status_ctrl = ctrl.ControlSystem([rule1, rule2, rule3])
    status_sim = ctrl.ControlSystemSimulation(status_ctrl)

    # Apply fuzzy logic to overall status
    df['overall_status'] = df[['temperature_status', 'humidity_status', 'lightIntensity_status', 'soilMoisture_status']].mean(axis=1)
    df['overall_status'] = df['overall_status'].apply(lambda x: get_fuzzy_status(x, status_sim))

    return df

def get_fuzzy_status(value, status_sim):
    status_sim.input['sensor_value'] = value
    status_sim.compute()
    return status_sim.output['status']


def main():
    data_list = fetch_data("CropDataPerDay")
    
    if not data_list:
        print("No data fetched")
        return
    
    df = pd.DataFrame(data_list) 

    # Reorder columns to have 'date' first
    df = df[['date'] + [col for col in df.columns if col != 'date']]          
    
    # Convert relevant columns to numeric
    for column in ['temperature', 'humidity', 'lightIntensity', 'soilMoisture']:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    
    # Get MA of sensors output for daily, weekly, monthly
    for column in ['temperature', 'humidity', 'lightIntensity', 'soilMoisture']:
        df = ma_analysis(df, column, 1, 'daily', is_append=is_append)
        df = ma_analysis(df, column, 7, 'weekly', is_append=is_append)
        df = ma_analysis(df, column, 30, 'monthly', is_append=is_append)
        df = get_sensor_value_status(df, column)

    # # Apply fuzzy logic for overall status
    # df = fuzzy_logic_overall_status(df)
    
    # Filter and rearrange the column order
    df = reorder_and_filter_columns(df)
        
    with open("combined.txt", 'w') as f:
        f.write(df.to_string())

    save_analysis(df, is_append=is_append)

    # NEXT IS PLOT THE CHANGES IN STATS GRAPH
    # FIND RELIABLE DATA FOR BAD, GOOD, HEALTHY DATA

# MAIN FUNCTION
if __name__ == "__main__":
    main()