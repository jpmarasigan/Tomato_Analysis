import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import datetime
import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl
from collections import defaultdict
import matplotlib.pyplot as plt
import json
import asyncio  # Import asyncio for asynchronous programming
import time

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
    
    hardware_crop_dict = {}

    try:
        main_col_ref = db.collection(collection_name)
        user_doc_ref = main_col_ref.stream()
        user_ids = [doc.id for doc in user_doc_ref]
        
        for user_id in user_ids:
            sub_collection = list(main_col_ref.document(user_id).collections())

            if not sub_collection:
                print(f"User {user_id} has no crops collections")
                continue        # go to next user
            
            crop_col_ref = main_col_ref.document(user_id).collection("crops")
            crop_doc_ref = crop_col_ref.stream()

            crop_ids = [crop.id for crop in crop_doc_ref]

            # Get inside the crop info
            for crop_id in crop_ids:
                crop_doc_field = crop_col_ref.document(crop_id).get()

                if crop_doc_field.exists:
                    data = crop_doc_field.to_dict()
                    hardware_crop_dict[data['hardwareID']] = data['cropType'].lower()

        data_list = []
        structured_hardware_data = {}

        for hardware_id, crop_type in hardware_crop_dict.items():
            hardware_col_ref = db.collection(hardware_id)
            
            # Check if hardware_id first exist
            if not (hardware_col_ref.limit(1).get()):
                print(f"Hardware {hardware_id} not exist")
                continue

            # Get all the dates of data in hardware_id collection (hardware120502 -> 2025-01-01-19:57, ...,)
            hardware_dates = hardware_col_ref.stream()
            
            # Dictionary to store the latest time per date
            date_dict = defaultdict(str)

            for dates in hardware_dates:
                # Convert string to datetime object
                timestamp = datetime.datetime.strptime(dates.id, "%Y-%m-%d-%H:%M")
                
                # Extract the date part (YYYY-MM-DD)
                date = timestamp.strftime("%Y-%m-%d")
                
                # Compare and store the latest time per date
                if date_dict[date] == '' or timestamp > datetime.datetime.strptime(date_dict[date], "%Y-%m-%d-%H:%M"):
                    date_dict[date] = dates.id

            # Extract the latest times per date
            date_list = list(date_dict.values())

            data = {}
            for date in date_list:
                doc_ref = hardware_col_ref.document(date).get()

                # Store all data (values) per date (key)
                if doc_ref.exists:
                    sensor_data = doc_ref.to_dict()
                    sensor_data["cropType"] = crop_type
                    data[date] = sensor_data

            structured_hardware_data[hardware_id] = data    
        data_list.append(structured_hardware_data)

        print(json.dumps(data_list, indent=4))

        with open("sample.txt", "w") as f:
            f.write(json.dumps(data_list, indent=4)) 

        return data_list



            

            
        # Fetch hardware document (hardware id)
        # hardware_docs = collection_ref.stream()
        
        # data_list = []  # Initialize an empty list to store data

        # for hardware_doc in hardware_docs:
        #     hardware_sensor_data = {}
        #     # Get dates under the given document hardware id   
        #     date_colls = collection_ref.document(hardware_doc.id).collections()
        #     for date_coll in date_colls:
        #         date_docs = date_coll.stream()

        #         # Sort date_docs by the document ID (which is assumed to be a date)
        #         sorted_date_docs = sorted(date_docs, key=lambda doc: doc.id, reverse=True)
                
        #         if sorted_date_docs:
        #             last_date_doc = sorted_date_docs[0]
        #             data = last_date_doc.to_dict()
        #             # Convert DatetimeWithNanoseconds to string
        #             for key, value in data.items():
        #                 # Converting DatetimeWithNanoseconds to a string
        #                 if isinstance(value, datetime.datetime):
        #                     data[key] = value.isoformat()
        #             hardware_sensor_data[date_coll.id] = data

        #     structured_data = {hardware_doc.id: hardware_sensor_data}
        #     data_list.append(structured_data)

        # if not data_list:
        #     return None  

        # with open("sample.txt", "w") as f:
        #     f.write(json.dumps(data_list, indent=4)) 

        # return data_list
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Define a function to calculate moving window statistics
async def ma_analysis(df, column, window_size, interval, is_append):
    global last_fetched_date

    # Ensure the column is numeric
    if column not in df.columns:
        print(f"Column {column} does not exist in the DataFrame.")
        return df
    
    # Ensure the column is numeric
    df[column] = pd.to_numeric(df[column], errors='coerce')

    if window_size == 1:
        df[f'{column}_{interval}_mean'] = df[column].rolling(window=1).mean()
        df[f'{column}_{interval}_rate_change'] = df[column].pct_change(periods=1, fill_method=None)
        df[f'{column}_{interval}_rate_change'] = df.apply(
            lambda row: 0 if row[column] == 0 and pd.isna(row[f'{column}_{interval}_rate_change']) else row[f'{column}_{interval}_rate_change'],
            axis=1
        )
        return df
    
    if not is_append and not last_fetched_date:
        df[f'{column}_{interval}_mean'] = df[column].rolling(window=window_size).mean()
        df[f'{column}_{interval}_std'] = df[column].rolling(window=window_size).std()
        df[f'{column}_{interval}_rate_change'] = df[column].pct_change(periods=window_size, fill_method=None)
        df[f'{column}_{interval}_rate_change'] = df.apply(
            lambda row: 0 if row[column] == 0 and pd.isna(row[f'{column}_{interval}_rate_change']) else row[f'{column}_{interval}_rate_change'],
            axis=1
        )
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
            append_df[f'{column}_{interval}_rate_change'] = append_df[column].pct_change(periods=window_size, fill_method=None)
            append_df[f'{column}_{interval}_rate_change'] = append_df.apply(
                lambda row: 0 if row[column] == 0 and pd.isna(row[f'{column}_{interval}_rate_change']) else row[f'{column}_{interval}_rate_change'],
                axis=1
            )

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


async def process_columns(df, columns, is_append):
    tasks = []
    for column in columns:
        tasks.append(ma_analysis(df, column, 1, 'daily', is_append))
        tasks.append(ma_analysis(df, column, 7, 'weekly', is_append))
        tasks.append(ma_analysis(df, column, 30, 'monthly', is_append))
    await asyncio.gather(*tasks)


def reorder_and_filter_columns(df):
    columns_to_remove = [
        'humidity',
        'lightIntensity',      
        'temperature',
        'soilMoisture1',
        'soilMoisture2',
        'soilMoisture3',
        'cropType'
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
        'soilMoisture1_daily_mean', 'soilMoisture1_daily_rate_change', 
        'soilMoisture1_monthly_mean', 'soilMoisture1_monthly_rate_change', 'soilMoisture1_monthly_std',
        'soilMoisture1_weekly_mean', 'soilMoisture1_weekly_rate_change', 'soilMoisture1_weekly_std', 
        'soilMoisture2_daily_mean', 'soilMoisture2_daily_rate_change', 
        'soilMoisture2_monthly_mean', 'soilMoisture2_monthly_rate_change', 'soilMoisture2_monthly_std',
        'soilMoisture2_weekly_mean', 'soilMoisture2_weekly_rate_change', 'soilMoisture2_weekly_std', 
        'soilMoisture3_daily_mean', 'soilMoisture3_daily_rate_change', 
        'soilMoisture3_monthly_mean', 'soilMoisture3_monthly_rate_change', 'soilMoisture3_monthly_std',
        'soilMoisture3_weekly_mean', 'soilMoisture3_weekly_rate_change', 'soilMoisture3_weekly_std', 
        # Temperature 
        'temperature_daily_mean', 'temperature_daily_rate_change',
        'temperature_monthly_mean', 'temperature_monthly_rate_change', 'temperature_monthly_std',
        'temperature_weekly_mean', 'temperature_weekly_rate_change', 'temperature_weekly_std',
        # Statuses
        'humidity_daily_status', 'humidity_weekly_status', 'humidity_monthly_status',
        'lightIntensity_daily_status', 'lightIntensity_weekly_status', 'lightIntensity_monthly_status',
        'soilMoisture1_daily_status', 'soilMoisture1_weekly_status', 'soilMoisture1_monthly_status',
        'soilMoisture2_daily_status', 'soilMoisture2_weekly_status', 'soilMoisture2_monthly_status',
        'soilMoisture3_daily_status', 'soilMoisture3_weekly_status', 'soilMoisture3_monthly_status',
        'temperature_daily_status', 'temperature_weekly_status', 'temperature_monthly_status',
        'daily_overall_status', 'weekly_overall_status', 'monthly_overall_status', 'overall_status'  
    ]
    # Drop columns of choice
    df = df.drop(columns=columns_to_remove)    
    # Rearrange based on preferred column
    df = df[preferred_column_order]

    return df


def save_analysis(latest_df, hardware_id, is_append):
    print(hardware_id)
    batch = db.batch()  # Create a batch for batch writes

    # Save to firestore database
    for _, row in latest_df.iterrows():
        try:
            if is_append:   # Skip the last date to avoid NaN values in rate of change
                is_append = False
                continue

            parent_doc_ref = db.collection("Analysis2").document(str(hardware_id))
            # If hardware id not exists, create one
            if not parent_doc_ref.get().exists:
                parent_doc_ref.set({"initialized": True})

            # Get the date from the row (assuming 'date' is a string in the format YYYY-MM-DD)
            date_str = datetime.datetime.strptime(row['date'], '%Y-%m-%d').strftime('%Y%m%d')  # Convert to YYYYMMDD
            document_id = f"{date_str}"  # Format as YYYYMMDDtomato

            # Get date document which is the unique key for date
            date_collection_ref = db.collection("Analysis2").document(str(hardware_id)).collection("dates")
            doc_ref = date_collection_ref.document(document_id)
            
            # drop date value
            # row_data_without_date = row.drop(labels='date')
            batch.set(doc_ref, row.to_dict(), merge=True)  # Add to batch
        except Exception as e:
            print(f"An error occured: {e}")
        
    batch.commit()  # Commit the batch
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
    elif column == 'soilMoisture1':
        if 38 < value < 33:
            return 'good'
        else:
            return 'bad'
    elif column == 'soilMoisture2':
        if 38 < value < 33:
            return 'good'
        else:
            return 'bad'
    elif column == 'soilMoisture3':
        if 38 < value < 33:
            return 'good'
        else:
            return 'bad'


def get_sensor_value_status(df, column):
    for interval in ['daily', 'weekly', 'monthly']:
        df[f'{column}_{interval}_status'] = df[f'{column}_{interval}_mean'].apply(lambda x: compare_to_defined_values(x, column))
    return df


def classify_overall_status(value):
    if value >= 50:
        return 'healthy'
    else:
        return 'unhealthy'


def fuzzy_logic_overall_status(df):
    def create_fuzzy_system(crop_type):
        temperature_mean = ctrl.Antecedent(np.arange(-1, 61, 1), 'temperature_mean')
        humidity_mean = ctrl.Antecedent(np.arange(-1, 102, 1), 'humidity_mean')
        lightIntensity_mean = ctrl.Antecedent(np.arange(-1, 102, 1), 'lightIntensity_mean')
        soilMoisture1_mean = ctrl.Antecedent(np.arange(-1, 102, 1), 'soilMoisture1_mean')
        soilMoisture2_mean = ctrl.Antecedent(np.arange(-1, 102, 1), 'soilMoisture2_mean')
        soilMoisture3_mean = ctrl.Antecedent(np.arange(-1, 102, 1), 'soilMoisture3_mean')
        # Overall if 100 perfectly healthy
        overall_status = ctrl.Consequent(np.arange(-1, 101, 1), 'overall_status')
        
        if crop_type.lower() == "potato":
            # Ideal Temp 13-29
            temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [-1, -1, 13])
            temperature_mean['ideal'] = fuzz.trimf(temperature_mean.universe, [12, 21, 30])     
            temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [29, 60, 60])
            
            # Ideal Humid 50-(60-80)-100
            humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [-1, -1, 50])
            humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [49, 60, 80, 101])
            humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [100, 101, 101])

            # Ideal Light Exposure 70-(80-100)
            lightIntensity_mean['not ideal low'] = fuzz.trimf(lightIntensity_mean.universe, [-1, -1, 70])
            lightIntensity_mean['ideal'] = fuzz.trapmf(lightIntensity_mean.universe, [69, 80, 100, 101])
            lightIntensity_mean['not ideal high'] = fuzz.trimf(lightIntensity_mean.universe, [100, 101, 101])

            # Ideal Soil Moisture 70-90
            soilMoisture1_mean['not ideal low'] = fuzz.trimf(soilMoisture1_mean.universe, [-1, -1, 70])
            soilMoisture1_mean['ideal'] = fuzz.trapmf(soilMoisture1_mean.universe, [69, 70, 90, 91])
            soilMoisture1_mean['not ideal high'] = fuzz.trimf(soilMoisture1_mean.universe, [90, 100, 100])
            soilMoisture2_mean['not ideal low'] = fuzz.trimf(soilMoisture2_mean.universe, [-1, -1, 70])
            soilMoisture2_mean['ideal'] = fuzz.trapmf(soilMoisture2_mean.universe, [69, 70, 90, 91])
            soilMoisture2_mean['not ideal high'] = fuzz.trimf(soilMoisture2_mean.universe, [90, 100, 100])
            soilMoisture3_mean['not ideal low'] = fuzz.trimf(soilMoisture3_mean.universe, [-1, -1, 70])
            soilMoisture3_mean['ideal'] = fuzz.trapmf(soilMoisture3_mean.universe, [69, 70, 90, 91])
            soilMoisture3_mean['not ideal high'] = fuzz.trimf(soilMoisture3_mean.universe, [90, 100, 100])
        
        elif crop_type.lower() == 'tomato':
            # Ideal Temp 22-29
            temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [-1, -1, 22])
            temperature_mean['ideal'] = fuzz.trimf(temperature_mean.universe, [21, 26, 30])     
            temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [29, 60, 60])
            
            # Ideal Humid 65-85
            humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [-1, -1, 65])
            humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [64, 65, 85, 86])
            humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [85, 101, 101])

            # Ideal Light Exposure 70-(80-100)
            lightIntensity_mean['not ideal low'] = fuzz.trimf(lightIntensity_mean.universe, [-1, -1, 70])
            lightIntensity_mean['ideal'] = fuzz.trapmf(lightIntensity_mean.universe, [69, 80, 100, 101])
            lightIntensity_mean['not ideal high'] = fuzz.trimf(lightIntensity_mean.universe, [100, 101, 101])

            # Ideal Soil Moisture 50-70
            soilMoisture1_mean['not ideal low'] = fuzz.trimf(soilMoisture1_mean.universe, [-1, -1, 50])
            soilMoisture1_mean['ideal'] = fuzz.trapmf(soilMoisture1_mean.universe, [49, 50, 70, 71])
            soilMoisture1_mean['not ideal high'] = fuzz.trimf(soilMoisture1_mean.universe, [70, 100, 100])
            soilMoisture2_mean['not ideal low'] = fuzz.trimf(soilMoisture2_mean.universe, [-1, -1, 50])
            soilMoisture2_mean['ideal'] = fuzz.trapmf(soilMoisture2_mean.universe, [49, 50, 70, 71])
            soilMoisture2_mean['not ideal high'] = fuzz.trimf(soilMoisture2_mean.universe, [70, 100, 100])
            soilMoisture3_mean['not ideal low'] = fuzz.trimf(soilMoisture3_mean.universe, [-1, -1, 50])
            soilMoisture3_mean['ideal'] = fuzz.trapmf(soilMoisture3_mean.universe, [49, 50, 70, 71])
            soilMoisture3_mean['not ideal high'] = fuzz.trimf(soilMoisture3_mean.universe, [70, 100, 100])
        
        elif crop_type.lower() == 'pepper':
            # Ideal Temp 16-21
            temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [-1, -1, 16])
            temperature_mean['ideal'] = fuzz.trapmf(temperature_mean.universe, [15, 16, 21, 22])     
            temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [21, 60, 60])

            # Ideal Humid 65-85
            humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [-1, -1, 65])
            humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [64, 65, 85, 86])
            humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [85, 101, 101])

            # Ideal Light Exposure 60-100
            lightIntensity_mean['not ideal low'] = fuzz.trimf(lightIntensity_mean.universe, [-1, -1, 60])
            lightIntensity_mean['ideal'] = fuzz.trapmf(lightIntensity_mean.universe, [59, 60, 100, 101])
            lightIntensity_mean['not ideal high'] = fuzz.trimf(lightIntensity_mean.universe, [100, 101, 101])

            # Ideal Soil Moisture 20-60
            soilMoisture1_mean['not ideal low'] = fuzz.trimf(soilMoisture1_mean.universe, [-1, -1, 20])
            soilMoisture1_mean['ideal'] = fuzz.trapmf(soilMoisture1_mean.universe, [19, 20, 60, 61])
            soilMoisture1_mean['not ideal high'] = fuzz.trimf(soilMoisture1_mean.universe, [60, 100, 100])
            soilMoisture2_mean['not ideal low'] = fuzz.trimf(soilMoisture2_mean.universe, [-1, -1, 20])
            soilMoisture2_mean['ideal'] = fuzz.trapmf(soilMoisture2_mean.universe, [19, 20, 60, 61])
            soilMoisture2_mean['not ideal high'] = fuzz.trimf(soilMoisture2_mean.universe, [60, 100, 100])
            soilMoisture3_mean['not ideal low'] = fuzz.trimf(soilMoisture3_mean.universe, [-1, -1, 20])
            soilMoisture3_mean['ideal'] = fuzz.trapmf(soilMoisture3_mean.universe, [19, 20, 60, 61])
            soilMoisture3_mean['not ideal high'] = fuzz.trimf(soilMoisture3_mean.universe, [60, 100, 100])
        
        elif crop_type.lower() == 'cucumber':
            # Ideal Temp 18-35
            temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [-1, -1, 15])
            temperature_mean['ideal'] = fuzz.trapmf(temperature_mean.universe, [14, 21, 26, 30])     
            temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [29, 60, 60])

            # Ideal Humid 60-85
            humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [-1, -1, 60])
            humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [59, 60, 85, 86])
            humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [85, 101, 101])

            # Ideal Light Exposure 60-100
            lightIntensity_mean['not ideal low'] = fuzz.trimf(lightIntensity_mean.universe, [-1, -1, 70])
            lightIntensity_mean['ideal'] = fuzz.trapmf(lightIntensity_mean.universe, [69, 70, 100, 101])
            lightIntensity_mean['not ideal high'] = fuzz.trimf(lightIntensity_mean.universe, [100, 101, 101])

            # Ideal Soil Moisture 80-100
            soilMoisture1_mean['not ideal low'] = fuzz.trimf(soilMoisture1_mean.universe, [-1, -1, 80])
            soilMoisture1_mean['ideal'] = fuzz.trapmf(soilMoisture1_mean.universe, [79, 80, 100, 101])
            soilMoisture1_mean['not ideal high'] = fuzz.trimf(soilMoisture1_mean.universe, [100, 101, 101])
            soilMoisture2_mean['not ideal low'] = fuzz.trimf(soilMoisture2_mean.universe, [-1, -1, 80])
            soilMoisture2_mean['ideal'] = fuzz.trapmf(soilMoisture2_mean.universe, [79, 80, 100, 101])
            soilMoisture2_mean['not ideal high'] = fuzz.trimf(soilMoisture2_mean.universe, [100, 101, 101])
            soilMoisture3_mean['not ideal low'] = fuzz.trimf(soilMoisture3_mean.universe, [-1, -1, 80])
            soilMoisture3_mean['ideal'] = fuzz.trapmf(soilMoisture3_mean.universe, [79, 80, 100, 101])
            soilMoisture3_mean['not ideal high'] = fuzz.trimf(soilMoisture3_mean.universe, [100, 101, 101])

        
        sensors = {
            'temperature_mean': temperature_mean,
            'humidity_mean': humidity_mean,
            'lightIntensity_mean': lightIntensity_mean,
            'soilMoisture1_mean': soilMoisture1_mean,
            'soilMoisture2_mean': soilMoisture2_mean,
            'soilMoisture3_mean': soilMoisture3_mean
        }
        
        # display_calculated_membership(sensors)

        overall_status['unhealthy'] = fuzz.trimf(overall_status.universe, [0, 0, 50])
        overall_status['healthy'] = fuzz.trimf(overall_status.universe, [50, 100, 100])

        rule1 = ctrl.Rule(
            humidity_mean['not ideal low'] | humidity_mean['not ideal high'] |
            temperature_mean['not ideal low'] | temperature_mean['not ideal high'] |
            lightIntensity_mean['not ideal low'] | lightIntensity_mean['not ideal high'] | 
            soilMoisture1_mean['not ideal low'] | soilMoisture1_mean['not ideal high'] |
            soilMoisture2_mean['not ideal low'] | soilMoisture2_mean['not ideal high'] |
            soilMoisture3_mean['not ideal low'] | soilMoisture3_mean['not ideal high'],
            overall_status['unhealthy']
            )
        rule2 = ctrl.Rule(
            humidity_mean['ideal'] & temperature_mean['ideal'] &
            lightIntensity_mean['ideal'] & soilMoisture1_mean['ideal'] &
            soilMoisture2_mean['ideal'] & soilMoisture3_mean['ideal'], 
            overall_status['healthy']
            )

        overall_status_ctrl = ctrl.ControlSystem([rule1, rule2])
        return ctrl.ControlSystemSimulation(overall_status_ctrl)


    # Get the crop type (just one is enough because they are the same)
    crop_type = df['cropType'][0]

    # Create fuzzy systems for daily, weekly, and monthly
    daily_fuzzy_system = create_fuzzy_system(crop_type)
    weekly_fuzzy_system = create_fuzzy_system(crop_type)
    monthly_fuzzy_system = create_fuzzy_system(crop_type)

    # Apply fuzzy logic to overall status for each interval
    df['daily_overall_status'] = df.apply(lambda row: get_fuzzy_status_combined(row, daily_fuzzy_system, 'daily'), axis=1)
    df['weekly_overall_status'] = df.apply(lambda row: get_fuzzy_status_combined(row, weekly_fuzzy_system, 'weekly'), axis=1)
    df['monthly_overall_status'] = df.apply(lambda row: get_fuzzy_status_combined(row, monthly_fuzzy_system, 'monthly'), axis=1)

    # Combine the results to get the final overall status
    df['overall_status'] = df[['daily_overall_status', 'weekly_overall_status', 'monthly_overall_status']].mean(axis=1)

    # Classify the overall status
    df['overall_status_class'] = df['overall_status'].apply(classify_overall_status)

    return df


def get_fuzzy_status_combined(row, fuzzy_system, interval):
    try:
        fuzzy_system.input['temperature_mean'] = row[f'temperature_{interval}_mean']
        fuzzy_system.input['humidity_mean'] = row[f'humidity_{interval}_mean']
        fuzzy_system.input['lightIntensity_mean'] = row[f'lightIntensity_{interval}_mean']
        fuzzy_system.input['soilMoisture1_mean'] = row[f'soilMoisture1_{interval}_mean']
        fuzzy_system.input['soilMoisture2_mean'] = row[f'soilMoisture2_{interval}_mean']
        fuzzy_system.input['soilMoisture3_mean'] = row[f'soilMoisture3_{interval}_mean']
        fuzzy_system.compute()
        print(f"Fuzzy system output for {interval}: {fuzzy_system.output['overall_status']}%")
        return fuzzy_system.output['overall_status']
    except KeyError as e:
        print(f"KeyError: {e}")
        print(f"Row: {row}")
        raise


async def main():
    hardwares = fetch_data("users")

    # For speed up
    # with open("sample.txt", "r") as file:
    #     hardwares = json.load(file)

    if not hardwares:
        print("No data fetched")
        return

    for hardware in hardwares:
        for hardware_id, hardware_data in hardware.items():
            print(hardware_id)
            print(hardware_data)
            # Flatten the nested dictionary
            flattened_data = []
            for date, sensors_data in hardware_data.items():
                all_data = {}
                all_data['date'] = '-'.join(date.split('-')[:3])
                for sensor, data in sensors_data.items():
                    all_data[sensor] = data
                flattened_data.append(all_data)

            # print(json.dumps(flattened_data, indent=4))

            # Create a DataFrame from the flattened data
            df = pd.DataFrame(flattened_data)

            # Reorder columns to have 'date' first
            df = df[['date'] + [col for col in df.columns if col != 'date']]          
            
            # Convert relevant columns to numeric
            columns = ['temperature', 'humidity', 'lightIntensity', 'soilMoisture1', 'soilMoisture2', 'soilMoisture3']
            for column in columns:
                df[column] = pd.to_numeric(df[column], errors='coerce')
            
            # Get MA of sensors output for daily, weekly, monthly
            await process_columns(df, columns, is_append)

            # Get sensor value status
            for column in columns:
                df = get_sensor_value_status(df, column)

            # print(df)

            # Apply fuzzy logic for overall status
            df = fuzzy_logic_overall_status(df)
            
            # Filter and rearrange the column order
            df = reorder_and_filter_columns(df)

            # Write to CSV file
            with open(f"{hardware_id}.txt", 'w') as f:
                f.write(df.to_string())

            save_analysis(df, hardware_id=hardware_id, is_append=is_append)

    # FIND RELIABLE DATA FOR BAD, GOOD, HEALTHY DATA


# MAIN FUNCTION
if __name__ == "__main__":
    # Start time
    start_time = time.time()
    asyncio.run(main())
    # End time
    end_time = time.time()
    # Calculate the time taken
    execution_time = end_time - start_time
    print(f"\nExecution time: {execution_time}\n")



def display_trapezoidal_membership(sensor):
    # Plot all defined membership functions
    plt.figure(figsize=(10, 6))
    for term_name, membership_function in sensor.terms.items():
        plt.plot(sensor.universe, membership_function.mf, label=term_name)

    # Add titles and legend
    plt.title(f'Membership Functions for {sensor.label}')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Membership Degree')
    plt.legend()
    plt.grid()
    plt.show()


# def display_calculated_membership(sensor_dict):
#     data = []
#     for sensor_name, sensor in sensor_dict.items():
#         value_to_check = sensor_data[sensor_name.split('_')[0]]
        
#         # Calculate the membership values for the specified sensor value
#         low_membership = fuzz.interp_membership(sensor.universe, sensor['not ideal low'].mf, value_to_check)
#         ideal_membership = fuzz.interp_membership(sensor.universe, sensor['ideal'].mf, value_to_check)
#         high_membership = fuzz.interp_membership(sensor.universe, sensor['not ideal high'].mf, value_to_check)
        
#         data.append({
#             "Sensor": sensor_name,
#             "Value": value_to_check,
#             "Low": low_membership,
#             "Ideal": ideal_membership,
#             "High": high_membership
#         })

#     # Create DataFrame
#     df = pd.DataFrame(data)

#     # with open("sensorFuzzyDF.txt", "w") as f:
#     #     f.write(df.to_string())

#     print(df)



# OLD CODE (BASIS)
# import firebase_admin
# from firebase_admin import credentials, firestore
# import pandas as pd
# import datetime
# import numpy as np
# import skfuzzy as fuzz
# from skfuzzy import control as ctrl

# cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
# firebase_admin.initialize_app(cred)

# db = firestore.client()          # Initialize Firestore client
# is_append = False
# last_fetched_date = None

# def get_last_fetched_timestamp():
#     try:
#         with open('./latest_timestamp.txt', 'r') as f:
#             timestamp = f.read().strip()
#             if not timestamp:
#                 return None
#             return timestamp
#     except FileNotFoundError:
#         return None
    

# def save_last_fetched_timestamp(timestamp):
#     with open('./latest_timestamp.txt', 'w') as f:
#         f.write(timestamp.isoformat())


# def fetch_data(collection_name):
#     global is_append, last_fetched_date

#     try:
#         now = datetime.date.today()
#         date_now_string = now.strftime("%Y-%m-%d")
#         last_fetched_date = get_last_fetched_timestamp()

#         # Compare first last fetched date and date today if there is new data
#         if last_fetched_date:
#             if last_fetched_date.strip() == date_now_string.strip():
#                 print("No new data")
#                 is_append = False
#                 return 
        
#         collection_ref = db.collection(collection_name)
    
#         # Decision for fetching firestore data
#         if last_fetched_date:
#             docs = collection_ref.order_by('__name__').start_at([last_fetched_date]).stream()
#             is_append = True
#         else:
#             docs = collection_ref.stream()
#             is_append = False
        
#         data_list = []  # Initialize an empty list to store data

#         for doc in docs:
#             data = doc.to_dict()
#             data['date'] = doc.id     # Add the document ID (date) to the data
#             data_list.append(data)

#         if not data_list:
#             return None  
        
#         # Save the last timestamp from Firestore Database
#         if data_list:
#             last_timestamp = max(datetime.datetime.fromisoformat(data['date']).date() for data in data_list)
#             save_last_fetched_timestamp(last_timestamp)

#         return data_list
    
#     except Exception as e:
#         print(f"An unexpected error occured: {e}")


# # Define a function to calculate moving window statistics
# def ma_analysis(df, column, window_size, interval, is_append):
#     global last_fetched_date

#     # Ensure the column is numeric
#     df[column] = pd.to_numeric(df[column], errors='coerce')

#     if window_size == 1:
#         df[f'{column}_{interval}_mean'] = df[column].rolling(window=1).mean()
#         df[f'{column}_{interval}_rate_change'] = df[column].pct_change(periods=1)        
#         return df
    
#     if not is_append and not last_fetched_date:
#         df[f'{column}_{interval}_mean'] = df[column].rolling(window=window_size).mean()
#         df[f'{column}_{interval}_std'] = df[column].rolling(window=window_size).std()
#         df[f'{column}_{interval}_rate_change'] = df[column].pct_change(periods=window_size)
#     else:
#         if last_fetched_date:
#             date = datetime.datetime.strptime(last_fetched_date, "%Y-%m-%d").date()
#             seven_days_ago = date - datetime.timedelta(days=window_size) 
#             seven_days_ago = seven_days_ago.strftime('%Y-%m-%d')

#             collection_ref = db.collection("CropDataPerDay")
#             docs = collection_ref.order_by('__name__').start_at([seven_days_ago]).stream()
            
#             append_data_list = []
#             for doc in docs:
#                 data = doc.to_dict()
#                 data['date'] = doc.id
#                 append_data_list.append(data)

#             append_df = pd.DataFrame(append_data_list)

#             append_df[column] = pd.to_numeric(append_df[column], errors='coerce')

#             append_df[f'{column}_{interval}_mean'] = append_df[column].rolling(window=window_size).mean()
#             append_df[f'{column}_{interval}_std'] = append_df[column].rolling(window=window_size).std()
#             append_df[f'{column}_{interval}_rate_change'] = append_df[column].pct_change(periods=window_size)

#             # Append to the previous df
#             df.set_index('date', inplace=True)
#             append_df.set_index('date', inplace=True)

#             # Use combine_first to merge and fill NaN values
#             df = df.combine_first(append_df)

#             df.reset_index(inplace=True)

#             # df = pd.concat([df, append_df], ignore_index=True)
#             # df.drop_duplicates(subset=['date'], keep='first', inplace=True)

#             # # TROUBLESHOOT PURPOSES
#             # with open("data.txt", "w") as f:
#             #     f.write(df.to_string(index=False))

#             # Filter the new data to last fetched time onwards
#             df['date'] = pd.to_datetime(df['date']).dt.date
#             cutoff_date = pd.to_datetime(last_fetched_date).date()
#             df = df[df['date'] >= cutoff_date]

#             # Convert to date only
#             df['date'] = pd.to_datetime(df['date']).dt.date
#             # Convert the date column back to string format
#             df['date'] = df['date'].astype(str)

#             # TROUBLESHOOT PURPOSES
#             with open("combined.txt", "w") as f:
#                 f.write(df.to_string())
                
#     return df


# def reorder_and_filter_columns(df):
#     columns_to_remove = [
#         'humidity',
#         'soilMoisture',
#         'lightIntensity',      
#         'temperature'
#     ]

#     preferred_column_order = [
#         'date', 
#         # Humidity
#         'humidity_daily_mean', 'humidity_daily_rate_change', 'humidity_monthly_mean', 'humidity_monthly_rate_change', 'humidity_monthly_std', 
#         'humidity_weekly_mean', 'humidity_weekly_rate_change', 'humidity_weekly_std', 
#         # Light Intensity 
#         'lightIntensity_daily_mean', 'lightIntensity_daily_rate_change',        
#         'lightIntensity_monthly_mean', 'lightIntensity_monthly_rate_change', 'lightIntensity_monthly_std',
#         'lightIntensity_weekly_mean', 'lightIntensity_weekly_rate_change', 'lightIntensity_weekly_std',
#         # Soil Moisture 
#         'soilMoisture_daily_mean', 'soilMoisture_daily_rate_change', 
#         'soilMoisture_monthly_mean', 'soilMoisture_monthly_rate_change', 'soilMoisture_monthly_std',
#         'soilMoisture_weekly_mean', 'soilMoisture_weekly_rate_change', 'soilMoisture_weekly_std', 
#         # Temperature 
#         'temperature_daily_mean', 'temperature_daily_rate_change',
#         'temperature_monthly_mean', 'temperature_monthly_rate_change', 'temperature_monthly_std',
#         'temperature_weekly_mean', 'temperature_weekly_rate_change', 'temperature_weekly_std',
#         # Statuses
#         'humidity_daily_status', 'humidity_weekly_status', 'humidity_monthly_status',
#         'lightIntensity_daily_status', 'lightIntensity_weekly_status', 'lightIntensity_monthly_status',
#         'soilMoisture_daily_status', 'soilMoisture_weekly_status', 'soilMoisture_monthly_status',
#         'temperature_daily_status', 'temperature_weekly_status', 'temperature_monthly_status',
#         'daily_overall_status', 'weekly_overall_status', 'monthly_overall_status', 'overall_status'  
#     ]
#     # Drop columns of choice
#     df = df.drop(columns=columns_to_remove)    
#     # Rearrange based on preferred column
#     df = df[preferred_column_order]

#     return df


# def save_analysis(latest_df, is_append):
#     # Save to firestore database
#     for _, row in latest_df.iterrows():
#         try:
#             if is_append:   # Skip the last date to avoid NaN values in rate of change
#                 is_append = False
#                 continue
#             doc_ref = db.collection("Analysis").document(row['date'])
#             # drop date value
#             row_data_without_date = row.drop(labels='date')
#             doc_ref.set(row_data_without_date.to_dict(), merge=True)
#         except Exception as e:
#             print(f"An error occured: {e}")
        
#     print(f"Data added to Document (Analysis): {latest_df}")


# def compare_to_defined_values(value, column):
#     if pd.isna(value):
#         return np.nan
    
#     if column == 'temperature':
#         if 38 < value < 33:
#             return 'good'
#         else:
#             return 'bad'
#     elif column == 'humidity':
#         if 38 < value < 33:
#             return 'good'
#         else:
#             return 'bad'
#     elif column == 'lightIntensity':
#         if 38 < value < 33:
#             return 'good'
#         else:
#             return 'bad'
#     elif column == 'soilMoisture':
#         if 38 < value < 33:
#             return 'good'
#         else:
#             return 'bad'


# def get_sensor_value_status(df, column):
#     for interval in ['daily', 'weekly', 'monthly']:
#         df[f'{column}_{interval}_status'] = df[f'{column}_{interval}_mean'].apply(lambda x: compare_to_defined_values(x, column))
#     return df


# def classify_overall_status(value):
#     if value >= 50:
#         return 'good'
#     else:
#         return 'bad'


# def fuzzy_logic_overall_status(df):
#     def create_fuzzy_system():
#         # Define fuzzy variables
#         # https://bagong.pagasa.dost.gov.ph/information/climate-philippines
#         # Temperature 0-50
#         temperature_mean = ctrl.Antecedent(np.arange(0, 51, 1), 'temperature_mean')
#         # Humidity 0-100
#         humidity_mean = ctrl.Antecedent(np.arange(0, 101, 1), 'humidity_mean')
#         # Light Intensity 0-65536
#         lightIntensity_mean = ctrl.Antecedent(np.arange(0, 65536, 1), 'lightIntensity_mean')
#         # Soil Moisture 0-100
#         soilMoisture_mean = ctrl.Antecedent(np.arange(0, 101, 1), 'soilMoisture_mean')
#         # Overall if 100 perfectly healthy
#         overall_status = ctrl.Consequent(np.arange(0, 101, 1), 'overall_status')

#         # Define fuzzy membership functions
#         temperature_mean['low'] = fuzz.trimf(temperature_mean.universe, [0, 0, 20])
#         temperature_mean['ideal'] = fuzz.trimf(temperature_mean.universe, [18, 21, 25])
#         temperature_mean['high'] = fuzz.trimf(temperature_mean.universe, [23, 50, 50])

#         humidity_mean['low'] = fuzz.trimf(humidity_mean.universe, [0, 0, 30])
#         humidity_mean['ideal'] = fuzz.trimf(humidity_mean.universe, [30, 45, 60])
#         humidity_mean['high'] = fuzz.trimf(humidity_mean.universe, [60, 100, 100])

#         lightIntensity_mean['low'] = fuzz.trimf(lightIntensity_mean.universe, [0, 0, 20000])
#         lightIntensity_mean['ideal'] = fuzz.trimf(lightIntensity_mean.universe, [20000, 35000, 60000])
#         lightIntensity_mean['high'] = fuzz.trimf(lightIntensity_mean.universe, [50000, 65536, 65536])

#         soilMoisture_mean['low'] = fuzz.trimf(soilMoisture_mean.universe, [0, 0, 30])  
#         soilMoisture_mean['ideal'] = fuzz.trimf(soilMoisture_mean.universe, [40, 60, 80])  
#         soilMoisture_mean['high'] = fuzz.trimf(soilMoisture_mean.universe, [70, 100, 100])  

#         overall_status['unhealthy'] = fuzz.trimf(overall_status.universe, [0, 0, 50])
#         overall_status['healthy'] = fuzz.trimf(overall_status.universe, [50, 100, 100])

#         # Define fuzzy rules (TENTATIVE VALUES)
#         rule1 = ctrl.Rule(temperature_mean['low'] | humidity_mean['low'] | lightIntensity_mean['low'] | soilMoisture_mean['low'], overall_status['unhealthy'])
#         rule2 = ctrl.Rule(temperature_mean['ideal'] & humidity_mean['ideal'] & lightIntensity_mean['ideal'] & soilMoisture_mean['ideal'], overall_status['healthy'])
#         rule3 = ctrl.Rule(temperature_mean['high'] & humidity_mean['high'] & lightIntensity_mean['high'] & soilMoisture_mean['high'], overall_status['unhealthy'])

#         # Create control system
#         overall_status_ctrl = ctrl.ControlSystem([rule1, rule2, rule3])
#         return ctrl.ControlSystemSimulation(overall_status_ctrl)

#     # Create fuzzy systems for daily, weekly, and monthly
#     daily_fuzzy_system = create_fuzzy_system()
#     weekly_fuzzy_system = create_fuzzy_system()
#     monthly_fuzzy_system = create_fuzzy_system()

#     # Apply fuzzy logic to overall status for each interval
#     df['daily_overall_status'] = df.apply(lambda row: get_fuzzy_status_combined(row, daily_fuzzy_system, 'daily'), axis=1)
#     df['weekly_overall_status'] = df.apply(lambda row: get_fuzzy_status_combined(row, weekly_fuzzy_system, 'weekly'), axis=1)
#     df['monthly_overall_status'] = df.apply(lambda row: get_fuzzy_status_combined(row, monthly_fuzzy_system, 'monthly'), axis=1)

#     # Combine the results to get the final overall status
#     df['overall_status'] = df[['daily_overall_status', 'weekly_overall_status', 'monthly_overall_status']].mean(axis=1)

#     # Classify the overall status
#     df['overall_status_class'] = df['overall_status'].apply(classify_overall_status)

#     return df


# def get_fuzzy_status_combined(row, fuzzy_system, interval):
#     try:
#         fuzzy_system.input['temperature_mean'] = row[f'temperature_{interval}_mean']
#         fuzzy_system.input['humidity_mean'] = row[f'humidity_{interval}_mean']
#         fuzzy_system.input['lightIntensity_mean'] = row[f'lightIntensity_{interval}_mean']
#         fuzzy_system.input['soilMoisture_mean'] = row[f'soilMoisture_{interval}_mean']
#         fuzzy_system.compute()
#         print(f"Fuzzy system output for {interval}: {fuzzy_system.output['overall_status']}")
#         return fuzzy_system.output['overall_status']
#     except KeyError as e:
#         print(f"KeyError: {e}")
#         print(f"Row: {row}")
#         raise


# def main():
#     data_list = fetch_data("CropDataPerDay")
    
#     if not data_list:
#         print("No data fetched")
#         return
    
#     df = pd.DataFrame(data_list) 

#     # Reorder columns to have 'date' first
#     df = df[['date'] + [col for col in df.columns if col != 'date']]          
    
#     # Convert relevant columns to numeric
#     for column in ['temperature', 'humidity', 'lightIntensity', 'soilMoisture']:
#         df[column] = pd.to_numeric(df[column], errors='coerce')
    
#     # Get MA of sensors output for daily, weekly, monthly
#     for column in ['temperature', 'humidity', 'lightIntensity', 'soilMoisture']:
#         df = ma_analysis(df, column, 1, 'daily', is_append=is_append)
#         df = ma_analysis(df, column, 7, 'weekly', is_append=is_append)
#         df = ma_analysis(df, column, 30, 'monthly', is_append=is_append)
#         df = get_sensor_value_status(df, column)

#     # Apply fuzzy logic for overall status
#     df = fuzzy_logic_overall_status(df)
    
#     # Filter and rearrange the column order
#     df = reorder_and_filter_columns(df)
    
#     # Debugging: Print the processed data
#     print("Processed data:")
#     print(df)

#     with open("combined.txt", 'w') as f:
#         f.write(df.to_string())

#     save_analysis(df, is_append=is_append)

#     # CREATE A SEPARATE FILE TO TEST YOUR FUZZY LOGIC TO MOCK DATA OF (DAILY ONLY)
#     # FIND RELIABLE DATA FOR BAD, GOOD, HEALTHY DATA

# # MAIN FUNCTION
# if __name__ == "__main__":
#     main()