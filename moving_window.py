import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import datetime
import os 

cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
firebase_admin.initialize_app(cred)

db = firestore.client()          # Initialize Firestore client
is_append = False


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
    global is_append

    try:
        now = datetime.date.today()
        date_now_string = now.strftime("%Y-%m-%d")
        last_fetched_date = get_last_fetched_timestamp()

        # Compare first last fetched date and date today if there is new data
        if last_fetched_date:
            if last_fetched_date.strip() == date_now_string.strip():
                print("No new data")
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
def moving_window_analysis(df, window_size, column, interval):
    if window_size != 1: 
        df[f'{column}_{interval}_mean'] = df[column].rolling(window=window_size).mean()
        df[f'{column}_{interval}_std'] = df[column].rolling(window=window_size).std()
    df[f'{column}_{interval}_rate_change'] = df[column].pct_change(periods=window_size)
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


def main():
    data_list = fetch_data("CropDataPerDay")
    
    if not data_list:
        print("No data fetched")
        return
    
    df = pd.DataFrame(data_list) 
    # Reorder columns to have 'date' first
    df = df[['date'] + [col for col in df.columns if col != 'date']]          
    
    # Convert 'temperature' column to numeric
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    
    # Get MA of daily
    df = moving_window_analysis(df, 1, 'temperature', 'daily')

    save_analysis(df, is_append=is_append)


# MAIN FUNCTION
if __name__ == "__main__":
    main()













# def save_analysis(latest_df, is_append):
#     if is_append:
#         existing_df = pd.read_csv("original.txt", sep='\s+')
#         latest_df = pd.concat([existing_df, latest_df], ignore_index=True)  
        
#     with open("original.txt", "w") as f:
#         f.write(latest_df.to_string())