import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta  # Keep this import
from collections import OrderedDict
import threading
import random
import time
import json
import os

cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()


def list_collections():
    collections = db.collections()
    for collection in collections:
        print(f'Collection ID: {collection.id}')


def get_last_fetched_timestamp():
    try:
        with open('./private/last_fetched_timestamp.txt', 'r') as f:
            timestamp = f.read().strip()
            if not timestamp:
                return None
            return timestamp
    except FileNotFoundError:
        return None


def save_last_fetched_timestamp(timestamp):
    with open('./private/last_fetched_timestamp.txt', 'w') as f:
        f.write(timestamp.isoformat())


def fetch_data(collection_name):
    is_append = False
    
    try:
        now = datetime.today().date()
        date_now_string = now.strftime("%Y-%m-%d")
        last_fetched_date = get_last_fetched_timestamp()

        # Compare first last fetched date and date today if there is new data
        if last_fetched_date:
            if last_fetched_date.strip() == date_now_string.strip():
                print("No new data")
                return 
        
        collection_ref = db.collection(collection_name)

        if last_fetched_date:
            docs = collection_ref.order_by('__name__').start_after([last_fetched_date]).stream()
            is_append = True
        else:
            docs = collection_ref.stream()
    
        data_list = {}
        start_time = time.time()        # Timer starts

        for doc in docs:
            date_object = doc.id        
            data = doc.to_dict()
            
            if isinstance(data, dict):
                ordered_data = OrderedDict(sorted(data.items()))

                # Map the data into new dict format 
                data_list[date_object] = {
                    'humidity': ordered_data.get('humidity', None),
                    'lightIntensity': ordered_data.get('lightIntensity', None),
                    'soilMoisture': ordered_data.get('soilMoisture', None),
                    'temperature': ordered_data.get('temperature', None)
                }
            else:
                print(f"Unexpected format happen to occur: {doc.id}: {type(doc.id)}")

        print("Tomato Latest Data:")
        print(json.dumps(data_list, indent=4))

        if is_append:
            save_to_json(data_list, is_append=is_append)
        else:
            save_to_json(data_list, is_append=False)
        
        end_time = time.time()      # end time
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time:.2f} seconds")

        # Save the last timestamp from Firestore Database
        if data_list:
            last_timestamp = max(datetime.fromisoformat(date).date() for date in data_list.keys())
            save_last_fetched_timestamp(last_timestamp)
          
    except Exception as e:
        print(f"An error occurred: {e}")


def save_to_json(tomato_data, is_append):
    file_path = './private/tomato_firebase_sensor_data.json'
    
    if is_append:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
            except FileNotFoundError:
                existing_data = {}
            except PermissionError:
                print(f"Do not have permission to open this file")
                return
            
            existing_data.update(tomato_data)
            with open(file_path, 'w') as f:
                json.dump(existing_data, f, indent=4)
            return

    with open(file_path, 'w') as f:
        json.dump(tomato_data, f, indent=4)


# MAIN FUNCTION
if __name__ == "__main__":
    # list_collections()            # Display collections on firestore
    
    # Adding random mock data in Firestore Database
    # task_thread = threading.Thread(target=schedule_add_task)
    # task_thread.start()

    # Add data for specific date
    # add_random_data("2025-02-03")
    
    # Fetch data from Firestore
    fetch_data("CropDataPerDay")

    # UP NEXT:
    # (1) CREATE A TIMESTAMP OF ONCE A DAY THIS CODE WILL RUN    
    
    # (2) IF THE DATA GOES LARGE, IT WILL CAUSE DELAY. THEREFORE  
    #     THE SOLUTION IS TO USE APPEND INSTEAD OF RETRIEVING FROM THE START














# def fetch_data_no_last_fetched(collection_name):
#     collection_ref = db.collection(collection_name)
#     docs = collection_ref.stream()
    
#     start_time = time.time()        # Timer starts
#     data_list = {}
#     for doc in docs:
#         date_object = doc.id
#         data = doc.to_dict()
#         ordered_data = OrderedDict(sorted(data.items()))

#         data_list[date_object] = {
#             'humidity': ordered_data.get('humidity', None),
#             'lightIntensity': ordered_data.get('lightIntensity', None),
#             'soilMoisture': ordered_data.get('soilMoisture', None),
#             'temperature': ordered_data.get('temperature', None)
#         }

#     save_to_json(data_list, is_append=False)
    
#     end_time = time.time()      # end time
#     elapsed_time = end_time - start_time
#     print(f"Execution time: {elapsed_time:.2f} seconds")


# def add_random_data(date_document):
#     collection_name = "CropDataPerDay"
#     time_field = datetime.now().strftime("%H:%M:%S")

#     # Mock random values
#     data = {
#         "humidity": f"{random.uniform(50, 70):.1f}",
#         "lightIntensity": f"{random.randint(50, 70)}",
#         "soilMoisture": f"{random.randint(40, 50)}",
#         "temperature": f"{random.uniform(30, 40)::.1f}",
#         "timestamp": datetime.now()  # Add timestamp field
#     }

#     # Check if it is added or not
#     try:
#         doc_ref = db.collection(collection_name).document(date_document)
#         doc_ref.set(data, merge=True)
#         print(f"Data added to Firestore: {data}")
#     except Exception as e:
#         print(f"An error occurred: {e}")


# def schedule_add_task():
#     current_date = datetime.now()
#     entries_per_day = 1

#     while True:
#         date_document = current_date.strftime("%Y-%m-%d")
#         for _ in range(entries_per_day):
#             add_random_data(date_document)

#         # Move to the next day
#         current_date += timedelta(days=1)
