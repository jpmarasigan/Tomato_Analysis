import firebase_admin
import datetime
from firebase_admin import credentials, firestore
import datetime
import random

cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

collection_name = "hardwares"
hardware_id = "hardware120502"

def add_mock_data(num_of_iter):
    parent_doc_ref = db.collection(collection_name).document(hardware_id)
    date_collections = parent_doc_ref.collections()
    start_date = None
    
    for date_collection in date_collections:
        start_date = datetime.datetime.strptime(date_collection.id, "%Y-%m-%d").date()
    
    for _ in range(num_of_iter):
        start_date = start_date + datetime.timedelta(days=1)
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        time = f"{hours:02}:{minutes:02}"
 
        date_str = start_date.strftime("%Y-%m-%d")
        time_docs = parent_doc_ref.collection(date_str).document(time)

        if not time_docs.get().exists:
            data = {
                'humidity': random.randint(0, 70),
                'lightIntensity': random.randint(0, 100),
                'soilMoisture1': random.randint(0, 70),
                'soilMoisture2': random.randint(0, 70),
                'soilMoisture3': random.randint(0, 70),
                'temperature': random.randint(0, 70),
            }
            time_docs.set(data)

        
def del_collection_date(date):
    collection_ref = db.collection(collection_name).document(hardware_id).collections()

    for collection in collection_ref:
        try:
            col_date = datetime.datetime.strptime(collection.id, '%Y-%m-%d').date()
            start_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

            if start_date <= col_date:
                # Delete all teh documents inside the collection
                # to able to delete the collection itself
                docs = collection.stream()
                for doc in docs:
                    try:
                        collection.document(doc.id).delete()
                        print(f"Deleting document: {doc.id}")
                    except Exception as e:
                        print("Error deleting the document inside the collection: ", e)
        except ValueError:
              print(f"Skipping collection {collection.id} as it does not match date format.")
            

if __name__ == "__main__":
    add_mock_data(num_of_iter=20)
    # del_collection_date("2025-01-01")