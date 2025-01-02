import firebase_admin
import datetime
from firebase_admin import credentials, firestore
import datetime
import json 
import random

cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

collection_name = "hardwares"
hardware_id = "hardware120502"

date_collection_ref = db.collection(collection_name).document(hardware_id).collection("2024-12-26")

# Check if the date collection exists
date_docs = date_collection_ref.stream()


def main():
    now = datetime.date.today()
    date_now_str = now.strftime("%Y-%m-%d")
    last_fetched_date = get_last_fetched_timestamp()

    if last_fetched_date:
        if date_now_str.strip() == last_fetched_date.strip():
            print("No New Data")
            return
        
        # Get the main collection
        collection_ref = db.collection(collection_name)
        # Get the hardware id document
        hardware_docs = collection_ref.document(hardware_id).collections()
        filtered_collection = []

        for doc in hardware_docs:
            try:
                # Example of using strptime to parse a date string
                doc_date = datetime.datetime.strptime(doc.id, "%Y-%m-%d").date()
                last_date = datetime.datetime.strptime(last_fetched_date, "%Y-%m-%d").date()

                if doc_date <= last_date:
                    continue
                else:
                    # Get the date collections starting from the last date
                    date_collection_ref = collection_ref.document(hardware_id).collection(doc.id)
                    filtered_collection.append(date_collection_ref)
            except ValueError:
                continue    

        for col in filtered_collection:
            date_docs = col.stream()
            for date_doc in date_docs:
                print(f"Document ID: {date_doc.id}")
                print(f"Document Data: {date_doc.to_dict()}")


def get_last_fetched_timestamp():
    try:
        with open('./latest_timestamp.txt', 'r') as f:
            timestamp = f.read().strip()
            if not timestamp:
                return None
            return timestamp
    except FileNotFoundError:
        return None
    


if __name__ == "__main__":
    main()


    # if not parent_doc_ref.get().exists:
    #     parent_doc_ref.set({"initialized": True})


# if not any(date_docs):
#     print("no docs")
#     # Create a dummy document to ensure the collection exists
#     date_collection_ref.document("dumdawdmy_doc").set({})

# date_docs = date_collection_ref.stream()

# for doc in date_docs:
#     collection_ref = date_collection_ref.document(doc.id)

#     data = {
#         "inamo": 1
#     }

#     try:
#         collection_ref.set(data, merge=True)
#         print("Data sent successfully")
#     except Exception as e:
#         print(f"Error occurred: {e}")
