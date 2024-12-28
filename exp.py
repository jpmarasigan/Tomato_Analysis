import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

date_collection_ref = db.collection("Analysis2").document("hardware120502").collection("2024-12-26")

# Check if the date collection exists
date_docs = date_collection_ref.stream()

if not any(date_docs):
    print("no docs")
    # Create a dummy document to ensure the collection exists
    date_collection_ref.document("dumdawdmy_doc").set({})

date_docs = date_collection_ref.stream()

for doc in date_docs:
    collection_ref = date_collection_ref.document(doc.id)

    data = {
        "inamo": 1
    }

    try:
        collection_ref.set(data, merge=True)
        print("Data sent successfully")
    except Exception as e:
        print(f"Error occurred: {e}")
