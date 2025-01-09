import firebase_admin
from firebase_admin import credentials, firestore
import random
from datetime import datetime
from pytz import timezone

cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
firebase_admin.initialize_app(cred)

db = firestore.client()          # Initialize Firestore client

dates = ["2026-12-25", "2026-12-26", "2026-12-28", "2026-12-30", "2026-12-31"]
# Remove the hours list
# hours = ["12:32", "44:44", "55.22", "66:66"]

collection_name = "Detection"
hardware_name = "hardware120505"

parent_doc_ref = db.collection(collection_name).document(str(hardware_name))
# If hardware ID not exists, create one
if not parent_doc_ref.get().exists:
    parent_doc_ref.set({"initialized": True})

for date in dates:
    date_doc_ref = parent_doc_ref.collection("dates").document(date)

    if not date_doc_ref.get().exists:
        date_doc_ref.set({"initialized": True})
        print(f"Date not exist. Creating one... {date_doc_ref.id}")

    # Generate the current time in HH:mm format and the full timestamp in the Philippines time zone
    tz = timezone('Asia/Manila')
    current_time = datetime.now(tz).strftime("%H:%M")
    time_stamp_format = "%Y-%m-%d %H:%M:%S %Z%z"
    timestamp = datetime.now(tz).strftime(time_stamp_format)

    hour_doc_ref = date_doc_ref.collection("hours").document(current_time)

    if not hour_doc_ref.get().exists:
        hour_doc_ref.set({"initialized": True})
        print(f"Hour not exist. Creating one... {hour_doc_ref.id}")

    data = {
        "createdAt": timestamp,
        "hello": random.randint(0, 100),
        "world": random.randint(0, 100)
    }
    hour_doc_ref.set(data, merge=True)


# for date in dates:
#     # Get date document which is the unique key for date
#     date_collection_ref = db.collection("Analysis2").document(str(hardware_name)).collection("dates")
#     new_doc_ref = date_collection_ref.add({"initialized": True})  # Automatically generate a new document ID
#     print(f"Created document with ID: {new_doc_ref[1].id}")

#     data = {
#         "hello": random.randint(0, 100),
#         "world": random.randint(0, 100)
#     }
#     # Use the second element of the tuple to set the data
#     new_doc_ref[1].set(data, merge=True)