import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import datetime
import json

cred = credentials.Certificate("./private/agripedia-c5439-firebase-adminsdk-6u9cs-65edf0294b.json")
firebase_admin.initialize_app(cred)

db = firestore.client()   

def get_last_fetched_timestamp():
    try:
        with open('./latest_timestamp.txt', 'r') as f:
            timestamp = f.read().strip()
            if not timestamp:
                return None
            return timestamp
    except FileNotFoundError:
        return None


def reorder_and_filter_columns(df):
    preferred_columns_and_order = [
        'temperature_daily_rate_change',
        'temperature_weekly_mean',
        'temperature_weekly_std', 
        'temperature_weekly_rate_change',
        'temperature_monthly_mean',
        'temperature_monthly_std',
        'temperature_monthly_rate_change',        
    ]
    df = df[preferred_columns_and_order]    

    return df



last_fetched_date = get_last_fetched_timestamp()

date = datetime.datetime.strptime(last_fetched_date, "%Y-%m-%d").date()
seven_days_ago = date - datetime.timedelta(days=7) 
seven_days_ago = seven_days_ago.strftime("%Y-%m-%d")

collection_ref = db.collection("Analysis")
docs = collection_ref.order_by('__name__').start_at([seven_days_ago]).stream()

data_list = []
for doc in docs:
    data = doc.to_dict()
    data['date'] = doc.id
    data_list.append(data)

df = pd.DataFrame(data_list)
df = reorder_and_filter_columns(df)

print(df.columns)
