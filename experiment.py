import os
import json
import pandas as pd


data = {
    "date": ["2024-12-02", "2024-12-03", "2024-12-04"],
    "humidity": [57.7, 59.9, 56.8],
    "soilMoisture": [41, 50, 43],
    "lightIntensity": [64, 68, 67],
    "temperature": [30.3, 31.7, 34.2],
    "temperature_daily_rate_change": [None, 0.046205, 0.078864]
}

df = pd.DataFrame(data)

for index, row in df.iterrows():
    row_data = row.drop(labels='date')
    print(row_data)
    # Save or process row_data here



