import os
import json

new_entry = {
    "2025-02-04": {
        "humidity": "60.0",
        "lightIntensity": "50",
        "soilMoisture": "40",
        "temperature": "35.0"
    }
}

file_path = './private/tomato_firebase_sensor_data.json'

with open(file_path, "r+") as f:
    data = json.load(f)
    data.update(new_entry)
    f.seek(0)
    json.dump(data, f, indent=4)
    f.truncate()

