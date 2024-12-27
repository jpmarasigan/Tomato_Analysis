import json
from datetime import datetime

# ...existing code...

def default_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    # Add other types if needed
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

# ...existing code...

# Example usage:
data = {
    # ...existing data...
    "timestamp": datetime.now()  # Assuming this is a DatetimeWithNanoseconds object
}

json_data = json.dumps(data, default=default_converter)
print(json_data)

# ...existing code...
