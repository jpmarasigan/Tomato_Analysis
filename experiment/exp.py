from collections import defaultdict
from datetime import datetime

# List of timestamps as strings
hardware_dates = [
    "2025-01-07-19:41",
    "2025-01-07-19:47",
    "2025-01-07-19:53",
    "2025-01-07-19:54",
    "2025-01-07-19:55",
    "2025-01-07-20:02",
    "2025-01-07-23:03",
    "2025-01-08-02:12",
    "2025-01-08-02:16",
    "2025-01-08-02:21",
    "2025-01-08-02:25",
    "2025-01-08-02:29"
]

# Dictionary to store the latest time per date
date_dict = defaultdict(str)

print(f"Date dict: {date_dict}")

# Iterate over all the hardware dates
for data in hardware_dates:
    # Convert string to datetime object
    timestamp = datetime.strptime(data, "%Y-%m-%d-%H:%M")
    
    # Extract the date part (YYYY-MM-DD)
    date = timestamp.strftime("%Y-%m-%d")
    
    # Compare and store the latest time per date
    if date_dict[date] == '' or timestamp > datetime.strptime(date_dict[date], "%Y-%m-%d-%H:%M"):
        date_dict[date] = data

# Extract the latest times per date
date_list = list(date_dict.values())

# Output the result
print(date_list)
