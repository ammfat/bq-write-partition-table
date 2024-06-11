import pandas as pd
import os
import random
import uuid

# Create a directory to store the CSV files
os.makedirs('data/users/', exist_ok=True)

# Define dates for which we want to create data files
dates = ['2024-06-01', '2024-06-02', '2024-06-03']

# Generate sample data for each date
for date in dates:
    data = [
        {
            "event_date": date,
            "user_id": f"{uuid.uuid4()}",
            "likes": random.randint(0, 50)}
        for i in range(10)
    ]

    df = pd.DataFrame(data)
    file_path = f"data/users/{date.replace('-', '')}.csv"
    df.to_csv(file_path, index=False)

print("Sample data files created.")
