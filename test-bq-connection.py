from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables from .env file
load_dotenv()

# # Initialize a BigQuery client
client = bigquery.Client()

# Define a simple query
query = """
    SELECT status, COUNT(station_id) as total_station
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations`
    GROUP BY status
    ORDER BY total_station DESC
    LIMIT 10
"""

# Run the query and fetch results
query_job = client.query(query)
results = query_job.result()

# Print the results
for row in results:
    print(f"{row.status}: {row.total_station}")
