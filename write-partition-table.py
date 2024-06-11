import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Initialize a BigQuery client
client = bigquery.Client()

# Define the dataset and table
dataset_id = os.environ.get("DATASET_ID")
table_id = os.environ.get("TABLE_ID")

# Directory containing the source files
data_directory = 'data/users/'


def load_data(file_name: str) -> None:
    # Ensure the file in csv format
    assert file_name.endswith('.csv')

    # Extract the date from the file name
    partition_date = file_name.split('.')[0]

    # Define the table reference with partition decorator
    table_ref = client.dataset(dataset_id).table(
        f'{table_id}${partition_date}'
    )

    # Load the CSV file into a Pandas DataFrame
    file_path = os.path.join(data_directory, file_name)
    dataframe = pd.read_csv(file_path)
    dataframe['updated_at'] = datetime.now()

    # print(dataframe.info())

    # Define the load job configuration
    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("event_date", "DATE", "REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", "NULLABLE"),
            bigquery.SchemaField("likes", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_="DAY",
            field="event_date",
            require_partition_filter=True
        ),
        source_format=bigquery.SourceFormat.CSV
    )

    # Load the data into the partitioned table
    load_job = client.load_table_from_dataframe(
        dataframe, table_ref, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete

    print(f'Loaded {load_job.output_rows} rows into {table_ref}')


# Load all data
for file_name in os.listdir(data_directory):
    load_data(file_name)


# # To see that bq only update the related partition,
# # uncomment and run the below code line.
# # Then, take a look at the bigquery table.
# # You would see that the `updated_at` will be different

# load_data("data_20240603.csv")  # uncomment this line

print("All data files loaded into BigQuery.")
