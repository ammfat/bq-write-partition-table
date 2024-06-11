# BQ Write Truncate into Partition Table

Demonstrate how to prevent data duplication in BigQuery, by applying table partition and WRITE_TRUNCATE.

Though this repo only uses flat-files (csv), the data source can be anything.

## How to Run

### Setup Environment

- Clone the repo

    ```
    git clone .....
    cd bq-write-partition-tables
    ```

- Create virtual environment

    ```
    python -m venv env
    source env/bin/activate
    ```

- Install the required libraries

    ```
    pip install -r requirements.txt
    ```

- Provide your GCP service account on `config/` directory

    ```
    mkdir config/
    mv /path/to/sa.json config/
    ```

- Setup the `.env` file according to your service account path, along with the dataset and table name as the load destination.

- Run the test connection script

    ```
    python test-bq-connection.py
    ```

    Expected output:

    ```
    active: 78
    closed: 24
    ```

    Proceed to next step if your output is correct.

- Run the write truncate demo

    ```
    python write-partition-table.py
    ```

    There is a bit of instruction on the code to demonstrate how partition works. So, make sure you take a look at it.

- If you want to have different sample, run the data generator:

    ```
    python generate-data.py
    ```