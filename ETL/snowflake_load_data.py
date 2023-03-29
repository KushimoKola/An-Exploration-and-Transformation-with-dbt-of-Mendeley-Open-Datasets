

get_ipython().system('pip install snowflake-sqlalchemy')
get_ipython().system('pip install sqlalchemy')
get_ipython().system('pip install tqdm')
get_ipython().system('pip install snowflake-connector-python')

import snowflake.connector
import pandas as pd
from tqdm import tqdm

# Connect to Snowflake account
conn = snowflake.connector.connect(
    user=' ',
    password=' ',
    account='x.canada-central.azure',
)

# Define database and schema
database = 'analytics_dbt'
schema = 'data_lake'

# Set current database
with conn.cursor() as cur:
    cur.execute(f"USE DATABASE {database}")

# List of files to load, and dictionary of corresponding table names
file_dict = {
    '/Users/kolawole/Documents/mendeley_json/mendeley_json/nbagames.json': 'nba_games',
    '/Users/kolawole/Documents/mendeley_json/mendeley_json/dblp.json': 'dblp_data',
    '/Users/kolawole/Documents/mendeley_json/mendeley_json/twitter.twitter2.json': 'twitter_data'
}

# Loop over files
for file_path, table_name in file_dict.items():
    # Load data from file
    json_dumps = []
    with open(file_path, 'r') as f:
        for lines in tqdm(f, desc=f"Loading {file_path}", unit="lines"):
            json_dumps.append(lines)
    f.close()
    data = pd.DataFrame({'data': json_dumps})

    # Create stage for file
    create_stage_query = f"CREATE OR REPLACE STAGE {schema}.{table_name}_stage"
    with conn.cursor() as cur:
        cur.execute(create_stage_query)

    # Put file into stage
    put_file_query = f"PUT file://{file_path} @{schema}.{table_name}_stage"
    with conn.cursor() as cur:
        cur.execute(put_file_query)
        print(f"File {file_path} uploaded to stage {schema}.{table_name}_stage")

    # Load data into Snowflake
    with conn.cursor() as cur:
        # Create table if it doesn't exist
        create_table_query = f"CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (data VARIANT)"
        cur.execute(create_table_query)
        print(f"Table {table_name} created")

        # Load data into table
        copy_query = f"COPY INTO {database}.{schema}.{table_name} FROM @{schema}.{table_name}_stage FILE_FORMAT = (TYPE = 'JSON')"
        cur.execute(copy_query)
        print(f"Data loaded to {database}.{schema}.{table_name}")