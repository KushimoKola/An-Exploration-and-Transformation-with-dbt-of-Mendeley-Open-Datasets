get_ipython().system('pip install sqlalchemy')
get_ipython().system('pip install tqdm')

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
from tqdm import tqdm

my_con = create_engine('postgresql://postgres:Colymore0900@localhost:5432/analytics_db')

# List of files to load, and dictionary of corresponding table names
file_dict = {
    '/Users/kolawole/Documents/mendeley_json/mendeley_json/nbagames.json': 'nba_games',
    '/Users/kolawole/Documents/mendeley_json/mendeley_json/dblp.json': 'dblp_data',
    '/Users/kolawole/Documents/mendeley_json/mendeley_json/twitter.twitter2.json': 'twitter_data'
}

# Dictionary of schemas
schema_dict = {'exploration': ['nba_games', 'dblp_data', 'twitter_data']}

# Loop over files
for file_path, table_name in file_dict.items():
    # Load data from file
    json_dumps = []
    with open(file_path, 'r') as f:
        for lines in tqdm(f, desc=f"Loading {file_path}", unit="lines"):
            json_dumps.append(lines)
    f.close()
    data = pd.DataFrame({'data': json_dumps})

    # Loop over schemas and tables
    for schema_name in schema_dict.keys():
        if table_name in schema_dict[schema_name]:
            chunksize = 100_000
            if table_name == 'nba_games':
                chunksize = 1000

            # Check if table already exists in schema
            inspector = Inspector.from_engine(my_con)
            if schema_name in inspector.get_schema_names():
                if table_name not in inspector.get_table_names(schema=schema_name):
                    continue # skip to the next iteration if the table doesn't exist in the schema
                else:
                    data.to_sql(table_name, my_con, index=False, if_exists='replace', schema=schema_name, chunksize=chunksize)
                    print(f"Data loaded to {schema_name}.{table_name}!")
            else:
                print(f"Schema {schema_name} does not exist!")