select 
    data::json as raw_data 
from 
    {{ source('analytics_db', 'dblp_data') }}