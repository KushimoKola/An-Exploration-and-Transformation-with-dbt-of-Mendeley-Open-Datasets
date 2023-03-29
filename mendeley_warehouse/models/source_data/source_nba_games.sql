select 
    data::json as raw_data
from 
    {{ source('analytics_db', 'nba_games') }}