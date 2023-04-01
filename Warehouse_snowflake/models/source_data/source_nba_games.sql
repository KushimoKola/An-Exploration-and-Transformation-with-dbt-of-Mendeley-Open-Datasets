select 
    data as raw_data
from 
    {{ source('analytics_dbt', 'nba_games') }}