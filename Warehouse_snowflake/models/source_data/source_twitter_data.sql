select 
    data as raw_data
from 
    {{ source('analytics_dbt', 'twitter_data') }}