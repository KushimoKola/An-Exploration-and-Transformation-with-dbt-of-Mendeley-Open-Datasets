with source_model as (
    {{ snowflake_flatten_json(
        stg_table_name = ref('source_twitter_data'),
        json_column = 'raw_data'
    ) }} 
),
final as (
    select * from source_model
)
select * from final;