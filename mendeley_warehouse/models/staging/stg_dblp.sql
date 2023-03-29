with source_model as (
    {{ normalise_json(
        model_name = ref('source_dblp_data'),
        json_column = 'raw_data'
    ) }} 
),
final as (
    select * from source_model
)
select * from final;