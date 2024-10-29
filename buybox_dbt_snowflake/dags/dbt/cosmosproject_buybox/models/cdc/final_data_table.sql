with source_data as(
    select * from DAG_DB.DEV.SINGLE_ASIN
)
select * ,current_timestamp() as created_at from source_data