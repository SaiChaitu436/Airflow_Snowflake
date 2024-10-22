with source_data as(
    select * from airflow.dev.single_ASIN
)
select * ,current_timestamp() as created_at from source_data