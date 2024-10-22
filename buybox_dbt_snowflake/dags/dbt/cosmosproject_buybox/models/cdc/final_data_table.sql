with source_data as(
    select * from airflow.dev.single_ASIN
)
select * from source_data