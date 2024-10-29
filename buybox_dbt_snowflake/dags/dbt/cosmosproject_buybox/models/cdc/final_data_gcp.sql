with source_data as(
    select * from dbtproject-439612.wellbefore.SINGLE_ASIN
)
select * ,current_timestamp() as created_at from source_data