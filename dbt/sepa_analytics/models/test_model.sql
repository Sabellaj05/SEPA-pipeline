{{ config(materialized="table") }}

SELECT
    1 as id,
    'Hello BigQuery' as message,
    current_timestamp() as created_at
