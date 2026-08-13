{{ config(materialized="table") }}

SELECT
    1 as id,
    'Hello SEPA' as message,
    now() as created_at
